import { randomUUID } from "node:crypto";
import { CONTROL_KINDS, EFFECT_STATUS, JOURNAL_PHASE } from "../constants.js";
import { currentTask } from "../fingerprint.js";
import { JournalError } from "../journal.js";
import { action } from "../next-safe-action.js";
import { payloadHash } from "../payload-hash.js";
import { envelope, lockKeyForGoal, upstreamDecisionArgs } from "../response.js";
import { normalizeRequestId, validateDecisionText, validateMaxAttempts, ValidationError } from "../request-id.js";

export async function queueResume(bridge, args = {}) {
  return ownerDecisionMutation(bridge, args, { allowedKinds: new Set(["RESUME", "ANSWER"]), isResume: true });
}

export async function queueControl(bridge, args = {}) {
  return ownerDecisionMutation(bridge, args, { allowedKinds: CONTROL_KINDS, isResume: false });
}

export async function ownerDecisionMutation(bridge, args, { allowedKinds, isResume }) {
  const traceId = randomUUID();
  let lock = null;
  try {
    bridge.requireJournal();
    const kind = args.kind;
    if (!allowedKinds.has(kind)) throw new ValidationError("unsupported owner decision kind");
    validateDecisionText(args.decision_text, {
      min: kind === "RAISE_ATTEMPT_CEILING" ? 20 : 1,
      field: "decision_text"
    });
    const upstreamRequestId = normalizeRequestId(args.request_id);
    const goalId = args.goal_id;
    if (typeof goalId !== "string" || goalId.length === 0) throw new ValidationError("goal_id is required");
    const maxAttemptsPerTask = kind === "RAISE_ATTEMPT_CEILING" ? validateMaxAttempts(args.max_attempts_per_task, { required: true }) : undefined;

    lock = await bridge.journal.acquireLock(lockKeyForGoal(goalId));
    const upstreamArgs = upstreamDecisionArgs({
      upstreamRequestId,
      goalId,
      kind,
      decisionText: args.decision_text,
      taskId: args.task_id,
      maxAttemptsPerTask
    });
    const hash = payloadHash(upstreamArgs);
    const existing = bridge.journal.getEntry(goalId, upstreamRequestId);
    if (existing && existing.tool !== "submit_owner_decision") throw new JournalError("request id collision across tools", "IDEMPOTENCY_KEY_REUSE");
    if (existing) {
      if (existing.phase === JOURNAL_PHASE.INTENT) return bridge.reconcileEnvelope(existing, traceId);
      return bridge.cachedEnvelope(existing, traceId);
    }

    const preStatus = await bridge.readStatus({ goal_id: goalId, include_events: true, event_limit: 500 });
    if (preStatus.vocabulary_drift.length > 0) {
      return envelope({
        error_code: "ESCALATE_UNKNOWN_STATE",
        effect_status: EFFECT_STATUS.NOT_APPLIED,
        goal_id: goalId,
        fingerprint: preStatus.fingerprint,
        next_safe_action: action("ESCALATE_UNKNOWN_STATE", "unknown_upstream_vocabulary"),
        trace_id: traceId,
        data: { vocabulary_drift: preStatus.vocabulary_drift }
      });
    }
    if (args.expected_fingerprint && args.expected_fingerprint !== preStatus.fingerprint.value) {
      return envelope({
        error_code: "STALE_FINGERPRINT",
        effect_status: EFFECT_STATUS.NOT_APPLIED,
        goal_id: goalId,
        fingerprint: preStatus.fingerprint,
        next_safe_action: action("RE_READ_STATUS", "stale_expected_fingerprint"),
        trace_id: traceId
      });
    }
    if (isResume) {
      const verdict = resumeGateVerdict(preStatus, kind, args.task_id);
      if (!verdict.ok) {
        return envelope({
          error_code: verdict.error_code,
          effect_status: EFFECT_STATUS.NOT_APPLIED,
          goal_id: goalId,
          fingerprint: preStatus.fingerprint,
          next_safe_action: verdict.next_safe_action,
          trace_id: traceId,
          data: { gates: verdict.gates }
        });
      }
    }
    if (kind === "RAISE_ATTEMPT_CEILING" && maxAttemptsPerTask <= Number(preStatus.goal?.max_attempts_per_task ?? 0)) {
      return envelope({
        error_code: "INVALID_ARGUMENT",
        effect_status: EFFECT_STATUS.NOT_APPLIED,
        goal_id: goalId,
        fingerprint: preStatus.fingerprint,
        next_safe_action: preStatus.next_safe_action,
        trace_id: traceId,
        data: { reason: "max_attempts_per_task must strictly increase current ceiling" }
      });
    }

    bridge.journal.createIntent({
      goal_key: goalId,
      goal_id: goalId,
      upstream_request_id: upstreamRequestId,
      caller_request_id: args.request_id,
      tool: "submit_owner_decision",
      payload_hash: hash,
      channel: bridge.channel,
      upstream_arguments: upstreamArgs,
      pre_status_snapshot: preStatus,
      trace_id: traceId
    });

    const upstreamResult = await bridge.callMutator("submit_owner_decision", upstreamArgs, goalId, upstreamRequestId, lock);
    const postStatus = await bridge.tryPostRead(goalId);
    bridge.journal.writeOutcome(goalId, upstreamRequestId, {
      goal_id: goalId,
      upstream_result: upstreamResult,
      post_status_snapshot: postStatus.status,
      effect_status: EFFECT_STATUS.APPLIED
    }, lock);
    return envelope({
      ok: true,
      effect_status: EFFECT_STATUS.APPLIED,
      goal_id: goalId,
      fingerprint: postStatus.status?.fingerprint ?? preStatus.fingerprint,
      next_safe_action: postStatus.status?.next_safe_action ?? action("RE_READ_STATUS", "post_read_status_required"),
      trace_id: traceId,
      data: {
        upstream_result: upstreamResult,
        post_read_failed: postStatus.failed,
        attempts_used_before: currentTask(preStatus.tasks)?.attempts_used ?? null,
        attempts_used_after: postStatus.status ? currentTask(postStatus.status.tasks)?.attempts_used ?? null : null,
        dispatch_will_wait: postStatus.status?.current_dispatch?.state === "PENDING" && !postStatus.status?.goal?.dispatch_authorized
      }
    });
  } catch (error) {
    return bridge.errorEnvelope(error, traceId);
  } finally {
    if (lock) bridge.journal?.releaseLock(lock);
  }
}

export function resumeGateVerdict(status, kind, taskId = null) {
  const goal = status?.goal;
  const target = taskId
    ? (status?.tasks ?? []).find((task) => task.task_id === taskId && task.state !== "completed")
    : currentTask(status?.tasks ?? []);
  const gates = [];
  if (taskId && !target) {
    return { ok: false, error_code: "INVALID_ARGUMENT", next_safe_action: action("RE_READ_STATUS", "target_task_not_found"), gates };
  }
  if (!goal || !target) {
    return { ok: false, error_code: "KIND_STATE_MISMATCH", next_safe_action: action("ESCALATE_UNRECOGNIZED", "missing_goal_or_target_task"), gates };
  }
  const live = status?.current_dispatch && ["PENDING", "FIRED", "CLAIMED"].includes(status.current_dispatch.state) ? status.current_dispatch : null;
  if (live && goal.state === "running" && target.state === "active") {
    gates.push({ code: "NOOP_ALREADY_IN_FLIGHT", passed: false });
    return { ok: false, error_code: "NOOP_ALREADY_IN_FLIGHT", next_safe_action: action("WAIT_FOR_WORKER", "already_in_flight"), gates };
  }
  const answerAllowed = Boolean(goal.pending_question) || goal.state === "awaiting_owner" || goal.state === "blocked" || target.state === "blocked";
  if (kind === "ANSWER" && !answerAllowed) {
    gates.push({ code: "ANSWER_STATE", passed: false });
    return { ok: false, error_code: "KIND_STATE_MISMATCH", next_safe_action: status.next_safe_action, gates };
  }
  const resumeAllowed = ["blocked", "awaiting_owner"].includes(goal.state) || target.state === "blocked" || target.state === "active";
  if (kind === "RESUME" && !resumeAllowed) {
    gates.push({ code: "RESUME_STATE", passed: false });
    return { ok: false, error_code: "KIND_STATE_MISMATCH", next_safe_action: status.next_safe_action, gates };
  }
  const attemptsUsed = Number(target.attempts_used ?? 0);
  const maxAttempts = Number(goal.max_attempts_per_task ?? 4);
  if (attemptsUsed >= maxAttempts && maxAttempts === 10) {
    gates.push({ code: "ATTEMPT_CEILING_AT_BOUND", passed: false });
    return {
      ok: false,
      error_code: "ATTEMPT_CEILING_AT_BOUND",
      next_safe_action: action("ESCALATE_ATTEMPT_CEILING_AT_BOUND", "attempt_ceiling_at_bound"),
      gates
    };
  }
  if (attemptsUsed >= maxAttempts) {
    gates.push({ code: "ATTEMPT_BUDGET_EXHAUSTED", passed: false });
    return {
      ok: false,
      error_code: "ATTEMPT_BUDGET_EXHAUSTED",
      next_safe_action: action("RAISE_ATTEMPT_CEILING", "attempt_budget_exhausted"),
      gates
    };
  }
  gates.push({ code: "ADMIT", passed: true });
  return {
    ok: true,
    gates,
    projected_effect: target.state === "blocked"
      ? { opens_new_attempt: true, attempts_used_changes: false }
      : { reuses_current_attempt: target.state === "active", attempts_used_changes: false }
  };
}
