import { EFFECT_STATUS, JOURNAL_PHASE } from "../constants.js";
import { action } from "../next-safe-action.js";
import { payloadHash } from "../payload-hash.js";
import { envelope, presubmitGoalKey, samePlan, upstreamDecisionArgs } from "../response.js";
import {
  authorizationRequestId,
  normalizeRequestId,
  toControllerTaskPlan,
  validateDecisionText,
  validateMaxAttempts,
  ValidationError
} from "../request-id.js";

export async function queueSubmit(bridge, args = {}) {
  const traceId = crypto.randomUUID();
  let lock = null;
  try {
    bridge.requireJournal();
    lock = await bridge.journal.acquireLock("active-goal-slot");
    const upstreamRequestId = normalizeRequestId(args.request_id);
    const tasks = toControllerTaskPlan(args.ordered_jira_keys);
    const maxAttemptsPerTask = validateMaxAttempts(args.max_attempts_per_task);
    if (args.authorize_dispatch === true) validateDecisionText(args.decision_text, { field: "decision_text" });
    if (typeof args.name !== "string" || args.name.length < 1 || args.name.length > 200) {
      throw new ValidationError("name must be 1-200 characters");
    }

    const upstreamArgs = { requestId: upstreamRequestId, name: args.name, tasks, policy: { maxAttemptsPerTask } };
    const hash = payloadHash(upstreamArgs);
    const goalKey = presubmitGoalKey(upstreamRequestId);
    const existing = bridge.journal.getEntry(goalKey, upstreamRequestId);
    if (existing) {
      if (existing.phase === JOURNAL_PHASE.INTENT) return bridge.reconcileEnvelope(existing, traceId);
      return bridge.cachedEnvelope(existing, traceId);
    }

    const activeStatus = await bridge.readStatus({ include_events: false });
    if (["running", "awaiting_owner", "blocked"].includes(activeStatus.goal?.state)) {
      return envelope({
        error_code: "ACTIVE_GOAL_EXISTS",
        effect_status: EFFECT_STATUS.NOT_APPLIED,
        goal_id: activeStatus.goal.goal_id,
        fingerprint: activeStatus.fingerprint,
        next_safe_action: activeStatus.next_safe_action,
        trace_id: traceId,
        data: { active_goal: activeStatus.goal }
      });
    }

    bridge.journal.createIntent({
      goal_key: goalKey,
      upstream_request_id: upstreamRequestId,
      caller_request_id: args.request_id,
      tool: "submit_goal",
      payload_hash: hash,
      channel: bridge.channel,
      upstream_arguments: upstreamArgs,
      pre_status_snapshot: activeStatus,
      trace_id: traceId
    });

    const upstreamResult = await bridge.callMutator("submit_goal", upstreamArgs, goalKey, upstreamRequestId, lock);
    const postStatus = await bridge.tryPostRead(upstreamResult.goal_id);
    if (upstreamResult.created === false && postStatus.status && !samePlan(postStatus.status, args.name, tasks)) {
      bridge.journal.writeOutcome(goalKey, upstreamRequestId, {
        goal_id: upstreamResult.goal_id,
        upstream_result: upstreamResult,
        post_status_snapshot: postStatus.status,
        effect_status: EFFECT_STATUS.NOT_APPLIED
      }, lock);
      return envelope({
        error_code: "IDEMPOTENCY_KEY_REUSE",
        effect_status: EFFECT_STATUS.NOT_APPLIED,
        goal_id: upstreamResult.goal_id,
        fingerprint: postStatus.status?.fingerprint ?? null,
        next_safe_action: postStatus.status?.next_safe_action ?? action("RE_READ_STATUS", "post_read_status_required"),
        trace_id: traceId,
        data: { created: false, conflict: true }
      });
    }

    const finalEntry = bridge.journal.writeOutcome(goalKey, upstreamRequestId, {
      goal_id: upstreamResult.goal_id,
      upstream_result: upstreamResult,
      post_status_snapshot: postStatus.status,
      effect_status: EFFECT_STATUS.APPLIED
    }, lock);

    let authorization = null;
    if (args.authorize_dispatch === true) {
      authorization = await authorizeAfterSubmit(bridge, {
        goalId: upstreamResult.goal_id,
        upstreamRequestId: authorizationRequestId(upstreamRequestId),
        callerRequestId: args.request_id,
        decisionText: args.decision_text,
        traceId,
        lock
      });
    }

    return envelope({
      ok: true,
      effect_status: EFFECT_STATUS.APPLIED,
      goal_id: upstreamResult.goal_id,
      fingerprint: postStatus.status?.fingerprint ?? null,
      next_safe_action: postStatus.status?.next_safe_action ?? action("RE_READ_STATUS", "post_read_status_required"),
      trace_id: traceId,
      data: {
        created: upstreamResult.created ?? true,
        tasks: upstreamResult.tasks ?? tasks,
        post_read_failed: postStatus.failed,
        authorization_status: authorization?.effect_status ?? null,
        authorization_request_id: authorization?.upstream_request_id ?? null,
        journal_entry: finalEntry
      }
    });
  } catch (error) {
    return bridge.errorEnvelope(error, traceId);
  } finally {
    if (lock) bridge.journal?.releaseLock(lock);
  }
}

export async function authorizeAfterSubmit(bridge, { goalId, upstreamRequestId, callerRequestId, decisionText, traceId, lock }) {
  const upstreamArgs = upstreamDecisionArgs({
    upstreamRequestId,
    goalId,
    kind: "AUTHORIZE_DISPATCH",
    decisionText
  });
  const hash = payloadHash(upstreamArgs);
  const existing = bridge.journal.getEntry(goalId, upstreamRequestId);
  if (existing?.phase === JOURNAL_PHASE.INTENT) {
    return { upstream_request_id: upstreamRequestId, effect_status: EFFECT_STATUS.INDETERMINATE };
  }
  if (existing) {
    return { upstream_request_id: upstreamRequestId, effect_status: existing.effect_status };
  }
  bridge.journal.createIntent({
    goal_key: goalId,
    goal_id: goalId,
    upstream_request_id: upstreamRequestId,
    caller_request_id: callerRequestId,
    tool: "submit_owner_decision",
    payload_hash: hash,
    channel: bridge.channel,
    upstream_arguments: upstreamArgs,
    trace_id: traceId
  });
  const result = await bridge.callMutator("submit_owner_decision", upstreamArgs, goalId, upstreamRequestId, lock);
  bridge.journal.writeOutcome(goalId, upstreamRequestId, {
    goal_id: goalId,
    upstream_result: result,
    effect_status: EFFECT_STATUS.APPLIED
  }, lock);
  return { upstream_request_id: upstreamRequestId, effect_status: EFFECT_STATUS.APPLIED };
}
