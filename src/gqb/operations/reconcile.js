import { randomUUID } from "node:crypto";
import { EFFECT_STATUS, JOURNAL_PHASE } from "../constants.js";
import { action } from "../next-safe-action.js";
import { envelope, lockKeyForGoal, presubmitGoalKey, samePlan } from "../response.js";
import { validateDecisionText, ValidationError } from "../request-id.js";

export async function queueReconcile(bridge, args = {}) {
  const traceId = randomUUID();
  let lock = null;
  try {
    bridge.requireJournal();
    const mode = args.mode;
    if (!["OBSERVE", "REPLAY", "ADOPT_SUBMIT"].includes(mode)) throw new ValidationError("mode must be OBSERVE, REPLAY, or ADOPT_SUBMIT");
    const goalKey = args.presubmit_request_id ? presubmitGoalKey(args.presubmit_request_id) : args.goal_id;
    if (!goalKey || !args.upstream_request_id) throw new ValidationError("explicit journal address is required");
    const entry = bridge.journal.getEntry(goalKey, args.upstream_request_id);
    if (!entry) throw new ValidationError("journal entry not found");
    if (entry.phase !== JOURNAL_PHASE.INTENT) return bridge.cachedEnvelope(entry, traceId);
    lock = await bridge.journal.acquireLock(entry.tool === "submit_goal" ? "active-goal-slot" : lockKeyForGoal(entry.goal_id ?? args.goal_id));

    if (mode === "OBSERVE") return observeIntent(bridge, entry, traceId, lock);
    if (mode === "ADOPT_SUBMIT") return adoptSubmit(bridge, entry, args, traceId, lock);
    if (entry.tool === "submit_goal") return replaySubmit(bridge, entry, traceId, lock);

    const result = await bridge.callMutator(entry.tool, entry.upstream_arguments, entry.goal_key, entry.upstream_request_id, lock);
    bridge.journal.writeOutcome(entry.goal_key, entry.upstream_request_id, {
      phase: JOURNAL_PHASE.RECONCILED,
      goal_id: result.goal_id ?? entry.goal_id,
      upstream_result: result,
      effect_status: EFFECT_STATUS.APPLIED
    }, lock);
    return envelope({
      ok: true,
      effect_status: EFFECT_STATUS.APPLIED,
      goal_id: result.goal_id ?? entry.goal_id,
      trace_id: traceId,
      data: { mode, upstream_result: result }
    });
  } catch (error) {
    return bridge.errorEnvelope(error, traceId);
  } finally {
    if (lock) bridge.journal?.releaseLock(lock);
  }
}

async function observeIntent(bridge, entry, traceId, lock) {
  if (entry.tool === "submit_goal") {
    return envelope({
      error_code: "UPSTREAM_INDETERMINATE",
      effect_status: EFFECT_STATUS.INDETERMINATE,
      goal_id: entry.goal_id,
      next_safe_action: action("RECONCILE_BRIDGE_UNCERTAINTY", "submit_observe_requires_replay"),
      trace_id: traceId,
      data: { mode: "OBSERVE", entry }
    });
  }
  const status = await bridge.readStatus({ goal_id: entry.goal_id, include_events: true, event_limit: 500 });
  const matched = (status.events ?? []).some((event) => event.event_type === "OWNER_DECISION" && event.payload?.request_id === entry.upstream_request_id);
  if (matched || status.events_complete === true) {
    const effect = matched ? EFFECT_STATUS.APPLIED : EFFECT_STATUS.NOT_APPLIED;
    bridge.journal.writeOutcome(entry.goal_key, entry.upstream_request_id, {
      phase: JOURNAL_PHASE.RECONCILED,
      goal_id: entry.goal_id,
      post_status_snapshot: status,
      effect_status: effect
    }, lock);
    return envelope({
      ok: matched,
      error_code: matched ? null : "UPSTREAM_REFUSED",
      effect_status: effect,
      goal_id: entry.goal_id,
      fingerprint: status.fingerprint,
      next_safe_action: status.next_safe_action,
      trace_id: traceId,
      data: { mode: "OBSERVE", matched }
    });
  }
  return envelope({
    error_code: "UPSTREAM_INDETERMINATE",
    effect_status: EFFECT_STATUS.INDETERMINATE,
    goal_id: entry.goal_id,
    next_safe_action: action("RECONCILE_BRIDGE_UNCERTAINTY", "event_window_incomplete"),
    trace_id: traceId,
    data: { mode: "OBSERVE", matched: false }
  });
}

async function replaySubmit(bridge, entry, traceId, lock) {
  const activeStatus = await bridge.readStatus({ include_events: true, event_limit: 500 });
  const activeGoal = activeStatus.goal;
  const activeIsNonTerminal = activeGoal && !["completed", "cancelled"].includes(activeGoal.state);
  const identityLinked = activeGoal?.goal_id === entry.goal_id || (activeStatus.events ?? []).some((event) =>
    event.event_type === "GOAL_SUBMITTED" &&
    (event.payload?.request_id === entry.upstream_request_id || event.payload?.requestId === entry.upstream_request_id)
  );
  if (activeIsNonTerminal && !identityLinked) {
    return envelope({
      error_code: "AMBIGUOUS_SUBMIT_OUTCOME",
      effect_status: EFFECT_STATUS.INDETERMINATE,
      goal_id: entry.goal_id,
      fingerprint: activeStatus.fingerprint,
      next_safe_action: action("OWNER_DECISION_REQUIRED", "ambiguous_submit_outcome"),
      trace_id: traceId,
      data: { active_goal: activeGoal }
    });
  }

  const result = await bridge.callMutator("submit_goal", entry.upstream_arguments, entry.goal_key, entry.upstream_request_id, lock);
  const status = await bridge.readStatus({ goal_id: result.goal_id, include_events: true, event_limit: 500 });
  if (result.created === false && !samePlan(status, entry.upstream_arguments.name, entry.upstream_arguments.tasks)) {
    bridge.journal.writeOutcome(entry.goal_key, entry.upstream_request_id, {
      phase: JOURNAL_PHASE.RECONCILED,
      goal_id: result.goal_id,
      upstream_result: result,
      post_status_snapshot: status,
      effect_status: EFFECT_STATUS.NOT_APPLIED
    }, lock);
    return envelope({
      error_code: "IDEMPOTENCY_KEY_REUSE",
      effect_status: EFFECT_STATUS.NOT_APPLIED,
      goal_id: result.goal_id,
      fingerprint: status.fingerprint,
      next_safe_action: status.next_safe_action,
      trace_id: traceId,
      data: { mode: "REPLAY", conflict: true }
    });
  }
  const effect = result.created === false ? EFFECT_STATUS.ALREADY_APPLIED_OR_NOOP : EFFECT_STATUS.APPLIED;
  bridge.journal.writeOutcome(entry.goal_key, entry.upstream_request_id, {
    phase: JOURNAL_PHASE.RECONCILED,
    goal_id: result.goal_id,
    upstream_result: result,
    post_status_snapshot: status,
    effect_status: effect
  }, lock);
  return envelope({
    ok: true,
    effect_status: effect,
    goal_id: result.goal_id,
    fingerprint: status.fingerprint,
    next_safe_action: status.next_safe_action,
    trace_id: traceId,
    data: { mode: "REPLAY", upstream_result: result }
  });
}

async function adoptSubmit(bridge, entry, args, traceId, lock) {
  if (entry.tool !== "submit_goal") throw new ValidationError("ADOPT_SUBMIT only applies to submit_goal entries");
  validateDecisionText(args.owner_confirmation_text, { min: 20, field: "owner_confirmation_text" });
  const status = await bridge.readStatus({ goal_id: args.adopt_goal_id, include_events: false });
  if (!samePlan(status, entry.upstream_arguments.name, entry.upstream_arguments.tasks)) {
    return envelope({
      error_code: "IDEMPOTENCY_KEY_REUSE",
      effect_status: EFFECT_STATUS.NOT_APPLIED,
      goal_id: args.adopt_goal_id,
      fingerprint: status.fingerprint,
      next_safe_action: action("OWNER_DECISION_REQUIRED", "adopted_goal_plan_mismatch"),
      trace_id: traceId,
      data: { mode: "ADOPT_SUBMIT", adopted: false }
    });
  }
  bridge.journal.writeOutcome(entry.goal_key, entry.upstream_request_id, {
    phase: JOURNAL_PHASE.RECONCILED,
    goal_id: args.adopt_goal_id,
    post_status_snapshot: {
      status,
      audit: {
        actor: args.actor ?? null,
        owner_confirmation_text: args.owner_confirmation_text,
        adopt_goal_id: args.adopt_goal_id,
        matched_plan: entry.upstream_arguments.tasks,
        adopted_goal_snapshot: status
      }
    },
    effect_status: EFFECT_STATUS.APPLIED
  }, lock);
  return envelope({
    ok: true,
    effect_status: EFFECT_STATUS.APPLIED,
    goal_id: args.adopt_goal_id,
    fingerprint: status.fingerprint,
    next_safe_action: status.next_safe_action,
    trace_id: traceId,
    data: { mode: "ADOPT_SUBMIT", adopted: true }
  });
}
