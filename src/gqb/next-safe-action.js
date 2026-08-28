import { LIVE_DISPATCH_STATES } from "./constants.js";
import { currentTask, liveDispatch } from "./fingerprint.js";
import { findVocabularyDrift } from "./vocabulary.js";

export function action(code, reason, extra = {}) {
  return {
    code,
    reason,
    required_owner_action: extra.required_owner_action ?? null,
    notes: extra.notes ?? null
  };
}

export function eventsComplete(events = [], eventLimit = 500, eventFetchFailed = false) {
  if (eventFetchFailed) return false;
  return events.length < eventLimit;
}

export function latestPauseDecisionMatches(status) {
  const blockedReason = status?.goal?.blocked_reason;
  if (!blockedReason) return false;
  const ownerEvents = [...(status?.events ?? [])]
    .filter((event) => event?.event_type === "OWNER_DECISION")
    .sort((a, b) => Number(b.ordinal ?? 0) - Number(a.ordinal ?? 0));
  const latestBlockAffecting = ownerEvents.find((event) => ["PAUSE", "RESUME", "ANSWER", "CANCEL"].includes(event?.payload?.kind));
  return latestBlockAffecting?.payload?.kind === "PAUSE" && latestBlockAffecting?.payload?.decision_text === blockedReason;
}

export function deriveNextSafeAction(status, options = {}) {
  const {
    ownershipChannelAvailable = true,
    openIntent = false,
    eventLimit = 500,
    eventFetchFailed = false,
    handoffCapability = false
  } = options;

  if (!ownershipChannelAvailable) return action("WAIT_FOR_OWNERSHIP_CHANNEL", "ownership_channel_unavailable");
  if (findVocabularyDrift(status).length > 0) return action("ESCALATE_UNKNOWN_STATE", "unknown_upstream_vocabulary");
  if (openIntent) return action("RECONCILE_BRIDGE_UNCERTAINTY", "open_bridge_intent");

  const goal = status?.goal;
  if (!goal) return action("ESCALATE_UNRECOGNIZED", "no_goal_status");
  if (["completed", "cancelled"].includes(goal.state)) return action("NO_ACTION_GOAL_TERMINAL", "goal_terminal");

  const task = currentTask(status?.tasks ?? []);
  const dispatch = liveDispatch(status?.current_dispatch ?? null);
  if (dispatch?.delivery_uncertain === true) return action("AWAIT_CONTROLLER_DELIVERY_RESOLUTION", "delivery_uncertain");
  if (goal.state === "awaiting_owner" && goal.pending_question) {
    return action("ANSWER_OWNER_QUESTION", "pending_question_present", { required_owner_action: "ANSWER" });
  }
  if (goal.state === "awaiting_owner") return action("OWNER_DECISION_REQUIRED", "awaiting_owner_without_question");

  const completeEvents = eventsComplete(status?.events ?? [], eventLimit, eventFetchFailed);
  if (goal.state === "blocked" && !completeEvents) return action("ESCALATE_UNKNOWN_STATE", "blocked_state_events_incomplete");
  if (goal.state === "blocked" && latestPauseDecisionMatches(status)) return action("NO_ACTION_OWNER_PAUSED", "owner_paused_goal");

  const attemptsUsed = Number(task?.attempts_used ?? 0);
  const maxAttempts = Number(goal.max_attempts_per_task ?? 4);
  const goalOrTaskBlocked = goal.state === "blocked" || task?.state === "blocked";
  if (goalOrTaskBlocked && attemptsUsed >= maxAttempts && maxAttempts === 10) {
    return action("ESCALATE_ATTEMPT_CEILING_AT_BOUND", "attempt_ceiling_at_bound");
  }
  if (goalOrTaskBlocked && attemptsUsed >= maxAttempts) return action("RAISE_ATTEMPT_CEILING", "attempt_budget_exhausted");
  if (goalOrTaskBlocked) return action("OWNER_DECISION_REQUIRED", "blocked_with_budget_remaining");

  if (goal.state === "running" && task?.state === "active" && !dispatch && attemptsUsed >= maxAttempts && maxAttempts === 10) {
    return action("ESCALATE_ATTEMPT_CEILING_AT_BOUND", "active_no_dispatch_ceiling_at_bound");
  }
  if (goal.state === "running" && task?.state === "active" && !dispatch && attemptsUsed >= maxAttempts) {
    return action("RAISE_ATTEMPT_CEILING", "active_no_dispatch_budget_exhausted");
  }
  if (goal.state === "running" && task?.state === "active" && !dispatch && attemptsUsed < maxAttempts) {
    return action("RESUME_QUEUE", "active_attempt_has_no_live_dispatch", { required_owner_action: "RESUME" });
  }

  if (dispatch?.state === "PENDING" && !goal.dispatch_authorized) {
    return action("AUTHORIZE_DISPATCH", "pending_dispatch_unauthorized", { required_owner_action: "AUTHORIZE_DISPATCH" });
  }
  if (dispatch?.state === "PENDING" && goal.dispatch_authorized) return action("WAIT_FOR_FIRE", "pending_dispatch_authorized");
  if (dispatch?.state === "FIRED") return action("WAIT_FOR_CLAIM", "dispatch_fired_not_claimed");
  if (dispatch?.state === "CLAIMED" && !handoffCapability) {
    return action("WAIT_FOR_WORKER", "dispatch_claimed", { notes: "RECOVERY_REQUIRES_UPSTREAM_CAPABILITY" });
  }
  if (dispatch?.state === "CLAIMED") return action("WAIT_FOR_WORKER", "dispatch_claimed");

  if (status?.current_dispatch?.state && !LIVE_DISPATCH_STATES.has(status.current_dispatch.state)) {
    return action("ESCALATE_UNRECOGNIZED", "terminal_dispatch_on_nonterminal_goal");
  }
  return action("ESCALATE_UNRECOGNIZED", "no_matching_safe_action");
}
