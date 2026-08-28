import { BLOCKER_KINDS, DISPATCH_KINDS, DISPATCH_STATES, EVENT_TYPES, GOAL_STATES, TASK_STATES } from "./constants.js";

export function findVocabularyDrift(status) {
  const drift = [];
  const goalState = status?.goal?.state;
  if (goalState && !GOAL_STATES.has(goalState)) drift.push({ path: "goal.state", value: goalState });

  for (const [index, task] of (status?.tasks ?? []).entries()) {
    if (task?.state && !TASK_STATES.has(task.state)) drift.push({ path: `tasks[${index}].state`, value: task.state });
    if (task?.blocker_kind && !BLOCKER_KINDS.has(task.blocker_kind)) {
      drift.push({ path: `tasks[${index}].blocker_kind`, value: task.blocker_kind });
    }
  }

  const dispatch = status?.current_dispatch;
  if (dispatch?.state && !DISPATCH_STATES.has(dispatch.state)) drift.push({ path: "current_dispatch.state", value: dispatch.state });
  if (dispatch?.kind && !DISPATCH_KINDS.has(dispatch.kind)) drift.push({ path: "current_dispatch.kind", value: dispatch.kind });

  for (const [index, event] of (status?.events ?? []).entries()) {
    if (!Number.isInteger(event?.ordinal) || event.ordinal < 1) drift.push({ path: `events[${index}].ordinal`, value: event?.ordinal });
    if (event?.event_type && !EVENT_TYPES.has(event.event_type)) drift.push({ path: `events[${index}].event_type`, value: event.event_type });
  }
  return drift;
}
