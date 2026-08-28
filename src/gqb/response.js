import { randomUUID } from "node:crypto";

export function envelope(fields = {}) {
  return {
    ok: fields.ok ?? false,
    error_code: fields.error_code ?? null,
    effect_status: fields.effect_status ?? null,
    goal_id: fields.goal_id ?? null,
    fingerprint: fields.fingerprint ?? null,
    next_safe_action: fields.next_safe_action ?? null,
    replayed: fields.replayed ?? false,
    trace_id: fields.trace_id ?? randomUUID(),
    data: fields.data ?? {}
  };
}

export function lockKeyForGoal(goalId) {
  return `goal:${goalId}`;
}

export function presubmitGoalKey(upstreamRequestId) {
  return `__PRESUBMIT__:${upstreamRequestId}`;
}

export function samePlan(status, expectedName, expectedTasks) {
  const goal = status?.goal;
  const tasks = [...(status?.tasks ?? [])].sort((a, b) => Number(a.position) - Number(b.position));
  return goal?.name === expectedName &&
    tasks.length === expectedTasks.length &&
    tasks.every((task, index) => task.jira_key === expectedTasks[index].jiraKey && Number(task.position) === expectedTasks[index].position);
}

export function upstreamDecisionArgs({ upstreamRequestId, goalId, kind, decisionText, taskId, maxAttemptsPerTask }) {
  const args = { requestId: upstreamRequestId, goalId, kind, decisionText };
  if (taskId) args.taskId = taskId;
  if (typeof maxAttemptsPerTask !== "undefined") args.maxAttemptsPerTask = maxAttemptsPerTask;
  return args;
}
