import { createHash } from "node:crypto";
import { canonicalJson } from "./canonical-json.js";

export function currentTask(tasks = []) {
  return [...tasks]
    .filter((task) => task?.state !== "completed")
    .sort((a, b) => Number(a.position ?? 0) - Number(b.position ?? 0))[0] ?? null;
}

export function liveDispatch(currentDispatch) {
  if (!currentDispatch) return null;
  return ["PENDING", "FIRED", "CLAIMED"].includes(currentDispatch.state) ? currentDispatch : null;
}

export function fingerprintProjection(status) {
  const goal = status?.goal ?? null;
  const task = currentTask(status?.tasks ?? []);
  const dispatch = liveDispatch(status?.current_dispatch ?? null);

  return {
    goal: {
      goal_id: goal?.goal_id ?? null,
      state: goal?.state ?? null,
      max_attempts_per_task: goal?.max_attempts_per_task ?? null,
      dispatch_authorized: goal?.dispatch_authorized ?? null,
      pending_question_present: Boolean(goal?.pending_question),
      blocked_reason_present: Boolean(goal?.blocked_reason)
    },
    current_task: {
      task_id: task?.task_id ?? null,
      state: task?.state ?? null,
      attempts_used: task?.attempts_used ?? null
    },
    current_dispatch: {
      dispatch_id: dispatch?.dispatch_id ?? null,
      state: dispatch?.state ?? null,
      delivery_session_id: dispatch?.delivery_session_id ?? null
    }
  };
}

export function statusFingerprint(status) {
  const projection = fingerprintProjection(status);
  return {
    algorithm: "sha256",
    bytes: 16,
    value: createHash("sha256").update(canonicalJson(projection), "utf8").digest("hex").slice(0, 32),
    projection
  };
}
