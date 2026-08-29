import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { homedir, platform } from "node:os";
import { DISPATCH_STATES, GOAL_STATES, OWNER_DECISIONS, TASK_STATES } from "./constants.js";
import { UpstreamError } from "./controller-client.js";
import { nullLogger } from "./diagnostics.js";

export const LOCAL_CONTROLLER_MODE = "embedded_local";

export class LocalController {
  constructor({ statePath = defaultStatePath(), logger = nullLogger() } = {}) {
    this.statePath = statePath;
    this.logger = logger;
  }

  static fromEnv(env = process.env, { logger = nullLogger() } = {}) {
    return new LocalController({
      statePath: env.GQB_LOCAL_CONTROLLER_STATE_PATH || defaultStatePath(env),
      logger
    });
  }

  describeTransport() {
    return { kind: LOCAL_CONTROLLER_MODE, state_path: this.statePath };
  }

  async ping() {
    const result = await this.callTool("get_goal_status", { includeEvents: false, eventLimit: 1 });
    return {
      reachable: true,
      ping_attempted: true,
      diagnosis: null,
      upstream_shape: shapeOf(result),
      idle_goal_null_accepted: Object.hasOwn(result ?? {}, "goal") && result.goal === null
    };
  }

  async callTool(name, args = {}) {
    const state = this.readState();
    if (name === "get_goal_status") return this.getGoalStatus(state, args);
    if (name === "submit_goal") return this.withWrite(state, () => this.submitGoal(state, args));
    if (name === "submit_owner_decision") return this.withWrite(state, () => this.submitOwnerDecision(state, args));
    throw upstream("unsupported local controller tool", "VALIDATION_ERROR", "INVALID_ARGUMENT");
  }

  getGoalStatus(state, args = {}) {
    const goalId = args.goalId ?? args.goal_id ?? state.active_goal_id;
    if (!goalId) return visibleStatus(emptyStatus(), args);
    const goalRecord = state.goals[goalId];
    if (!goalRecord) throw upstream("goal not found", "GOAL_NOT_FOUND", "UPSTREAM_REFUSED");
    return visibleStatus(goalRecord.status, args);
  }

  submitGoal(state, args = {}) {
    validateRequestId(args.requestId);
    validateGoalName(args.name);
    validateTasks(args.tasks);
    const existing = state.request_results[args.requestId];
    if (existing) return { ...existing, created: false };

    const activeGoal = state.active_goal_id ? state.goals[state.active_goal_id]?.status?.goal : null;
    if (activeGoal && ["running", "awaiting_owner", "blocked"].includes(activeGoal.state)) {
      throw upstream("active goal exists", "ACTIVE_GOAL_EXISTS", "ACTIVE_GOAL_EXISTS");
    }

    const goalId = `local-goal-${randomUUID()}`;
    const maxAttemptsPerTask = clampAttempts(args.policy?.maxAttemptsPerTask);
    const tasks = args.tasks.map((task, index) => ({
      task_id: `local-task-${index + 1}`,
      position: Number(task.position ?? index + 1),
      jira_key: task.jiraKey,
      state: index === 0 ? "active" : "pending",
      attempts_used: index === 0 ? 1 : 0
    }));
    const status = {
      goal: {
        goal_id: goalId,
        name: args.name,
        state: "running",
        max_attempts_per_task: maxAttemptsPerTask,
        pending_question: null,
        blocked_reason: null,
        dispatch_authorized: false
      },
      tasks,
      current_dispatch: {
        dispatch_id: `local-dispatch-${randomUUID()}`,
        kind: "EXECUTE",
        state: "PENDING",
        delivery_session_id: null
      },
      events: []
    };
    pushEvent(status, "GOAL_SUBMITTED", { request_id: args.requestId, name: args.name });
    pushEvent(status, "TASK_ACTIVATED", { task_id: tasks[0]?.task_id ?? null });
    pushEvent(status, "ATTEMPT_OPENED", { task_id: tasks[0]?.task_id ?? null, attempt: 1 });
    pushEvent(status, "DISPATCH_CREATED", { dispatch_id: status.current_dispatch.dispatch_id, kind: "EXECUTE" });

    const result = { goal_id: goalId, state: "running", tasks, created: true };
    state.goals[goalId] = { status };
    state.active_goal_id = goalId;
    state.request_results[args.requestId] = result;
    return result;
  }

  submitOwnerDecision(state, args = {}) {
    validateRequestId(args.requestId);
    if (!OWNER_DECISIONS.has(args.kind)) {
      throw upstream("unknown owner decision kind", "UNKNOWN_OWNER_DECISION_KIND", "INVALID_ARGUMENT");
    }
    const existing = state.request_results[args.requestId];
    if (existing) return existing;
    const goalRecord = state.goals[args.goalId];
    if (!goalRecord) throw upstream("goal not found", "GOAL_NOT_FOUND", "UPSTREAM_REFUSED");

    const status = goalRecord.status;
    const event = pushEvent(status, "OWNER_DECISION", {
      request_id: args.requestId,
      kind: args.kind,
      task_id: args.taskId ?? null
    });
    let dispatchCreated = false;

    switch (args.kind) {
      case "AUTHORIZE_DISPATCH":
        ensureDispatch(status);
        status.goal.dispatch_authorized = true;
        break;
      case "REVOKE_DISPATCH_AUTHORIZATION":
        status.goal.dispatch_authorized = false;
        break;
      case "PAUSE":
        status.goal.state = "blocked";
        status.goal.blocked_reason = args.decisionText || "owner pause";
        currentTask(status.tasks).state = "blocked";
        break;
      case "CANCEL":
        status.goal.state = "cancelled";
        if (status.current_dispatch) status.current_dispatch.state = "CANCELLED";
        if (state.active_goal_id === args.goalId) state.active_goal_id = null;
        pushEvent(status, "GOAL_CANCELLED", { goal_id: args.goalId });
        break;
      case "RAISE_ATTEMPT_CEILING":
        status.goal.max_attempts_per_task = clampAttempts(args.maxAttemptsPerTask, { required: true });
        break;
      case "ANSWER":
      case "RESUME":
        status.goal.state = "running";
        status.goal.pending_question = null;
        status.goal.blocked_reason = null;
        for (const task of status.tasks) {
          if (task.state === "blocked") task.state = "active";
        }
        if (!liveDispatch(status.current_dispatch)) {
          status.current_dispatch = {
            dispatch_id: `local-dispatch-${randomUUID()}`,
            kind: "EXECUTE",
            state: "PENDING",
            delivery_session_id: null
          };
          dispatchCreated = true;
          pushEvent(status, "DISPATCH_CREATED", { dispatch_id: status.current_dispatch.dispatch_id, kind: "EXECUTE" });
        }
        break;
      case "SUPERSEDE_ATTEMPT":
        if (status.current_dispatch) status.current_dispatch.state = "SUPERSEDED";
        status.current_dispatch = {
          dispatch_id: `local-dispatch-${randomUUID()}`,
          kind: "EXECUTE",
          state: "PENDING",
          delivery_session_id: null
        };
        dispatchCreated = true;
        pushEvent(status, "DISPATCH_CREATED", { dispatch_id: status.current_dispatch.dispatch_id, kind: "EXECUTE" });
        break;
      default:
        throw upstream("unsupported local owner decision", "KIND_STATE_MISMATCH", "KIND_STATE_MISMATCH");
    }

    validateStatus(status);
    const result = {
      goal_state: status.goal.state,
      event_ordinal: event.ordinal,
      dispatch_created: dispatchCreated,
      dispatch_authorized: Boolean(status.goal.dispatch_authorized)
    };
    state.request_results[args.requestId] = result;
    return result;
  }

  withWrite(state, operation) {
    const result = operation();
    this.writeState(state);
    return result;
  }

  readState() {
    if (!existsSync(this.statePath)) return initialState();
    try {
      const parsed = JSON.parse(readFileSync(this.statePath, "utf8"));
      return normalizeState(parsed);
    } catch (error) {
      throw new UpstreamError("local controller state is unreadable", {
        deterministic: true,
        mappedCode: "CONTROLLER_UNREACHABLE",
        cause: error
      });
    }
  }

  writeState(state) {
    try {
      mkdirSync(dirname(this.statePath), { recursive: true, mode: 0o700 });
      writeFileSync(this.statePath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
    } catch (error) {
      throw new UpstreamError("local controller state is not writable", {
        deterministic: true,
        mappedCode: "CONTROLLER_UNREACHABLE",
        cause: error
      });
    }
  }
}

function defaultStatePath(env = process.env) {
  if (platform() === "win32" && env.LOCALAPPDATA) return join(env.LOCALAPPDATA, "gqb", "controller-state.json");
  if (env.XDG_STATE_HOME) return join(env.XDG_STATE_HOME, "gqb", "controller-state.json");
  return join(homedir(), ".gqb", "controller-state.json");
}

function initialState() {
  return { schema_version: 1, active_goal_id: null, goals: {}, request_results: {} };
}

function normalizeState(value) {
  return {
    schema_version: 1,
    active_goal_id: typeof value?.active_goal_id === "string" ? value.active_goal_id : null,
    goals: value?.goals && typeof value.goals === "object" ? value.goals : {},
    request_results: value?.request_results && typeof value.request_results === "object" ? value.request_results : {}
  };
}

function emptyStatus() {
  return { goal: null, tasks: [], current_dispatch: null, events: [] };
}

function visibleStatus(status, args) {
  const eventLimit = Math.min(Math.max(Number(args.eventLimit ?? args.event_limit ?? 100), 1), 500);
  return {
    goal: status.goal,
    tasks: status.tasks ?? [],
    current_dispatch: status.current_dispatch ?? null,
    events: args.includeEvents === false || args.include_events === false ? [] : (status.events ?? []).slice(-eventLimit)
  };
}

function validateRequestId(value) {
  if (typeof value !== "string" || value.length < 8) {
    throw upstream("requestId is required", "INVALID_REQUEST_ID", "INVALID_ARGUMENT");
  }
}

function validateGoalName(value) {
  if (typeof value !== "string" || value.length < 1 || value.length > 200) {
    throw upstream("goal name is invalid", "VALIDATION_ERROR", "INVALID_ARGUMENT");
  }
}

function validateTasks(tasks) {
  if (!Array.isArray(tasks) || tasks.length === 0) {
    throw upstream("at least one task is required", "INVALID_TASK_PLAN", "INVALID_ARGUMENT");
  }
  for (const task of tasks) {
    if (typeof task.jiraKey !== "string" || task.jiraKey.length === 0) {
      throw upstream("task jiraKey is required", "INVALID_TASK_PLAN", "INVALID_ARGUMENT");
    }
  }
}

function clampAttempts(value, { required = false } = {}) {
  if (required && typeof value === "undefined") throw upstream("max attempts is required", "VALIDATION_ERROR", "INVALID_ARGUMENT");
  const attempts = Number(value ?? 4);
  if (!Number.isInteger(attempts) || attempts < 1 || attempts > 10) {
    throw upstream("max attempts must be an integer from 1 to 10", "VALIDATION_ERROR", "INVALID_ARGUMENT");
  }
  return attempts;
}

function currentTask(tasks = []) {
  return tasks.find((task) => task.state === "active") ?? tasks.find((task) => task.state !== "completed") ?? tasks[0] ?? null;
}

function ensureDispatch(status) {
  if (status.current_dispatch) return status.current_dispatch;
  status.current_dispatch = {
    dispatch_id: `local-dispatch-${randomUUID()}`,
    kind: "EXECUTE",
    state: "PENDING",
    delivery_session_id: null
  };
  pushEvent(status, "DISPATCH_CREATED", { dispatch_id: status.current_dispatch.dispatch_id, kind: "EXECUTE" });
  return status.current_dispatch;
}

function liveDispatch(dispatch) {
  return dispatch && ["PENDING", "FIRED", "CLAIMED"].includes(dispatch.state);
}

function pushEvent(status, eventType, payload = {}) {
  const events = status.events ?? [];
  const event = {
    ordinal: events.length + 1,
    event_type: eventType,
    payload,
    ts: new Date().toISOString()
  };
  events.push(event);
  status.events = events;
  return event;
}

function validateStatus(status) {
  if (status.goal && !GOAL_STATES.has(status.goal.state)) {
    throw upstream("invalid local goal state", "VALIDATION_ERROR", "INVALID_ARGUMENT");
  }
  for (const task of status.tasks ?? []) {
    if (!TASK_STATES.has(task.state)) throw upstream("invalid local task state", "VALIDATION_ERROR", "INVALID_ARGUMENT");
  }
  if (status.current_dispatch && !DISPATCH_STATES.has(status.current_dispatch.state)) {
    throw upstream("invalid local dispatch state", "VALIDATION_ERROR", "INVALID_ARGUMENT");
  }
}

function upstream(message, upstreamCode, mappedCode) {
  return new UpstreamError(message, {
    deterministic: true,
    upstreamCode,
    mappedCode
  });
}

function shapeOf(value) {
  if (!value || typeof value !== "object") return typeof value;
  return {
    has_goal: Object.hasOwn(value, "goal"),
    has_tasks: Array.isArray(value.tasks),
    has_events: Array.isArray(value.events),
    keys: Object.keys(value).sort()
  };
}
