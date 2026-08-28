import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  authorizationRequestId,
  canonicalJson,
  deriveNextSafeAction,
  GigTrackQueueBridge,
  normalizeRequestId,
  payloadHash,
  SqliteJournal,
  statusFingerprint
} from "../src/gqb/index.js";

class FakeController {
  constructor() {
    this.calls = [];
    this.status = { goal: null, tasks: [], current_dispatch: null, events: [] };
    this.mutatorResult = { goal_state: "running", event_ordinal: 2, dispatch_created: true, dispatch_authorized: false };
  }

  async callTool(name, args) {
    this.calls.push({ name, args });
    if (name === "get_goal_status") return this.status;
    if (name === "submit_goal") {
      this.status = {
        goal: {
          goal_id: "goal-1",
          name: args.name,
          state: "running",
          max_attempts_per_task: args.policy.maxAttemptsPerTask,
          pending_question: null,
          blocked_reason: null,
          dispatch_authorized: false
        },
        tasks: args.tasks.map((task, index) => ({
          task_id: `task-${index + 1}`,
          position: task.position,
          jira_key: task.jiraKey,
          state: index === 0 ? "active" : "pending",
          attempts_used: 0
        })),
        current_dispatch: { dispatch_id: "disp-1", kind: "EXECUTE", state: "PENDING", delivery_session_id: null },
        events: []
      };
      return { goal_id: "goal-1", state: "running", tasks: this.status.tasks, created: true };
    }
    if (name === "submit_owner_decision") return this.mutatorResult;
    throw new Error(`unexpected tool ${name}`);
  }
}

function tempJournal() {
  const dir = mkdtempSync(join(tmpdir(), "gqb-test-"));
  return {
    path: join(dir, "journal.sqlite"),
    cleanup: () => rmSync(dir, { recursive: true, force: true })
  };
}

function runningStatus(overrides = {}) {
  return {
    goal: {
      goal_id: "goal-1",
      name: "Queue",
      state: "running",
      max_attempts_per_task: 4,
      pending_question: null,
      blocked_reason: null,
      dispatch_authorized: false,
      ...overrides.goal
    },
    tasks: overrides.tasks ?? [{ task_id: "task-1", position: 1, jira_key: "KAN-142", state: "active", attempts_used: 0 }],
    current_dispatch: Object.hasOwn(overrides, "current_dispatch") ? overrides.current_dispatch : null,
    events: overrides.events ?? []
  };
}

test("request IDs normalize into the GQB namespace and reserve gqb:auth", () => {
  assert.equal(normalizeRequestId("KAN-142-submit"), "gqb:KAN-142-submit");
  assert.equal(normalizeRequestId("gqb:KAN-142-submit"), "gqb:KAN-142-submit");
  assert.throws(() => normalizeRequestId("gqb:auth:123456789012345678901234"), /reserved/);
  assert.match(authorizationRequestId("gqb:KAN-142-submit"), /^gqb:auth:[a-f0-9]{24}$/);
});

test("canonical payload hashing is stable across object key order", () => {
  const a = payloadHash({ b: 2, a: { z: 1, y: [3, 2] } });
  const b = payloadHash({ a: { y: [3, 2], z: 1 }, b: 2 });
  assert.equal(a, b);
  assert.equal(canonicalJson({ b: 1, a: 2 }), "{\"a\":2,\"b\":1}");
});

test("fingerprint has fixed null fields and changes when dispatch identity changes", () => {
  const one = statusFingerprint(runningStatus({ current_dispatch: null }));
  assert.equal(one.projection.current_dispatch.dispatch_id, null);
  const two = statusFingerprint(runningStatus({ current_dispatch: { dispatch_id: "disp-2", state: "PENDING", delivery_session_id: null } }));
  assert.notEqual(one.value, two.value);
  assert.equal(two.value.length, 32);
});

test("next-safe-action separates active/no-live-dispatch ceiling rows", () => {
  const raise = deriveNextSafeAction(runningStatus({
    goal: { max_attempts_per_task: 4 },
    tasks: [{ task_id: "task-1", position: 1, jira_key: "KAN-142", state: "active", attempts_used: 4 }]
  }));
  assert.equal(raise.code, "RAISE_ATTEMPT_CEILING");
  const bound = deriveNextSafeAction(runningStatus({
    goal: { max_attempts_per_task: 10 },
    tasks: [{ task_id: "task-1", position: 1, jira_key: "KAN-142", state: "active", attempts_used: 10 }]
  }));
  assert.equal(bound.code, "ESCALATE_ATTEMPT_CEILING_AT_BOUND");
});

test("pending unauthorized dispatch asks for authorization", () => {
  const next = deriveNextSafeAction(runningStatus({ current_dispatch: { dispatch_id: "disp-1", state: "PENDING", delivery_session_id: null } }));
  assert.equal(next.code, "AUTHORIZE_DISPATCH");
});

test("handoff mutation is refused without upstream CAS or correlation capability", async () => {
  const bridge = new GigTrackQueueBridge({ controller: new FakeController(), journal: null, handoffCapability: false });
  const result = await bridge.queue_handoff({ request_id: "KAN-142-handoff", goal_id: "goal-1", reason: "worker recovery requested by owner" });
  assert.equal(result.ok, false);
  assert.equal(result.error_code, "UPSTREAM_CAPABILITY_REQUIRED");
  assert.equal(result.effect_status, "NOT_APPLIED");
});

test("queue_submit journals presubmit intent and deterministic authorization entry after goal id resolves", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    const controller = new FakeController();
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const result = await bridge.queue_submit({
      request_id: "KAN-142-submit",
      name: "Build Queue Bridge",
      ordered_jira_keys: ["KAN-142"],
      authorize_dispatch: true,
      decision_text: "Owner authorizes initial dispatch."
    });
    assert.equal(result.ok, true);
    assert.equal(result.goal_id, "goal-1");
    assert.match(result.data.authorization_request_id, /^gqb:auth:[a-f0-9]{24}$/);
    assert.ok(journal.getEntry("__PRESUBMIT__:gqb:KAN-142-submit", "gqb:KAN-142-submit"));
    assert.ok(journal.getEntry("goal-1", result.data.authorization_request_id));
    assert.equal(controller.calls.filter((call) => call.name === "submit_owner_decision").length, 1);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("queue_resume refuses stale fingerprint before journaling intent", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    const controller = new FakeController();
    controller.status = runningStatus({ current_dispatch: null });
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const result = await bridge.queue_resume({
      request_id: "KAN-142-resume",
      goal_id: "goal-1",
      kind: "RESUME",
      decision_text: "resume queue",
      expected_fingerprint: "not-current"
    });
    assert.equal(result.error_code, "STALE_FINGERPRINT");
    assert.equal(journal.listOpenIntents("goal-1").length, 0);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("queue_resume ceiling gate matches preflight gate", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    const controller = new FakeController();
    controller.status = runningStatus({
      goal: { state: "blocked", max_attempts_per_task: 10 },
      tasks: [{ task_id: "task-1", position: 1, jira_key: "KAN-142", state: "blocked", attempts_used: 10 }]
    });
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const preflight = await bridge.queue_preflight({ operation: "RESUME", goal_id: "goal-1", args: {} });
    const resume = await bridge.queue_resume({ request_id: "KAN-142-resume", goal_id: "goal-1", kind: "RESUME", decision_text: "resume queue" });
    assert.equal(preflight.error_code, "ATTEMPT_CEILING_AT_BOUND");
    assert.equal(resume.error_code, "ATTEMPT_CEILING_AT_BOUND");
    assert.equal(resume.next_safe_action.code, "ESCALATE_ATTEMPT_CEILING_AT_BOUND");
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});
