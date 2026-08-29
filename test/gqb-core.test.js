import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  authorizationRequestId,
  canonicalJson,
  ControllerClient,
  DiagnosticsLogger,
  deriveNextSafeAction,
  GigTrackQueueBridge,
  normalizeRequestId,
  payloadHash,
  JournalError,
  rcaAssumptions,
  scanCodexConfig,
  SqliteJournal,
  statusFingerprint
} from "../src/gqb/index.js";

class FakeController {
  constructor() {
    this.calls = [];
    this.status = { goal: null, tasks: [], current_dispatch: null, events: [] };
    this.mutatorResult = { goal_state: "running", event_ordinal: 2, dispatch_created: true, dispatch_authorized: false };
  }

  describeTransport() {
    return { kind: "socket", socket_path: "/tmp/controller.sock" };
  }

  async ping() {
    const result = await this.callTool("get_goal_status", { includeEvents: false, eventLimit: 1 });
    return {
      reachable: true,
      diagnosis: null,
      upstream_shape: { has_goal: Object.hasOwn(result, "goal"), has_tasks: Array.isArray(result.tasks), has_events: Array.isArray(result.events) },
      idle_goal_null_accepted: result.goal === null
    };
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

class SequenceController {
  constructor(responses) {
    this.responses = [...responses];
    this.calls = [];
  }

  async callTool(name, args) {
    this.calls.push({ name, args });
    const next = this.responses.shift();
    if (next instanceof Error) throw next;
    if (typeof next === "function") return next(name, args, this);
    return next;
  }
}

class FailingController {
  constructor(error) {
    this.error = error;
  }

  describeTransport() {
    return { kind: "socket", socket_path: "/missing/controller.sock" };
  }

  async ping() {
    return { reachable: false, diagnosis: "CONTROLLER_UNREACHABLE", message: this.error.message };
  }

  async callTool() {
    throw this.error;
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

test("controller client has no implicit Replit socket default", async () => {
  const controller = new ControllerClient();
  assert.deepEqual(controller.describeTransport(), { kind: "none" });
  await assert.rejects(
    () => controller.callTool("get_goal_status", {}),
    (error) => {
      assert.equal(error.mappedCode, "CONTROLLER_UNCONFIGURED");
      assert.equal(error.deterministic, true);
      return true;
    }
  );
});

test("diagnostics are durable, attributed, redacted, and cycle-safe", () => {
  const dir = mkdtempSync(join(tmpdir(), "gqb-diag-test-"));
  try {
    const stderr = [];
    const logger = new DiagnosticsLogger({
      dir,
      origin: "codex_client",
      instanceId: "test-instance",
      pid: 123,
      stderrWriter: (line) => stderr.push(line)
    });
    const details = { token: "secret-value", endpoint: "postgres://user:password@db.example/gqb" };
    details.self = details;
    logger.event("gqb.test", { traceId: "trace-1", details });
    const persisted = JSON.parse(readFileSync(logger.filePath, "utf8").trim());
    assert.equal(persisted.origin, "codex_client");
    assert.equal(persisted.trace_id, "trace-1");
    assert.equal(persisted.details.token, "[REDACTED]");
    assert.equal(persisted.details.self, "[Circular]");
    assert.match(persisted.details.endpoint, /\[REDACTED\]/);
    assert.equal(JSON.parse(stderr[0]).event, "gqb.test");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("doctor config scan handles quoted server names and missing files", () => {
  const dir = mkdtempSync(join(tmpdir(), "gqb-config-test-"));
  try {
    const configPath = join(dir, "config.toml");
    writeFileSync(configPath, '[mcp_servers."gigtrack_queue_bridge"] # generated\ncommand = "node"\n');
    const found = scanCodexConfig(configPath);
    assert.equal(found.valid, true);
    assert.deepEqual(found.matching_servers, ["gigtrack_queue_bridge"]);
    const missing = scanCodexConfig(join(dir, "missing.toml"));
    assert.equal(missing.missing, true);
    assert.equal(missing.readable, false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("controller client rejects unsupported URL schemes and ambiguous transport config", async () => {
  const unsupported = new ControllerClient({ url: "ftp://controller.example/mcp" });
  assert.equal(unsupported.describeTransport().kind, "invalid_url");
  await assert.rejects(
    () => unsupported.callTool("get_goal_status", {}),
    (error) => {
      assert.equal(error.mappedCode, "CONTROLLER_UNCONFIGURED");
      return true;
    }
  );

  const ambiguous = new ControllerClient({ socketPath: "/tmp/controller.sock", url: "http://controller.example/mcp" });
  assert.equal(ambiguous.describeTransport().kind, "ambiguous");
  await assert.rejects(
    () => ambiguous.callTool("get_goal_status", {}),
    (error) => {
      assert.equal(error.mappedCode, "CONTROLLER_CONFIG_AMBIGUOUS");
      assert.equal(error.name, "UpstreamError");
      assert.equal(error.message, "controller socket and URL are both configured");
      return true;
    }
  );
});

test("queue_channel_health cannot report ok without controller transport", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({ controller: new ControllerClient(), journal });
    const result = await bridge.queue_channel_health();
    assert.equal(result.ok, false);
    assert.equal(result.error_code, "CONTROLLER_UNCONFIGURED");
    assert.equal(result.data.journal.available, true);
    assert.equal(result.data.controller.reachable, false);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("queue_channel_health pings controller before reporting ready", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    const controller = new FakeController();
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const result = await bridge.queue_channel_health();
    assert.equal(result.ok, true);
    assert.equal(result.error_code, null);
    assert.equal(result.data.controller.reachable, true);
    assert.equal(controller.calls[0].name, "get_goal_status");
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("queue_channel_health reports unreachable controller as not ready", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({
      controller: new FailingController(Object.assign(new Error("connect ENOENT /missing/controller.sock"), {
        mappedCode: "CONTROLLER_UNREACHABLE",
        deterministic: true
      })),
      journal
    });
    const result = await bridge.queue_channel_health();
    assert.equal(result.ok, false);
    assert.equal(result.error_code, "CONTROLLER_UNREACHABLE");
    assert.equal(result.data.controller.reachable, false);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("doctor RCA assumptions preserve unknown instead of false green booleans", () => {
  const assumptions = rcaAssumptions({
    configScan: { readable: true, valid: false, reason: "missing" },
    launchProbe: { ok: true, diagnosis: null },
    nodePath: { available: false },
    health: {
      ok: false,
      error_code: "CONTROLLER_UNCONFIGURED",
      data: {
        controller: {
          ping_attempted: false,
          reachable: false,
          diagnosis: "CONTROLLER_UNCONFIGURED"
        }
      }
    }
  });
  assert.equal(assumptions.config_entry_missing_or_invalid.verdict, "confirmed");
  assert.equal(assumptions.launch_command_unresolved.verdict, "unknown");
  assert.equal(assumptions.controller_transport_unconfigured.verdict, "confirmed");
  assert.equal(assumptions.controller_config_ambiguous.verdict, "refuted");
  assert.equal(assumptions.controller_unreachable.verdict, "unknown");
  assert.equal(assumptions.health_false_green_detected.verdict, "unknown");
  assert.equal(assumptions.node_unavailable_on_path.verdict, "confirmed");
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

test("queue_status fetches internal 500-event window even when events are not echoed", async () => {
  const controller = new FakeController();
  controller.status = runningStatus({
    goal: { state: "blocked", blocked_reason: "owner pause" },
    tasks: [{ task_id: "task-1", position: 1, jira_key: "KAN-142", state: "blocked", attempts_used: 0 }],
    events: [{ ordinal: 1, event_type: "OWNER_DECISION", payload: { kind: "PAUSE", decision_text: "owner pause" } }]
  });
  const bridge = new GigTrackQueueBridge({ controller });
  const result = await bridge.queue_status({ include_events: false });
  assert.equal(result.next_safe_action.code, "NO_ACTION_OWNER_PAUSED");
  assert.equal(result.data.status.events.length, 0);
  assert.equal(controller.calls[0].args.includeEvents, true);
  assert.equal(controller.calls[0].args.eventLimit, 500);
});

test("submit refuses unknown upstream state before journaling or calling submit_goal", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    const controller = new FakeController();
    controller.status = runningStatus({ goal: { state: "mystery" } });
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const result = await bridge.queue_submit({ request_id: "KAN-142-unknown", name: "Build", ordered_jira_keys: ["KAN-142"] });
    assert.equal(result.error_code, "ESCALATE_UNKNOWN_STATE");
    assert.equal(journal.listOpenIntents().length, 0);
    assert.equal(controller.calls.some((call) => call.name === "submit_goal"), false);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("created=false submit with failed post-read stays indeterminate and does not authorize", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    journal = new SqliteJournal({ path: tmp.path }).open();
    const controller = new SequenceController([
      { goal: null, tasks: [], current_dispatch: null, events: [] },
      { goal_id: "goal-foreign", state: "running", tasks: [], created: false },
      new Error("post read failed")
    ]);
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const result = await bridge.queue_submit({
      request_id: "KAN-142-false",
      name: "Build Queue Bridge",
      ordered_jira_keys: ["KAN-142"],
      authorize_dispatch: true,
      decision_text: "Owner authorizes initial dispatch."
    });
    assert.equal(result.error_code, "UPSTREAM_INDETERMINATE");
    assert.equal(result.effect_status, "INDETERMINATE");
    assert.equal(controller.calls.filter((call) => call.name === "submit_owner_decision").length, 0);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("reconcile replay refuses ambiguous submit when an unrelated active goal exists", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    journal = new SqliteJournal({ path: tmp.path }).open();
    journal.createIntent({
      goal_key: "__PRESUBMIT__:gqb:KAN-142-replay",
      upstream_request_id: "gqb:KAN-142-replay",
      caller_request_id: "KAN-142-replay",
      tool: "submit_goal",
      payload_hash: "hash",
      upstream_arguments: {
        requestId: "gqb:KAN-142-replay",
        name: "Build Queue Bridge",
        tasks: [{ jiraKey: "KAN-142", position: 1 }],
        policy: { maxAttemptsPerTask: 4 }
      },
      trace_id: "trace"
    });
    const controller = new FakeController();
    controller.status = runningStatus({ goal: { goal_id: "other-goal" }, events: [] });
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const result = await bridge.queue_reconcile({
      mode: "REPLAY",
      presubmit_request_id: "gqb:KAN-142-replay",
      upstream_request_id: "gqb:KAN-142-replay"
    });
    assert.equal(result.error_code, "AMBIGUOUS_SUBMIT_OUTCOME");
    assert.equal(controller.calls.some((call) => call.name === "submit_goal"), false);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("lost lock lease after upstream success is indeterminate and points to reconcile", async () => {
  const journal = {
    ensureOpen() {},
    async acquireLock() { return { lockKey: "goal:goal-1", ownerToken: "lost" }; },
    releaseLock() {},
    getEntry() { return null; },
    createIntent() {},
    writeOutcome() {
      throw new JournalError("lease lost", "LOCK_LEASE_LOST");
    },
    listOpenIntents() { return []; }
  };
  const controller = new FakeController();
  controller.status = runningStatus({ current_dispatch: null });
  const bridge = new GigTrackQueueBridge({ controller, journal });
  const result = await bridge.queue_resume({ request_id: "KAN-142-lease", goal_id: "goal-1", kind: "RESUME", decision_text: "resume queue" });
  assert.equal(result.error_code, "LOCK_LEASE_LOST");
  assert.equal(result.effect_status, "INDETERMINATE");
  assert.equal(result.next_safe_action.code, "RECONCILE_BRIDGE_UNCERTAINTY");
});

test("resume gate returns closed error codes for in-flight and invalid task target", async () => {
  const tmp = tempJournal();
  let journal;
  try {
    const controller = new FakeController();
    controller.status = runningStatus({ current_dispatch: { dispatch_id: "disp-1", state: "CLAIMED", delivery_session_id: "s1" } });
    journal = new SqliteJournal({ path: tmp.path }).open();
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const inFlight = await bridge.queue_resume({ request_id: "KAN-142-inflight", goal_id: "goal-1", kind: "RESUME", decision_text: "resume queue" });
    assert.equal(inFlight.error_code, "NOOP_ALREADY_IN_FLIGHT");

    controller.status = runningStatus({ current_dispatch: null });
    const invalidTask = await bridge.queue_resume({
      request_id: "KAN-142-badtask",
      goal_id: "goal-1",
      kind: "RESUME",
      decision_text: "resume queue",
      task_id: "missing-task"
    });
    assert.equal(invalidTask.error_code, "INVALID_ARGUMENT");
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});
