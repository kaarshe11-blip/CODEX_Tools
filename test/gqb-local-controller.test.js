import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  GigTrackQueueBridge,
  LocalController,
  rcaAssumptions,
  SqliteJournal
} from "../src/gqb/index.js";

function tempPaths() {
  const dir = mkdtempSync(join(tmpdir(), "gqb-local-controller-"));
  return {
    journalPath: join(dir, "journal.sqlite"),
    statePath: join(dir, "state.json"),
    cleanup: () => rmSync(dir, { recursive: true, force: true })
  };
}

test("embedded local controller makes health ready without socket or URL", async () => {
  const tmp = tempPaths();
  let journal;
  try {
    journal = new SqliteJournal({ path: tmp.journalPath }).open();
    const controller = new LocalController({ statePath: tmp.statePath });
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const health = await bridge.queue_channel_health();
    assert.equal(health.ok, true);
    assert.equal(health.error_code, null);
    assert.equal(health.data.controller_transport.kind, "embedded_local");
    assert.equal(health.data.controller.ping_attempted, true);

    const status = await bridge.queue_status();
    assert.equal(status.ok, true);
    assert.equal(status.data.status.goal, null);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("embedded local controller can submit and authorize a local queue goal", async () => {
  const tmp = tempPaths();
  let journal;
  try {
    journal = new SqliteJournal({ path: tmp.journalPath }).open();
    const controller = new LocalController({ statePath: tmp.statePath });
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const submit = await bridge.queue_submit({
      request_id: "KAN-142-local-submit",
      name: "Local queue goal",
      ordered_jira_keys: ["KAN-142"],
      authorize_dispatch: true,
      decision_text: "Owner authorizes the local embedded controller dispatch."
    });
    assert.equal(submit.ok, true);
    assert.equal(submit.effect_status, "APPLIED");
    assert.equal(submit.data.authorization_status, "APPLIED");

    const status = await bridge.queue_status({ goal_id: submit.goal_id, include_events: true });
    assert.equal(status.ok, true);
    assert.equal(status.data.status.goal.goal_id, submit.goal_id);
    assert.equal(status.data.status.goal.dispatch_authorized, true);
    assert.equal(status.data.status.events.some((event) => event.event_type === "OWNER_DECISION"), true);
  } finally {
    journal?.close();
    tmp.cleanup();
  }
});

test("doctor RCA false-green verdict is cleanly refuted after a successful ping", () => {
  const assumptions = rcaAssumptions({
    configScan: { missing: false, readable: true, valid: true, reason: null },
    launchProbe: { ok: true, diagnosis: null },
    nodePath: { available: true },
    health: {
      ok: true,
      error_code: null,
      data: {
        controller: {
          ping_attempted: true,
          reachable: true,
          diagnosis: null
        }
      }
    }
  });
  assert.equal(assumptions.health_false_green_detected.verdict, "refuted");
  assert.equal(assumptions.health_false_green_detected.reason, null);
});
