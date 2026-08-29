import { randomUUID } from "node:crypto";
import { CHANNEL, EFFECT_STATUS } from "./constants.js";
import { ControllerClient, UpstreamError } from "./controller-client.js";
import { DiagnosticsLogger, nullLogger } from "./diagnostics.js";
import { LOCAL_CONTROLLER_MODE, LocalController } from "./local-controller.js";
import { statusFingerprint } from "./fingerprint.js";
import { JournalError, SqliteJournal } from "./journal.js";
import { queueControl, queueResume, resumeGateVerdict } from "./operations/owner-decision.js";
import { queueReconcile } from "./operations/reconcile.js";
import { queueSubmit } from "./operations/submit.js";
import { action, deriveNextSafeAction, eventsComplete } from "./next-safe-action.js";
import { envelope } from "./response.js";
import { ValidationError } from "./request-id.js";
import { findVocabularyDrift } from "./vocabulary.js";

export class GigTrackQueueBridge {
  constructor({
    controller = new ControllerClient(),
    journal,
    channel = CHANNEL.LOCAL_SOCKET,
    handoffCapability = false,
    ownershipChannelAvailable = true,
    logger = nullLogger(),
    launchSource = "unknown"
  } = {}) {
    this.controller = controller;
    this.journal = journal;
    this.channel = channel;
    this.handoffCapability = handoffCapability;
    this.ownershipChannelAvailable = ownershipChannelAvailable;
    this.logger = logger;
    this.launchSource = launchSource;
  }

  static fromEnv(env = process.env, { logger = null } = {}) {
    const diagnostics = logger ?? new DiagnosticsLogger({ origin: env.GQB_LAUNCH_SOURCE || "unknown" });
    const journal = env.GQB_JOURNAL_PATH ? new SqliteJournal({ path: env.GQB_JOURNAL_PATH }) : null;
    const controller = controllerFromEnv(env, { logger: diagnostics });
    const transport = controller.describeTransport();
    diagnostics.event("gqb.transport.selected", {
      diagnosis: transportDiagnosis(transport),
      details: {
        transport,
        env_present: {
          GQB_CONTROLLER_SOCKET: Boolean(env.GQB_CONTROLLER_SOCKET),
          GQB_CONTROLLER_URL: Boolean(env.GQB_CONTROLLER_URL),
          GQB_CONTROLLER_MODE: env.GQB_CONTROLLER_MODE ?? null,
          GQB_ALLOW_DEV_LOCAL_CONTROLLER: env.GQB_ALLOW_DEV_LOCAL_CONTROLLER === "true",
          GQB_LOCAL_CONTROLLER_STATE_PATH: Boolean(env.GQB_LOCAL_CONTROLLER_STATE_PATH),
          GQB_JOURNAL_PATH: Boolean(env.GQB_JOURNAL_PATH),
          GQB_DIAG_DIR: Boolean(env.GQB_DIAG_DIR),
          GQB_LAUNCH_SOURCE: Boolean(env.GQB_LAUNCH_SOURCE)
        },
        launch_origin: env.GQB_LAUNCH_SOURCE || "unknown"
      }
    });
    return new GigTrackQueueBridge({
      controller,
      journal,
      channel: channelForTransport(transport),
      handoffCapability: env.GQB_HANDOFF_EXPECTED_DISPATCH_CAPABILITY === "true",
      logger: diagnostics,
      launchSource: env.GQB_LAUNCH_SOURCE || "unknown"
    });
  }

  requireJournal() {
    if (!this.journal) throw new JournalError("journal is required for mutating tools", "JOURNAL_UNAVAILABLE");
    this.journal.ensureOpen();
  }

  async readStatus({ goal_id = null, include_events = false, event_limit = 100 } = {}) {
    const requestedEventLimit = Math.min(Math.max(Number(event_limit || 100), 1), 500);
    const args = {
      includeEvents: true,
      eventLimit: 500
    };
    if (goal_id) args.goalId = goal_id;
    const upstream = await this.controller.callTool("get_goal_status", args, { timeoutMs: 10000 });
    const eventLimit = 500;
    const events = upstream.events ?? [];
    const normalized = {
      ...upstream,
      events: include_events ? events.slice(0, requestedEventLimit) : [],
      events_complete: eventsComplete(events, eventLimit),
      vocabulary_drift: findVocabularyDrift(upstream)
    };
    normalized.fingerprint = statusFingerprint(upstream);
    const goalKey = normalized.goal?.goal_id ?? null;
    const openIntent = goalKey && this.journal ? this.journal.listOpenIntents(goalKey).length > 0 : false;
    normalized.next_safe_action = deriveNextSafeAction(upstream, {
      ownershipChannelAvailable: this.ownershipChannelAvailable,
      openIntent,
      eventLimit,
      handoffCapability: this.handoffCapability
    });
    return normalized;
  }

  async queue_status(args = {}) {
    const traceId = randomUUID();
    try {
      const status = await this.readStatus(args);
      return envelope({
        ok: true,
        goal_id: status.goal?.goal_id ?? null,
        fingerprint: status.fingerprint,
        next_safe_action: status.next_safe_action,
        trace_id: traceId,
        data: {
          status,
          channel: this.channel,
          bridge_uncertainty: status.next_safe_action.code === "RECONCILE_BRIDGE_UNCERTAINTY"
        }
      });
    } catch (error) {
      return this.errorEnvelope(error, traceId);
    }
  }

  async queue_preflight(args = {}) {
    const traceId = randomUUID();
    try {
      const operation = args.operation;
      const status = operation === "SUBMIT" ? null : await this.readStatus({ goal_id: args.goal_id, include_events: true, event_limit: 500 });
      if (operation === "HANDOFF" && !this.handoffCapability) {
        return envelope({
          error_code: "UPSTREAM_CAPABILITY_REQUIRED",
          effect_status: EFFECT_STATUS.NOT_APPLIED,
          goal_id: args.goal_id ?? null,
          fingerprint: status?.fingerprint ?? null,
          next_safe_action: status?.next_safe_action ?? action("WAIT_FOR_WORKER", "recovery_requires_upstream_capability", { notes: "RECOVERY_REQUIRES_UPSTREAM_CAPABILITY" }),
          trace_id: traceId,
          data: { would_admit: false, gates: [{ code: "UPSTREAM_CAPABILITY_REQUIRED", passed: false }] }
        });
      }
      if (operation === "RESUME" || operation === "ANSWER") {
        const verdict = resumeGateVerdict(status, operation, args.args?.task_id);
        return envelope({
          ok: verdict.ok,
          error_code: verdict.error_code,
          effect_status: verdict.ok ? null : EFFECT_STATUS.NOT_APPLIED,
          goal_id: status.goal?.goal_id ?? null,
          fingerprint: status.fingerprint,
          next_safe_action: verdict.next_safe_action ?? status.next_safe_action,
          trace_id: traceId,
          data: { would_admit: verdict.ok, gates: verdict.gates, projected_effect: verdict.projected_effect ?? null }
        });
      }
      return envelope({
        ok: true,
        goal_id: status?.goal?.goal_id ?? null,
        fingerprint: status?.fingerprint ?? null,
        next_safe_action: status?.next_safe_action ?? null,
        trace_id: traceId,
        data: { would_admit: true, gates: [], projected_effect: operation }
      });
    } catch (error) {
      return this.errorEnvelope(error, traceId);
    }
  }

  async queue_submit(args = {}) {
    return queueSubmit(this, args);
  }

  async queue_resume(args = {}) {
    return queueResume(this, args);
  }

  async queue_control(args = {}) {
    return queueControl(this, args);
  }

  async queue_handoff(args = {}) {
    const traceId = randomUUID();
    return envelope({
      error_code: "UPSTREAM_CAPABILITY_REQUIRED",
      effect_status: EFFECT_STATUS.NOT_APPLIED,
      goal_id: args.goal_id ?? null,
      next_safe_action: action("WAIT_FOR_WORKER", "recovery_requires_upstream_capability", { notes: "RECOVERY_REQUIRES_UPSTREAM_CAPABILITY" }),
      trace_id: traceId,
      data: {
        mutation_enabled: false,
        expected_dispatch_cas_or_event_correlation: this.handoffCapability
      }
    });
  }

  async queue_reconcile(args = {}) {
    return queueReconcile(this, args);
  }

  async queue_channel_health() {
    const traceId = randomUUID();
    let journal = { configured: Boolean(this.journal), available: false, open_intents: null, error: null };
    const transport = this.controller.describeTransport();
    this.logger.event("gqb.transport.selected", {
      traceId,
      diagnosis: transportDiagnosis(transport),
      details: { transport, launch_origin: this.launchSource }
    });
    try {
      if (this.journal) {
        this.journal.ensureOpen();
        const openIntents = this.journal.listOpenIntents();
        journal = {
          configured: true,
          available: true,
          open_intents: openIntents.length,
          oldest_open_intent_age_ms: openIntents.length ? Date.now() - Date.parse(openIntents[0].created_at) : null
        };
      }
    } catch (error) {
      this.logger.event("gqb.journal.failed", {
        traceId,
        level: "warn",
        diagnosis: "JOURNAL_UNAVAILABLE",
        details: { code: error.code ?? null, message: error.message }
      });
      journal = { configured: true, available: false, open_intents: null, error: error.code ?? "JOURNAL_ERROR" };
    }
    let controller;
    try {
      controller = await this.controller.ping({ timeoutMs: 3000, traceId });
    } catch (error) {
      controller = {
        ping_attempted: false,
        reachable: false,
        diagnosis: error.mappedCode ?? "CONTROLLER_UNREACHABLE",
        message: error.message,
        upstream_code: error.upstreamCode ?? null
      };
      this.logger.event("gqb.controller.ping.failed", {
        traceId,
        level: "warn",
        diagnosis: controller.diagnosis,
        details: { message: error.message, code: error.code ?? null }
      });
    }
    this.logger.event("gqb.controller.ping.completed", {
      traceId,
      level: controller.reachable ? "info" : "warn",
      diagnosis: controller.diagnosis,
      details: {
        transport,
        reachable: controller.reachable,
        ping_attempted: controller.ping_attempted ?? null,
        idle_goal_null_accepted: controller.idle_goal_null_accepted ?? false,
        upstream_shape: controller.upstream_shape ?? null,
        upstream_code: controller.upstream_code ?? null,
        message: controller.message ?? null
      }
    });
    const ok = Boolean(journal.available && this.ownershipChannelAvailable && controller.reachable);
    const errorCode = controller.diagnosis
      ?? (!journal.configured
        ? "JOURNAL_UNCONFIGURED"
        : (!journal.available
            ? "JOURNAL_UNAVAILABLE"
            : (!this.ownershipChannelAvailable ? "OWNERSHIP_CHANNEL_UNAVAILABLE" : null)));
    this.logger.event("gqb.health.completed", {
      traceId,
      level: ok ? "info" : "warn",
      diagnosis: ok ? null : errorCode,
      details: {
        ok,
        journal_available: journal.available,
        ownership_channel_available: this.ownershipChannelAvailable,
        controller_reachable: controller.reachable,
        blocking_mutators: !ok
      }
    });
    return envelope({
      ok,
      error_code: ok ? null : errorCode,
      trace_id: traceId,
      data: {
        channel: this.channel,
        ownership_channel_available: this.ownershipChannelAvailable,
        controller_socket: this.controller.socketPath ?? null,
        controller_transport: transport,
        controller: {
          ping_attempted: controller.ping_attempted ?? transportCanAttempt(transport),
          reachable: controller.reachable,
          diagnosis: controller.diagnosis,
          message: controller.message ?? null,
          idle_goal_null_accepted: controller.idle_goal_null_accepted ?? false,
          upstream_shape: controller.upstream_shape ?? null
        },
        journal,
        handoff_capabilities: {
          expected_dispatch_cas_or_event_correlation: this.handoffCapability,
          mutating_handoff_enabled: false
        }
      }
    });
  }

  async callMutator(tool, args, goalKey, upstreamRequestId, lock) {
    try {
      return await this.controller.callTool(tool, args);
    } catch (error) {
      if (error instanceof UpstreamError && error.deterministic) {
        this.journal.writeOutcome(goalKey, upstreamRequestId, {
          upstream_result: error.upstreamError,
          effect_status: EFFECT_STATUS.NOT_APPLIED
        }, lock);
      }
      throw error;
    }
  }

  async tryPostRead(goalId) {
    try {
      return { failed: false, status: await this.readStatus({ goal_id: goalId, include_events: true, event_limit: 500 }) };
    } catch {
      return { failed: true, status: null };
    }
  }

  cachedEnvelope(entry, traceId) {
    return envelope({
      ok: [EFFECT_STATUS.APPLIED, EFFECT_STATUS.ALREADY_APPLIED_OR_NOOP].includes(entry.effect_status),
      effect_status: entry.effect_status,
      goal_id: entry.goal_id,
      fingerprint: entry.post_status_snapshot?.fingerprint ?? null,
      next_safe_action: entry.post_status_snapshot?.next_safe_action ?? null,
      replayed: true,
      trace_id: traceId,
      data: { upstream_result: entry.upstream_result, journal_entry: entry }
    });
  }

  reconcileEnvelope(entry, traceId) {
    return envelope({
      error_code: "RECONCILE_BRIDGE_UNCERTAINTY",
      effect_status: EFFECT_STATUS.INDETERMINATE,
      goal_id: entry.goal_id,
      next_safe_action: action("RECONCILE_BRIDGE_UNCERTAINTY", "open_bridge_intent"),
      trace_id: traceId,
      data: { journal_entry: entry }
    });
  }

  errorEnvelope(error, traceId, context = {}) {
    if (context.tool && (error instanceof JournalError || error instanceof UpstreamError)) {
      this.logger.event("gqb.mutator.blocked_by_health", {
        traceId,
        level: "warn",
        diagnosis: error.code ?? error.mappedCode ?? "MUTATOR_BLOCKED",
        details: {
          tool: context.tool,
          goal_id_present: Boolean(context.goalId),
          health_code: error.code ?? error.mappedCode ?? null
        }
      });
    }
    if (error instanceof JournalError && error.code === "LOCK_LEASE_LOST") {
      return envelope({
        error_code: "LOCK_LEASE_LOST",
        effect_status: EFFECT_STATUS.INDETERMINATE,
        next_safe_action: action("RECONCILE_BRIDGE_UNCERTAINTY", "lock_lease_lost"),
        trace_id: traceId,
        data: { message: error.message }
      });
    }
    if (error instanceof ValidationError || error instanceof JournalError) {
      return envelope({
        error_code: error.code ?? "INVALID_ARGUMENT",
        effect_status: EFFECT_STATUS.NOT_APPLIED,
        trace_id: traceId,
        data: { message: error.message }
      });
    }
    if (error instanceof UpstreamError) {
      const errorCode = error.timeout ? "UPSTREAM_TIMEOUT_INDETERMINATE" : error.mappedCode ?? "UPSTREAM_INDETERMINATE";
      return envelope({
        error_code: errorCode,
        effect_status: error.deterministic ? EFFECT_STATUS.NOT_APPLIED : EFFECT_STATUS.INDETERMINATE,
        next_safe_action: error.deterministic ? null : action("RECONCILE_BRIDGE_UNCERTAINTY", "upstream_indeterminate"),
        trace_id: traceId,
        data: { message: error.message, upstream_code: error.upstreamCode ?? null }
      });
    }
    return envelope({
      error_code: "UPSTREAM_INDETERMINATE",
      effect_status: EFFECT_STATUS.INDETERMINATE,
      next_safe_action: action("RECONCILE_BRIDGE_UNCERTAINTY", "unexpected_error"),
      trace_id: traceId,
      data: { message: error?.message ?? String(error) }
    });
  }
}

function transportDiagnosis(transport) {
  if (transport.kind === "none" || transport.kind === "invalid_url") return "CONTROLLER_UNCONFIGURED";
  if (transport.kind === "ambiguous") return "CONTROLLER_CONFIG_AMBIGUOUS";
  if (transport.kind === "embedded_local_disabled") return "CONTROLLER_DEV_MODE_REQUIRED";
  return null;
}

function transportCanAttempt(transport) {
  return transport.kind === "socket" || transport.kind === "http" || transport.kind === "existing_remote" || transport.kind === LOCAL_CONTROLLER_MODE;
}

function controllerFromEnv(env, { logger }) {
  if (env.GQB_CONTROLLER_MODE === LOCAL_CONTROLLER_MODE) {
    if (env.GQB_CONTROLLER_SOCKET || env.GQB_CONTROLLER_URL) {
      return new ConfigConflictController({
        logger,
        message: "embedded local controller mode cannot be combined with controller socket or URL",
        transport: {
          kind: "ambiguous",
          mode_configured: LOCAL_CONTROLLER_MODE,
          socket_configured: Boolean(env.GQB_CONTROLLER_SOCKET),
          url_configured: Boolean(env.GQB_CONTROLLER_URL),
          reason: "local_mode_with_external_controller"
        }
      });
    }
    if (env.GQB_ALLOW_DEV_LOCAL_CONTROLLER !== "true") {
      return new DisabledLocalController({
        logger,
        message: "embedded local controller is dev/test only; set GQB_ALLOW_DEV_LOCAL_CONTROLLER=true to opt in",
        transport: {
          kind: "embedded_local_disabled",
          mode_configured: LOCAL_CONTROLLER_MODE,
          reason: "dev_opt_in_required",
          state_path_configured: Boolean(env.GQB_LOCAL_CONTROLLER_STATE_PATH)
        }
      });
    }
    return LocalController.fromEnv(env, { logger });
  }
  return ControllerClient.fromEnv(env, { logger });
}

function channelForTransport(transport) {
  if (transport.kind === LOCAL_CONTROLLER_MODE) return CHANNEL.EMBEDDED_LOCAL;
  return CHANNEL.LOCAL_SOCKET;
}

class ConfigConflictController {
  constructor({ transport, message, logger = nullLogger() }) {
    this.transport = transport;
    this.message = message;
    this.logger = logger;
  }

  describeTransport() {
    return this.transport;
  }

  async ping() {
    return {
      reachable: false,
      ping_attempted: false,
      diagnosis: "CONTROLLER_CONFIG_AMBIGUOUS",
      message: this.message
    };
  }

  async callTool() {
    throw new UpstreamError(this.message, {
      deterministic: true,
      mappedCode: "CONTROLLER_CONFIG_AMBIGUOUS",
      preSend: true,
      transport: this.transport
    });
  }
}

class DisabledLocalController {
  constructor({ transport, message, logger = nullLogger() }) {
    this.transport = transport;
    this.message = message;
    this.logger = logger;
  }

  describeTransport() {
    return this.transport;
  }

  async ping() {
    return {
      reachable: false,
      ping_attempted: false,
      diagnosis: "CONTROLLER_DEV_MODE_REQUIRED",
      message: this.message
    };
  }

  async callTool() {
    throw new UpstreamError(this.message, {
      deterministic: true,
      mappedCode: "CONTROLLER_DEV_MODE_REQUIRED",
      preSend: true,
      transport: this.transport
    });
  }
}
