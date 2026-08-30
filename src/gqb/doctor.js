import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { homedir } from "node:os";
import { fileURLToPath } from "node:url";
import { DiagnosticsLogger, diagnosticsDirFromEnv, nullLogger } from "./diagnostics.js";
import { GigTrackQueueBridge } from "./bridge.js";
import { listTools } from "./mcp-server.js";

const SERVER_NAMES = ["gigtrack_queue_bridge", "gqb"];

export async function runDoctor({ env = process.env, argv = process.argv.slice(2), logger = nullLogger() } = {}) {
  const writeReportIndex = argv.indexOf("--write-report");
  const writeReportPath = writeReportIndex >= 0 ? argv[writeReportIndex + 1] : null;
  const configPath = configPathFromEnv(env);
  const configScan = scanCodexConfig(configPath);
  const configuredEnv = loadConfiguredMcpEnv(configPath, configScan.matching_servers[0]);
  const effectiveEnv = { ...env, ...configuredEnv };
  logger.event("gqb.config.scan.completed", {
    diagnosis: configScan.valid ? null : "MCP_CONFIG_MISSING",
    details: { ...configScan, configured_env_keys: Object.keys(configuredEnv) }
  });

  const launchProbe = await probeLaunch(effectiveEnv);
  logger.event("gqb.launch.probe.completed", {
    diagnosis: launchProbe.ok ? null : launchProbe.diagnosis,
    details: launchProbe
  });

  const nodePath = probeNodeOnPath(effectiveEnv);
  let health;
  try {
    const bridge = GigTrackQueueBridge.fromEnv(effectiveEnv, { logger });
    health = await bridge.queue_channel_health();
  } catch (error) {
    logger.event("gqb.doctor.health.failed", {
      level: "warn",
      diagnosis: "DOCTOR_HEALTH_PROBE_FAILED",
      details: { message: error.message, code: error.code ?? null }
    });
    health = {
      ok: false,
      error_code: "DOCTOR_HEALTH_PROBE_FAILED",
      trace_id: null,
      data: { controller: { ping_attempted: false, reachable: false, diagnosis: "DOCTOR_HEALTH_PROBE_FAILED" } }
    };
  }
  const report = {
    ok: Boolean(configScan.valid && launchProbe.ok && health.ok),
    generated_at: new Date().toISOString(),
    config_scan: configScan,
    launch_probe: launchProbe,
    node_path: nodePath,
    tools: listTools().map((tool) => tool.name),
    diagnostics_file: logger.filePath ?? null,
    health,
    environment: redactedEnvironmentSummary(effectiveEnv),
    rca_assumptions: rcaAssumptions({ configScan, launchProbe, nodePath, health })
  };

  if (writeReportPath) writeFileSync(resolve(writeReportPath), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
  return report;
}

export async function runDoctorCli({ env = process.env, argv = process.argv.slice(2) } = {}) {
  const logger = new DiagnosticsLogger({ origin: env.GQB_LAUNCH_SOURCE || "doctor", dir: diagnosticsDirFromEnv(env) });
  const report = await runDoctor({ env, argv, logger });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

export function scanCodexConfig(configPath = configPathFromEnv()) {
  if (!configPath || !existsSync(configPath)) {
    return {
      config_path: configPath,
      missing: true,
      readable: false,
      matching_servers: [],
      valid: false,
      reason: "config file not found"
    };
  }
  try {
    const text = readFileSync(configPath, "utf8");
    const sections = [...text.matchAll(/^\s*\[mcp_servers\.(?:"([^"]+)"|([A-Za-z0-9_-]+))\]\s*(?:#.*)?$/gm)]
      .map((match) => match[1] ?? match[2]);
    const matchingServers = sections.filter((name) => SERVER_NAMES.includes(name));
    const valid = matchingServers.length > 0;
    return {
      config_path: configPath,
      missing: false,
      readable: true,
      mcp_servers: sections,
      matching_servers: matchingServers,
      valid,
      reason: valid ? null : "no gigtrack_queue_bridge or gqb MCP server entry"
    };
  } catch (error) {
    return {
      config_path: configPath,
      readable: false,
      matching_servers: [],
      valid: false,
      missing: false,
      reason: `config file unreadable: ${error.code ?? error.message}`
    };
  }
}

export function probeNodeOnPath(env = process.env) {
  const command = env.GQB_NODE_PATH || "node";
  const result = spawnSync(command, ["--version"], { encoding: "utf8", shell: false });
  if (result.error?.code === "ENOENT") {
    return { available: false, diagnosis: "NODE_UNAVAILABLE", command, version: null };
  }
  if (result.status === 0) {
    return { available: true, diagnosis: null, command, version: result.stdout.trim() };
  }
  return {
    available: false,
    diagnosis: "NODE_UNAVAILABLE",
    command,
    version: null,
    stderr: result.stderr?.trim() ?? null
  };
}

export async function probeLaunch(env = process.env) {
  const serverPath = resolve(dirname(fileURLToPath(import.meta.url)), "../../bin/gqb-mcp.js");
  const command = env.GQB_NODE_PATH || process.execPath;
  const startedAt = Date.now();
  const child = spawn(command, [serverPath], {
    env,
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => { stderr += chunk; });

  const timeoutMs = 5000;
  const result = await new Promise((resolveResult) => {
    const responses = [];
    let buffer = "";
    let listSent = false;
    let finished = false;
    const timer = setTimeout(() => finish({ timed_out: true, responses }), timeoutMs);
    const finish = (value) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      child.kill();
      resolveResult(value);
    };

    child.on("error", (error) => finish({ error: error.message, code: error.code ?? null, responses }));
    child.stdin.on("error", (error) => finish({ error: error.message, code: error.code ?? null, responses }));
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      buffer += chunk;
      let newlineIndex;
      while ((newlineIndex = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (!line) continue;
        let response;
        try {
          response = JSON.parse(line);
        } catch (error) {
          finish({ parse_error: error.message, responses });
          return;
        }
        responses.push(response);
        if (response.id === 1 && !listSent) {
          listSent = true;
          try {
            child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized", params: {} })}\n`);
            child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} })}\n`);
          } catch (error) {
            finish({ error: error.message, code: error.code ?? null, responses });
            return;
          }
        }
        if (response.id === 2) {
          finish({ timed_out: false, responses });
          return;
        }
      }
    });
    child.on("exit", (code, signal) => finish({ exited: true, code, signal, responses }));
    const initialize = {
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "gqb-doctor", version: "0.1.0" } }
    };
    try {
      child.stdin.write(`${JSON.stringify(initialize)}\n`);
    } catch (error) {
      finish({ error: error.message, code: error.code ?? null, responses });
    }
  });

  const durationMs = Date.now() - startedAt;
  if (result.timed_out) {
    return { ok: false, diagnosis: "MCP_LAUNCH_FAILED", command, server_path: serverPath, duration_ms: durationMs, timeout_ms: timeoutMs, stderr: stderr.slice(0, 2000) };
  }
  if (result.error || result.parse_error || result.exited) {
    return {
      ok: false,
      diagnosis: "MCP_LAUNCH_FAILED",
      command,
      server_path: serverPath,
      duration_ms: durationMs,
      message: result.error ?? result.parse_error ?? "MCP process exited before tools/list response",
      exit_code: result.code ?? null,
      signal: result.signal ?? null,
      stdout: stdout.slice(0, 2000),
      stderr: stderr.slice(0, 2000)
    };
  }
  const tools = result.responses.find((response) => response.id === 2)?.result?.tools ?? [];
  return {
    ok: tools.length > 0,
    diagnosis: tools.length > 0 ? null : "MCP_LAUNCH_FAILED",
    command,
    server_path: serverPath,
    duration_ms: durationMs,
    node_version: process.version,
    tool_count: tools.length,
    tools: tools.map((tool) => tool.name),
    stderr: stderr.slice(0, 2000)
  };
}

function loadConfiguredMcpEnv(configPath, serverName) {
  if (!configPath || !serverName || !existsSync(configPath)) return {};
  try {
    const text = readFileSync(configPath, "utf8");
    const sectionPattern = /^\s*\[mcp_servers\.(?:"([^"]+)"|([A-Za-z0-9_-]+))\.env\]\s*(?:#.*)?$/gm;
    const sectionMatch = [...text.matchAll(sectionPattern)].find((match) => (match[1] ?? match[2]) === serverName);
    if (!sectionMatch) return {};
    const sectionStart = sectionMatch.index + sectionMatch[0].length;
    const nextSection = text.slice(sectionStart).search(/^\s*\[/m);
    const sectionText = text.slice(sectionStart, nextSection < 0 ? undefined : sectionStart + nextSection);
    const allowed = new Set([
      "GQB_CONTROLLER_SOCKET",
      "GQB_CONTROLLER_URL",
      "GQB_CONTROLLER_BEARER_TOKEN",
      "GQB_CONTROLLER_AUTH0_TOKEN_URL",
      "GQB_CONTROLLER_AUTH0_DOMAIN",
      "GQB_CONTROLLER_AUTH0_CLIENT_ID",
      "GQB_CONTROLLER_AUTH0_CLIENT_SECRET",
      "GQB_CONTROLLER_AUTH0_AUDIENCE",
      "GQB_CONTROLLER_AUTH0_SCOPE",
      "GQB_CONTROLLER_MODE",
      "GQB_ALLOW_DEV_LOCAL_CONTROLLER",
      "GQB_LOCAL_CONTROLLER_STATE_PATH",
      "GQB_DIAG_DIR",
      "GQB_JOURNAL_PATH",
      "GQB_LAUNCH_SOURCE",
      "GQB_NODE_PATH"
    ]);
    const result = {};
    for (const line of sectionText.split(/\r?\n/)) {
      const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)(?:\s+#.*)?$/);
      if (!match || !allowed.has(match[1])) continue;
      const value = parseTomlString(match[2]);
      if (value !== null) result[match[1]] = value;
    }
    return result;
  } catch {
    return {};
  }
}

function parseTomlString(value) {
  const trimmed = value.trim();
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) return trimmed.slice(1, -1);
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    try { return JSON.parse(trimmed); } catch { return null; }
  }
  return null;
}

export function rcaAssumptions({ configScan, launchProbe, nodePath, health }) {
  const controllerDiagnosis = health?.data?.controller?.diagnosis ?? health?.error_code ?? null;
  const healthProbeFailed = controllerDiagnosis === "DOCTOR_HEALTH_PROBE_FAILED";
  const transportAssumption = (condition) => healthProbeFailed
    ? unknown("health probe failed before transport diagnosis was available")
    : verdict(Boolean(health?.data?.controller), condition, "gqb.transport.selected");
  return {
    config_entry_missing_or_invalid: configScan.missing
      ? verdict(true, true, "gqb.config.scan.completed", configScan.reason)
      : verdict(configScan.readable, !configScan.valid, "gqb.config.scan.completed", configScan.reason),
    launch_command_unresolved: verdict(configScan.valid, launchProbe.diagnosis === "MCP_LAUNCH_FAILED", "gqb.launch.probe.completed", configScan.valid ? null : "MCP config was not valid, so configured command could not be assessed"),
    stdio_mcp_ready: verdict(true, launchProbe.ok, "gqb.launch.probe.completed"),
    controller_transport_unconfigured: transportAssumption(controllerDiagnosis === "CONTROLLER_UNCONFIGURED"),
    controller_invalid_url: transportAssumption(controllerDiagnosis === "CONTROLLER_INVALID_URL"),
    controller_config_ambiguous: transportAssumption(controllerDiagnosis === "CONTROLLER_CONFIG_AMBIGUOUS"),
    embedded_local_dev_opt_in_missing: transportAssumption(controllerDiagnosis === "CONTROLLER_DEV_MODE_REQUIRED"),
    controller_unreachable: controllerDiagnosis === "CONTROLLER_UNCONFIGURED"
      ? unknown("transport was not configured, so reachability was not tested")
      : verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_UNREACHABLE", "gqb.controller.ping.completed"),
    controller_dns_failure: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_DNS_FAILURE", "gqb.controller.ping.completed"),
    controller_connectivity_failure: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_CONNECTIVITY_FAILURE", "gqb.controller.ping.completed"),
    controller_tls_failure: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_TLS_FAILURE", "gqb.controller.ping.completed"),
    controller_authentication_rejected: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_AUTH_REJECTED", "gqb.controller.ping.completed"),
    controller_authorization_rejected: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_AUTHORIZATION_REJECTED", "gqb.controller.ping.completed"),
    controller_media_negotiation_failed: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_MEDIA_NEGOTIATION_FAILED", "gqb.controller.ping.completed"),
    controller_content_type_rejected: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_CONTENT_TYPE_REJECTED", "gqb.controller.ping.completed"),
    controller_endpoint_not_found: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_ENDPOINT_NOT_FOUND", "gqb.controller.ping.completed"),
    controller_upstream_5xx: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_UPSTREAM_ERROR", "gqb.controller.ping.completed"),
    controller_malformed_json_response: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_MALFORMED_JSON_RESPONSE", "gqb.controller.ping.completed"),
    controller_malformed_sse_response: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_MALFORMED_SSE_RESPONSE", "gqb.controller.ping.completed"),
    controller_response_id_mismatch: verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_RESPONSE_ID_MISMATCH", "gqb.controller.ping.completed"),
    auth0_m2m_config_incomplete: transportAssumption(controllerDiagnosis === "AUTH0_M2M_CONFIG_INCOMPLETE"),
    auth0_token_acquisition_failed: transportAssumption(controllerDiagnosis === "AUTH0_TOKEN_ACQUISITION_FAILED"),
    auth0_invalid_token_response: transportAssumption(controllerDiagnosis === "AUTH0_INVALID_TOKEN_RESPONSE"),
    auth0_token_missing_required_scope: transportAssumption(controllerDiagnosis === "AUTH0_TOKEN_MISSING_REQUIRED_SCOPE"),
    health_false_green_detected: health?.data?.controller?.ping_attempted
      ? verdict(true, Boolean(health?.ok && health?.data?.controller?.reachable === false), "gqb.health.completed")
      : unknown("controller ping did not run"),
    node_unavailable_on_path: verdict(true, nodePath.available === false, "gqb.launch.probe.completed")
  };
}

function verdict(checked, condition, event, reason = null) {
  if (!checked) return unknown(reason ?? "prerequisite check did not run");
  return { verdict: condition ? "confirmed" : "refuted", evidence_event: event, reason };
}

function unknown(reason) {
  return { verdict: "unknown", evidence_event: null, reason };
}

function configPathFromEnv(env = process.env) {
  const codexHome = env.CODEX_HOME || join(env.USERPROFILE || env.HOME || homedir(), ".codex");
  return join(codexHome, "config.toml");
}

function redactedEnvironmentSummary(env) {
  return {
    GQB_CONTROLLER_SOCKET: env.GQB_CONTROLLER_SOCKET ? "[set]" : null,
    GQB_CONTROLLER_URL: env.GQB_CONTROLLER_URL ? redactedUrl(env.GQB_CONTROLLER_URL) : null,
    GQB_CONTROLLER_MODE: env.GQB_CONTROLLER_MODE ?? null,
    GQB_CONTROLLER_AUTH0_TOKEN_URL: env.GQB_CONTROLLER_AUTH0_TOKEN_URL ? redactedUrl(env.GQB_CONTROLLER_AUTH0_TOKEN_URL) : null,
    GQB_CONTROLLER_AUTH0_DOMAIN: env.GQB_CONTROLLER_AUTH0_DOMAIN ? "[set]" : null,
    GQB_CONTROLLER_AUTH0_CLIENT_ID: env.GQB_CONTROLLER_AUTH0_CLIENT_ID ? "[set]" : null,
    GQB_CONTROLLER_AUTH0_CLIENT_SECRET: env.GQB_CONTROLLER_AUTH0_CLIENT_SECRET ? "[set]" : null,
    GQB_CONTROLLER_AUTH0_AUDIENCE: env.GQB_CONTROLLER_AUTH0_AUDIENCE ? redactedUrl(env.GQB_CONTROLLER_AUTH0_AUDIENCE) : null,
    GQB_CONTROLLER_AUTH0_SCOPE: env.GQB_CONTROLLER_AUTH0_SCOPE ? "[set]" : null,
    GQB_ALLOW_DEV_LOCAL_CONTROLLER: env.GQB_ALLOW_DEV_LOCAL_CONTROLLER ?? null,
    GQB_LOCAL_CONTROLLER_STATE_PATH: env.GQB_LOCAL_CONTROLLER_STATE_PATH ?? null,
    GQB_JOURNAL_PATH: env.GQB_JOURNAL_PATH ?? null,
    GQB_DIAG_DIR: env.GQB_DIAG_DIR ?? null,
    GQB_LAUNCH_SOURCE: env.GQB_LAUNCH_SOURCE ?? null,
    GQB_CONTROLLER_BEARER_TOKEN: env.GQB_CONTROLLER_BEARER_TOKEN ? "[set]" : null
  };
}

function redactedUrl(value) {
  try {
    const url = new URL(value);
    return `${url.protocol}//${url.host}${url.pathname}`;
  } catch {
    return "[invalid-url]";
  }
}

