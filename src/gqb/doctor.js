import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { DiagnosticsLogger, nullLogger } from "./diagnostics.js";
import { GigTrackQueueBridge } from "./bridge.js";
import { listTools } from "./mcp-server.js";

const SERVER_NAMES = ["gigtrack_queue_bridge", "gqb"];

export async function runDoctor({ env = process.env, argv = process.argv.slice(2), logger = nullLogger() } = {}) {
  const writeReportIndex = argv.indexOf("--write-report");
  const writeReportPath = writeReportIndex >= 0 ? argv[writeReportIndex + 1] : null;
  const configPath = configPathFromEnv(env);
  const configScan = scanCodexConfig(configPath);
  logger.event("gqb.config.scan.completed", {
    diagnosis: configScan.valid ? null : "MCP_CONFIG_MISSING",
    details: configScan
  });

  const launchProbe = await probeLaunch(env);
  logger.event("gqb.launch.probe.completed", {
    diagnosis: launchProbe.ok ? null : launchProbe.diagnosis,
    details: launchProbe
  });

  const nodePath = probeNodeOnPath();
  const bridge = GigTrackQueueBridge.fromEnv(env, { logger });
  const health = await bridge.queue_channel_health();
  const report = {
    ok: configScan.valid && launchProbe.ok && health.ok,
    generated_at: new Date().toISOString(),
    config_scan: configScan,
    launch_probe: launchProbe,
    node_path: nodePath,
    tools: listTools().map((tool) => tool.name),
    health,
    environment: redactedEnvironmentSummary(env),
    rca_assumptions: rcaAssumptions({ configScan, launchProbe, nodePath, health })
  };

  if (writeReportPath) writeFileSync(resolve(writeReportPath), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
  return report;
}

export async function runDoctorCli({ env = process.env, argv = process.argv.slice(2) } = {}) {
  const logger = new DiagnosticsLogger({ origin: env.GQB_LAUNCH_SOURCE || "doctor" });
  const report = await runDoctor({ env, argv, logger });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

export function scanCodexConfig(configPath = configPathFromEnv()) {
  if (!configPath || !existsSync(configPath)) {
    return {
      config_path: configPath,
      readable: false,
      matching_servers: [],
      valid: false,
      reason: "config file not found"
    };
  }
  const text = readFileSync(configPath, "utf8");
  const sections = [...text.matchAll(/^\s*\[mcp_servers\.([A-Za-z0-9_-]+)\]\s*$/gm)].map((match) => match[1]);
  const matchingServers = sections.filter((name) => SERVER_NAMES.includes(name));
  const valid = matchingServers.length > 0;
  return {
    config_path: configPath,
    readable: true,
    mcp_servers: sections,
    matching_servers: matchingServers,
    valid,
    reason: valid ? null : "no gigtrack_queue_bridge or gqb MCP server entry"
  };
}

export function probeNodeOnPath() {
  const result = spawnSync("node", ["--version"], { encoding: "utf8", shell: false });
  if (result.error?.code === "ENOENT") {
    return { available: false, diagnosis: "NODE_UNAVAILABLE", command: "node", version: null };
  }
  if (result.status === 0) {
    return { available: true, diagnosis: null, command: "node", version: result.stdout.trim() };
  }
  return {
    available: false,
    diagnosis: "NODE_UNAVAILABLE",
    command: "node",
    version: null,
    stderr: result.stderr?.trim() ?? null
  };
}

export async function probeLaunch(env = process.env) {
  const serverPath = resolve(dirname(fileURLToPath(import.meta.url)), "../../bin/gqb-mcp.js");
  const startedAt = Date.now();
  const child = spawn(process.execPath, [serverPath], {
    env,
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });

  const initialize = {
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "gqb-doctor", version: "0.1.0" } }
  };
  const list = { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} };

  try {
    child.stdin.write(`${JSON.stringify(initialize)}\n`);
    child.stdin.write(`${JSON.stringify(list)}\n`);
  } catch (error) {
    child.kill();
    return { ok: false, diagnosis: "MCP_LAUNCH_FAILED", command: process.execPath, server_path: serverPath, message: error.message };
  }

  const timeoutMs = 5000;
  const result = await new Promise((resolveResult) => {
    const timer = setTimeout(() => {
      child.kill();
      resolveResult({ timed_out: true });
    }, timeoutMs);
    child.stdout.on("data", () => {
      const lines = stdout.trim().split(/\r?\n/).filter(Boolean);
      if (lines.length >= 2) {
        clearTimeout(timer);
        child.kill();
        resolveResult({ timed_out: false, lines });
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      resolveResult({ exited: true, code });
    });
  });

  const durationMs = Date.now() - startedAt;
  if (result.timed_out) {
    return { ok: false, diagnosis: "MCP_LAUNCH_FAILED", command: process.execPath, server_path: serverPath, duration_ms: durationMs, timeout_ms: timeoutMs, stderr: stderr.slice(0, 2000) };
  }
  try {
    const responses = result.lines.map((line) => JSON.parse(line));
    const tools = responses.find((response) => response.id === 2)?.result?.tools ?? [];
    return {
      ok: tools.length > 0,
      diagnosis: tools.length > 0 ? null : "MCP_LAUNCH_FAILED",
      command: process.execPath,
      server_path: serverPath,
      duration_ms: durationMs,
      node_version: process.version,
      tool_count: tools.length,
      tools: tools.map((tool) => tool.name),
      stderr: stderr.slice(0, 2000)
    };
  } catch (error) {
    return { ok: false, diagnosis: "MCP_LAUNCH_FAILED", command: process.execPath, server_path: serverPath, duration_ms: durationMs, message: error.message, stdout: stdout.slice(0, 2000), stderr: stderr.slice(0, 2000) };
  }
}

export function rcaAssumptions({ configScan, launchProbe, nodePath, health }) {
  const controllerDiagnosis = health?.data?.controller?.diagnosis ?? health?.error_code ?? null;
  return {
    config_entry_missing_or_invalid: verdict(configScan.readable, !configScan.valid, "gqb.config.scan.completed", configScan.reason),
    launch_command_unresolved: verdict(configScan.valid, launchProbe.diagnosis === "MCP_LAUNCH_FAILED", "gqb.launch.probe.completed", configScan.valid ? null : "MCP config was not valid, so configured command could not be assessed"),
    stdio_mcp_ready: verdict(true, launchProbe.ok, "gqb.launch.probe.completed"),
    controller_transport_unconfigured: verdict(true, controllerDiagnosis === "CONTROLLER_UNCONFIGURED", "gqb.transport.selected"),
    controller_unreachable: controllerDiagnosis === "CONTROLLER_UNCONFIGURED"
      ? unknown("transport was not configured, so reachability was not tested")
      : verdict(Boolean(health?.data?.controller?.ping_attempted), controllerDiagnosis === "CONTROLLER_UNREACHABLE", "gqb.controller.ping.completed"),
    health_false_green_detected: verdict(
      Boolean(health?.data?.controller?.ping_attempted),
      Boolean(health?.ok && health?.data?.controller?.reachable === false),
      "gqb.health.completed",
      "controller ping did not run"
    ),
    node_unavailable_on_path: verdict(true, nodePath.available === false, "gqb.launch.probe.completed")
  };
}

function verdict(checked, condition, event, reason = null) {
  if (!checked) return unknown(reason ?? "prerequisite check did not run");
  return { verdict: condition ? "confirmed" : "refuted", evidence_event: event, reason: null };
}

function unknown(reason) {
  return { verdict: "unknown", evidence_event: null, reason };
}

function configPathFromEnv(env = process.env) {
  const codexHome = env.CODEX_HOME || (env.USERPROFILE ? `${env.USERPROFILE}\\.codex` : null);
  return codexHome ? `${codexHome}\\config.toml` : null;
}

function redactedEnvironmentSummary(env) {
  return {
    GQB_CONTROLLER_SOCKET: env.GQB_CONTROLLER_SOCKET ? "[set]" : null,
    GQB_CONTROLLER_URL: env.GQB_CONTROLLER_URL ? redactedUrl(env.GQB_CONTROLLER_URL) : null,
    GQB_JOURNAL_PATH: env.GQB_JOURNAL_PATH ?? null,
    GQB_DIAG_DIR: env.GQB_DIAG_DIR ?? null,
    GQB_LAUNCH_SOURCE: env.GQB_LAUNCH_SOURCE ?? null
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
