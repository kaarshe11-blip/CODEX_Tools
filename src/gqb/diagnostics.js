import { appendFileSync, chmodSync, existsSync, mkdirSync, renameSync, statSync } from "node:fs";
import { homedir, platform } from "node:os";
import { dirname, join } from "node:path";
import { stderr } from "node:process";

const DEFAULT_MAX_BYTES = 10 * 1024 * 1024;
const DEFAULT_RETAINED_FILES = 10;
const SECRET_RE = /(token|secret|key|authorization|password|cookie|credential|bearer)/i;

export function diagnosticsDirFromEnv(env = process.env) {
  if (env.GQB_DIAG_DIR) return env.GQB_DIAG_DIR;
  if (env.GQB_JOURNAL_PATH) return join(dirname(env.GQB_JOURNAL_PATH), "diagnostics");
  if (platform() === "win32" && env.LOCALAPPDATA) return join(env.LOCALAPPDATA, "gqb", "diagnostics");
  if (env.XDG_STATE_HOME) return join(env.XDG_STATE_HOME, "gqb", "diagnostics");
  return join(homedir(), ".gqb", "diagnostics");
}

export function redact(value) {
  if (value === null || typeof value === "undefined") return value;
  if (Array.isArray(value)) return value.map((item) => redact(item));
  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      code: value.code ?? null
    };
  }
  if (typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, inner]) => [
      key,
      SECRET_RE.test(key) ? "[REDACTED]" : redact(inner)
    ]));
  }
  if (typeof value === "string" && /^https?:\/\//i.test(value)) {
    try {
      const parsed = new URL(value);
      return `${parsed.protocol}//${parsed.host}${parsed.pathname}`;
    } catch {
      return "[REDACTED_URL]";
    }
  }
  return value;
}

export class DiagnosticsLogger {
  constructor({
    dir = diagnosticsDirFromEnv(),
    origin = process.env.GQB_LAUNCH_SOURCE || "unknown",
    instanceId = process.env.GQB_INSTANCE_ID || cryptoRandomId(),
    pid = process.pid,
    component = "gqb",
    maxBytes = DEFAULT_MAX_BYTES,
    retainedFiles = DEFAULT_RETAINED_FILES,
    stderrWriter = (line) => stderr.write(`${line}\n`)
  } = {}) {
    this.dir = dir;
    this.origin = sanitizeFilePart(origin);
    this.instanceId = instanceId;
    this.pid = pid;
    this.component = component;
    this.maxBytes = maxBytes;
    this.retainedFiles = retainedFiles;
    this.stderrWriter = stderrWriter;
    this.filePath = join(this.dir, `gqb-${this.origin}-${this.pid}-${sanitizeFilePart(this.instanceId)}.jsonl`);
  }

  event(event, {
    traceId = null,
    level = "info",
    diagnosis = null,
    details = {},
    component = this.component
  } = {}) {
    const entry = {
      event,
      ts: new Date().toISOString(),
      component,
      trace_id: traceId,
      instance_id: this.instanceId,
      pid: this.pid,
      level,
      diagnosis,
      details: redact(details)
    };
    const line = JSON.stringify(entry);
    try {
      this.stderrWriter(line);
    } catch {
      // Logging must never break MCP request handling.
    }
    try {
      this.ensureDir();
      this.rotateIfNeeded();
      appendFileSync(this.filePath, `${line}\n`, { mode: 0o600 });
      safeChmod(this.filePath, 0o600);
    } catch (error) {
      try {
        this.stderrWriter(JSON.stringify({
          event: "gqb.log.sink.failed",
          ts: new Date().toISOString(),
          component,
          trace_id: traceId,
          instance_id: this.instanceId,
          pid: this.pid,
          level: "warn",
          diagnosis: "LOG_SINK_FAILED",
          details: redact({ message: error.message, code: error.code ?? null })
        }));
      } catch {
        // Best effort only.
      }
    }
    return entry;
  }

  ensureDir() {
    if (!existsSync(this.dir)) mkdirSync(this.dir, { recursive: true, mode: 0o700 });
    safeChmod(this.dir, 0o700);
  }

  rotateIfNeeded() {
    if (!existsSync(this.filePath)) return;
    const size = statSync(this.filePath).size;
    if (size < this.maxBytes) return;
    for (let index = this.retainedFiles - 1; index >= 1; index -= 1) {
      const from = `${this.filePath}.${index}`;
      const to = `${this.filePath}.${index + 1}`;
      if (existsSync(from)) renameSync(from, to);
    }
    renameSync(this.filePath, `${this.filePath}.1`);
  }
}

export function nullLogger() {
  return { event() {} };
}

function sanitizeFilePart(value) {
  return String(value || "unknown").replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 80);
}

function cryptoRandomId() {
  return `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
}

function safeChmod(path, mode) {
  if (platform() === "win32") return;
  try {
    chmodSync(path, mode);
  } catch {
    // chmod is best effort on non-POSIX filesystems.
  }
}
