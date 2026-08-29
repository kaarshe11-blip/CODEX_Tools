import http from "node:http";
import https from "node:https";
import { DETERMINISTIC_UPSTREAM_ERROR_MAP } from "./constants.js";
import { nullLogger } from "./diagnostics.js";

export class UpstreamError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "UpstreamError";
    for (const key of [
      "cause",
      "deterministic",
      "indeterminate",
      "mappedCode",
      "preSend",
      "statusCode",
      "timeout",
      "transport",
      "upstreamCode",
      "upstreamError"
    ]) {
      if (Object.hasOwn(details, key)) this[key] = details[key];
    }
  }
}

export class ControllerClient {
  constructor({ socketPath = null, url = null, bearerToken = null, timeoutMs = 30000, logger = nullLogger() } = {}) {
    this.socketPath = socketPath;
    this.url = url;
    this.bearerToken = bearerToken;
    this.timeoutMs = timeoutMs;
    this.logger = logger;
  }

  static fromEnv(env = process.env, { logger = nullLogger() } = {}) {
    return new ControllerClient({
      socketPath: env.GQB_CONTROLLER_SOCKET || null,
      url: env.GQB_CONTROLLER_URL || null,
      bearerToken: env.GQB_CONTROLLER_BEARER_TOKEN || null,
      logger
    });
  }

  describeTransport() {
    if (this.url && this.socketPath) {
      return {
        kind: "ambiguous",
        url_configured: true,
        socket_configured: true
      };
    }
    if (this.url) {
      try {
        const parsed = new URL(this.url);
        if (!["http:", "https:"].includes(parsed.protocol)) {
          return {
            kind: "invalid_url",
            reason: "unsupported_protocol",
            url_protocol: parsed.protocol,
            url_host: parsed.host,
            url_path: parsed.pathname || "/"
          };
        }
        return {
          kind: parsed.protocol === "https:" ? "existing_remote" : "http",
          url_protocol: parsed.protocol,
          url_host: parsed.host,
          url_path: parsed.pathname || "/"
        };
      } catch {
        return { kind: "invalid_url", url_protocol: null, url_host: null, url_path: null };
      }
    }
    if (this.socketPath) return { kind: "socket", socket_path: this.socketPath };
    return { kind: "none" };
  }

  async ping({ timeoutMs = 3000, traceId = null } = {}) {
    try {
      const result = await this.callTool("get_goal_status", { includeEvents: false, eventLimit: 1 }, { timeoutMs, traceId });
      return {
        reachable: true,
        ping_attempted: true,
        diagnosis: null,
        upstream_shape: shapeOf(result),
        idle_goal_null_accepted: Object.hasOwn(result ?? {}, "goal") && result.goal === null
      };
    } catch (error) {
      return {
        reachable: false,
        ping_attempted: error.preSend !== true,
        diagnosis: error.mappedCode ?? (error.timeout ? "UPSTREAM_TIMEOUT_INDETERMINATE" : "CONTROLLER_UNREACHABLE"),
        message: error.message,
        upstream_code: error.upstreamCode ?? null
      };
    }
  }

  async callTool(name, args, { timeoutMs = this.timeoutMs, traceId = null } = {}) {
    const transport = this.describeTransport();
    if (transport.kind === "none") {
      throw new UpstreamError("controller transport is not configured", {
        deterministic: true,
        mappedCode: "CONTROLLER_UNCONFIGURED",
        preSend: true,
        transport
      });
    }
    if (transport.kind === "invalid_url" || transport.kind === "ambiguous") {
      const mappedCode = transport.kind === "ambiguous" ? "CONTROLLER_CONFIG_AMBIGUOUS" : "CONTROLLER_UNCONFIGURED";
      throw new UpstreamError(
        transport.kind === "ambiguous" ? "controller socket and URL are both configured" : "controller URL is invalid",
        {
          deterministic: true,
          mappedCode,
          preSend: true,
          transport
        }
      );
    }

    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      method: "tools/call",
      params: { name, arguments: args }
    });

    let responseStarted = false;
    const responseText = await new Promise((resolve, reject) => {
      const options = this.requestOptions(transport, body, timeoutMs);
      const client = options.protocol === "https:" ? https : http;
      const req = client.request(
        options,
        (res) => {
          responseStarted = true;
          let data = "";
          res.setEncoding("utf8");
          res.on("data", (chunk) => {
            data += chunk;
          });
          res.on("end", () => {
            if (res.statusCode >= 300) {
              const mappedCode = res.statusCode === 401 || res.statusCode === 403
                ? "CONTROLLER_AUTH_REJECTED"
                : res.statusCode === 404
                    ? "CONTROLLER_ENDPOINT_NOT_FOUND"
                    : res.statusCode >= 500
                        ? "CONTROLLER_UPSTREAM_ERROR"
                        : res.statusCode >= 400
                            ? "CONTROLLER_REQUEST_REJECTED"
                            : "CONTROLLER_REDIRECTED";
              const event = mappedCode === "CONTROLLER_AUTH_REJECTED" ? "gqb.controller.auth.rejected" : "gqb.transport.request.failed";
              this.logger.event(event, {
                traceId,
                level: "warn",
                diagnosis: mappedCode,
                details: { tool: name, status_code: res.statusCode, transport, response_started: true }
              });
              reject(new UpstreamError(`controller HTTP ${res.statusCode}`, {
                deterministic: true,
                mappedCode,
                statusCode: res.statusCode,
                transport
              }));
              return;
            }
            resolve(data);
          });
        }
      );
      req.on("timeout", () => {
        req.destroy(new UpstreamError("upstream timeout after send", { indeterminate: true, timeout: true, transport }));
      });
      req.on("error", (error) => {
        if (error instanceof UpstreamError) {
          this.logger.event("gqb.transport.request.failed", {
            traceId,
            level: "warn",
            diagnosis: error.timeout ? "UPSTREAM_TIMEOUT_INDETERMINATE" : (error.mappedCode ?? "CONTROLLER_UNREACHABLE"),
            details: {
              tool: name,
              transport,
              timeout: Boolean(error.timeout),
              response_started: responseStarted,
              socket_bytes_written: req.socket?.bytesWritten ?? 0
            }
          });
          reject(error);
          return;
        }
        const bytesWritten = req.socket?.bytesWritten ?? 0;
        const mappedCode = responseStarted || bytesWritten > 0 ? "UPSTREAM_INDETERMINATE" : "CONTROLLER_UNREACHABLE";
        this.logger.event("gqb.transport.request.failed", {
          traceId,
          level: "warn",
          diagnosis: mappedCode,
          details: {
            tool: name,
            transport,
            response_started: responseStarted,
            socket_bytes_written: bytesWritten,
            error_code: error.code ?? null,
            message: error.message
          }
        });
        reject(new UpstreamError(error.message, {
          deterministic: mappedCode === "CONTROLLER_UNREACHABLE",
          indeterminate: mappedCode !== "CONTROLLER_UNREACHABLE",
          mappedCode,
          preSend: mappedCode === "CONTROLLER_UNREACHABLE",
          cause: error,
          transport
        }));
      });
      req.write(body);
      req.end();
    });

    let response;
    try {
      response = JSON.parse(responseText);
    } catch (error) {
      throw new UpstreamError("malformed upstream response", { indeterminate: true, cause: error });
    }
    if (response?.error) {
      const upstreamCode = response.error?.data?.code;
      const mappedCode = DETERMINISTIC_UPSTREAM_ERROR_MAP.get(upstreamCode);
      if (mappedCode) {
        throw new UpstreamError(response.error.message ?? upstreamCode, {
          deterministic: true,
          upstreamCode,
          mappedCode,
          upstreamError: response.error
        });
      }
      throw new UpstreamError(response.error.message ?? "unrecognized upstream error", {
        indeterminate: true,
        upstreamCode,
        upstreamError: response.error
      });
    }
    if (!response || typeof response !== "object" || typeof response.result === "undefined") {
      throw new UpstreamError("malformed upstream result", { indeterminate: true });
    }
    return response.result;
  }

  requestOptions(transport, body, timeoutMs) {
    const headers = {
      "content-type": "application/json",
      "content-length": Buffer.byteLength(body)
    };
    if (this.bearerToken) headers.authorization = `Bearer ${this.bearerToken}`;

    if (transport.kind === "socket") {
      return {
        socketPath: this.socketPath,
        path: "/mcp",
        method: "POST",
        headers,
        timeout: timeoutMs,
        protocol: "http:"
      };
    }

    const parsed = new URL(this.url);
    return {
      protocol: parsed.protocol,
      hostname: parsed.hostname,
      port: parsed.port || undefined,
      path: `${parsed.pathname && parsed.pathname !== "/" ? parsed.pathname : "/mcp"}${parsed.search || ""}`,
      method: "POST",
      headers,
      timeout: timeoutMs
    };
  }
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
