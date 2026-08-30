import http from "node:http";
import https from "node:https";
import { DETERMINISTIC_UPSTREAM_ERROR_MAP } from "./constants.js";
import { nullLogger } from "./diagnostics.js";

const REQUIRED_CONTROLLER_SCOPE = "mcp:controller";
const STREAMABLE_HTTP_ACCEPT = "application/json, text/event-stream";
const TOKEN_REFRESH_FRACTION = 0.8;

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
  constructor({
    socketPath = null,
    url = null,
    bearerToken = null,
    auth0 = null,
    timeoutMs = 30000,
    logger = nullLogger(),
    now = () => Date.now()
  } = {}) {
    this.socketPath = socketPath;
    this.url = url;
    this.bearerToken = bearerToken;
    this.auth0 = normalizeAuth0Config(auth0);
    this.timeoutMs = timeoutMs;
    this.logger = logger;
    this.now = now;
    this.cachedToken = null;
    this.tokenPromise = null;
  }

  static fromEnv(env = process.env, { logger = nullLogger() } = {}) {
    return new ControllerClient({
      socketPath: env.GQB_CONTROLLER_SOCKET || null,
      url: env.GQB_CONTROLLER_URL || null,
      bearerToken: env.GQB_CONTROLLER_BEARER_TOKEN || null,
      auth0: auth0ConfigFromEnv(env),
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

  describeAuth() {
    const m2m = describeM2mConfig(this.auth0);
    if (this.bearerToken) {
      return {
        configured: true,
        method: "static_bearer",
        precedence: "static_bearer",
        m2m
      };
    }
    if (this.auth0) {
      return {
        configured: true,
        method: "auth0_client_credentials",
        precedence: "auth0_client_credentials",
        m2m
      };
    }
    return { configured: false, method: "none", precedence: null, m2m };
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
      const mappedCode = transport.kind === "ambiguous" ? "CONTROLLER_CONFIG_AMBIGUOUS" : "CONTROLLER_INVALID_URL";
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

    const requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: requestId,
      method: "tools/call",
      params: { name, arguments: args }
    });

    const httpResponse = await this.sendControllerRequest({ name, body, transport, timeoutMs, traceId });
    const response = parseControllerResponse(httpResponse, requestId);
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
    if (!response || typeof response !== "object" || !Object.hasOwn(response, "result")) {
      throw new UpstreamError("malformed upstream result", {
        indeterminate: true,
        mappedCode: "CONTROLLER_MALFORMED_JSON_RESPONSE"
      });
    }
    return normalizeControllerToolResult(name, response.result);
  }

  async sendControllerRequest({ name, body, transport, timeoutMs, traceId }) {
    let retriedAfter401 = false;
    for (;;) {
      const auth = await this.authorizationForTransport(transport, traceId, timeoutMs);
      const response = await this.performRequest({ name, body, transport, timeoutMs, traceId, authorization: auth.header });
      if (response.statusCode === 401 && auth.source === "auth0_client_credentials" && !retriedAfter401) {
        retriedAfter401 = true;
        this.invalidateAccessToken();
        this.logger.event("gqb.controller.auth.retry", {
          traceId,
          level: "warn",
          diagnosis: "CONTROLLER_AUTH_REJECTED",
          details: { tool: name, status_code: 401, transport }
        });
        continue;
      }
      if (response.statusCode >= 300) {
        throw this.httpStatusError(response, { name, transport, traceId });
      }
      return response;
    }
  }

  async authorizationForTransport(transport, traceId = null, timeoutMs = this.timeoutMs) {
    if (this.bearerToken) {
      validateCredentialTransport(transport, this.url);
      return { source: "static_bearer", header: `Bearer ${this.bearerToken}` };
    }
    if (!this.auth0 || !["http", "existing_remote"].includes(transport.kind)) return { source: "none", header: null };
    validateCredentialTransport(transport, this.url);
    const token = await this.getAccessToken(traceId, timeoutMs);
    return { source: "auth0_client_credentials", header: `Bearer ${token}` };
  }

  async getAccessToken(traceId = null, timeoutMs = this.timeoutMs) {
    if (this.cachedToken && this.now() < this.cachedToken.refreshAt) return this.cachedToken.accessToken;
    if (this.tokenPromise) return this.tokenPromise;
    this.tokenPromise = this.acquireAccessToken(traceId, timeoutMs).finally(() => {
      this.tokenPromise = null;
    });
    return this.tokenPromise;
  }

  invalidateAccessToken() {
    this.cachedToken = null;
  }

  async acquireAccessToken(traceId = null, timeoutMs = this.timeoutMs) {
    validateAuth0Config(this.auth0);
    const response = await requestAuth0Token(this.auth0, timeoutMs, traceId, this.logger);
    const token = parseAuth0TokenResponse(response, this.auth0, this.now);
    this.cachedToken = token;
    return token.accessToken;
  }

  performRequest({ name, body, transport, timeoutMs, traceId, authorization = null }) {
    let responseStarted = false;
    return new Promise((resolve, reject) => {
      const options = this.requestOptions(transport, body, timeoutMs, authorization);
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
            resolve({ statusCode: res.statusCode ?? 0, headers: res.headers, body: data });
          });
        }
      );
      req.on("timeout", () => {
        req.destroy(new UpstreamError("upstream timeout after send", {
          indeterminate: true,
          mappedCode: "UPSTREAM_TIMEOUT_INDETERMINATE",
          timeout: true,
          transport
        }));
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
        const mappedCode = responseStarted || bytesWritten > 0 ? "UPSTREAM_INDETERMINATE" : networkDiagnosis(error);
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
          deterministic: mappedCode !== "UPSTREAM_INDETERMINATE",
          indeterminate: mappedCode === "UPSTREAM_INDETERMINATE",
          mappedCode,
          preSend: mappedCode !== "UPSTREAM_INDETERMINATE",
          cause: error,
          transport
        }));
      });
      req.write(body);
      req.end();
    });
  }

  httpStatusError(response, { name, transport, traceId }) {
    const mappedCode = httpStatusDiagnosis(response.statusCode);
    const indeterminate = response.statusCode >= 500 || response.statusCode === 408 || response.statusCode === 429;
    const event = mappedCode === "CONTROLLER_AUTH_REJECTED" ? "gqb.controller.auth.rejected" : "gqb.transport.request.failed";
    this.logger.event(event, {
      traceId,
      level: "warn",
      diagnosis: mappedCode,
      details: { tool: name, status_code: response.statusCode, transport, response_started: true }
    });
    return new UpstreamError(`controller HTTP ${response.statusCode}`, {
      deterministic: !indeterminate,
      indeterminate,
      mappedCode,
      statusCode: response.statusCode,
      transport
    });
  }

  requestOptions(transport, body, timeoutMs, authorization = null) {
    const headers = {
      "content-type": "application/json",
      "accept": STREAMABLE_HTTP_ACCEPT,
      "content-length": Buffer.byteLength(body)
    };
    if (authorization) headers.authorization = authorization;

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

function auth0ConfigFromEnv(env) {
  const hasAuth0Input = Boolean(
    env.GQB_CONTROLLER_AUTH0_TOKEN_URL
    || env.GQB_CONTROLLER_AUTH0_DOMAIN
    || env.GQB_CONTROLLER_AUTH0_CLIENT_ID
    || env.GQB_CONTROLLER_AUTH0_CLIENT_SECRET
    || env.GQB_CONTROLLER_AUTH0_AUDIENCE
    || env.GQB_CONTROLLER_AUTH0_SCOPE
  );
  if (!hasAuth0Input) return null;
  const raw = {
    tokenUrl: env.GQB_CONTROLLER_AUTH0_TOKEN_URL || null,
    domain: env.GQB_CONTROLLER_AUTH0_DOMAIN || null,
    clientId: env.GQB_CONTROLLER_AUTH0_CLIENT_ID || null,
    clientSecret: env.GQB_CONTROLLER_AUTH0_CLIENT_SECRET || null,
    audience: env.GQB_CONTROLLER_AUTH0_AUDIENCE || null,
    scope: env.GQB_CONTROLLER_AUTH0_SCOPE || REQUIRED_CONTROLLER_SCOPE
  };
  return raw;
}

function normalizeAuth0Config(auth0) {
  if (!auth0) return null;
  const tokenUrl = auth0.tokenUrl || tokenUrlFromDomain(auth0.domain);
  return {
    tokenUrl,
    domain: auth0.domain || null,
    clientId: auth0.clientId || null,
    clientSecret: auth0.clientSecret || null,
    audience: auth0.audience || null,
    scope: auth0.scope || REQUIRED_CONTROLLER_SCOPE
  };
}

function tokenUrlFromDomain(domain) {
  if (!domain) return null;
  try {
    const withProtocol = /^https?:\/\//i.test(domain) ? domain : `https://${domain}`;
    const parsed = new URL(withProtocol);
    parsed.pathname = "/oauth/token";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return null;
  }
}

function describeM2mConfig(auth0) {
  if (!auth0) return { configured: false };
  return {
    configured: true,
    token_endpoint_configured: Boolean(auth0.tokenUrl),
    client_id_configured: Boolean(auth0.clientId),
    client_secret_configured: Boolean(auth0.clientSecret),
    audience_configured: Boolean(auth0.audience),
    scope: auth0.scope || null
  };
}

function validateAuth0Config(auth0) {
  const missing = [];
  if (!auth0?.tokenUrl) missing.push("GQB_CONTROLLER_AUTH0_TOKEN_URL or GQB_CONTROLLER_AUTH0_DOMAIN");
  if (!auth0?.clientId) missing.push("GQB_CONTROLLER_AUTH0_CLIENT_ID");
  if (!auth0?.clientSecret) missing.push("GQB_CONTROLLER_AUTH0_CLIENT_SECRET");
  if (!auth0?.audience) missing.push("GQB_CONTROLLER_AUTH0_AUDIENCE");
  if (missing.length) {
    throw new UpstreamError("Auth0 client credentials configuration is incomplete", {
      deterministic: true,
      mappedCode: "AUTH0_M2M_CONFIG_INCOMPLETE",
      preSend: true,
      upstreamError: { missing }
    });
  }
  try {
    const parsed = new URL(auth0.tokenUrl);
    if (!["http:", "https:"].includes(parsed.protocol)) throw new Error("unsupported token endpoint protocol");
    if (parsed.protocol !== "https:" && !isLoopbackHost(parsed.hostname)) {
      throw new Error("Auth0 token endpoint must use HTTPS unless it is loopback");
    }
  } catch (error) {
    throw new UpstreamError("Auth0 token endpoint is invalid", {
      deterministic: true,
      mappedCode: "AUTH0_M2M_CONFIG_INCOMPLETE",
      preSend: true,
      cause: error
    });
  }
}

async function requestAuth0Token(auth0, timeoutMs, traceId, logger) {
  const body = JSON.stringify({
    grant_type: "client_credentials",
    client_id: auth0.clientId,
    client_secret: auth0.clientSecret,
    audience: auth0.audience,
    scope: auth0.scope || REQUIRED_CONTROLLER_SCOPE
  });
  const parsed = new URL(auth0.tokenUrl);
  const headers = {
    "content-type": "application/json",
    "accept": "application/json",
    "content-length": Buffer.byteLength(body)
  };
  const options = {
    protocol: parsed.protocol,
    hostname: parsed.hostname,
    port: parsed.port || undefined,
    path: `${parsed.pathname || "/oauth/token"}${parsed.search || ""}`,
    method: "POST",
    headers,
    timeout: timeoutMs
  };
  const client = options.protocol === "https:" ? https : http;
  return new Promise((resolve, reject) => {
    const req = client.request(options, (res) => {
      let data = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => {
        data += chunk;
      });
      res.on("end", () => {
        if ((res.statusCode ?? 0) >= 300) {
          logger.event("gqb.auth0.token.failed", {
            traceId,
            level: "warn",
            diagnosis: "AUTH0_TOKEN_ACQUISITION_FAILED",
            details: { status_code: res.statusCode, token_endpoint: auth0.tokenUrl }
          });
          reject(new UpstreamError("Auth0 token acquisition failed", {
            deterministic: true,
            mappedCode: "AUTH0_TOKEN_ACQUISITION_FAILED",
            preSend: true,
            statusCode: res.statusCode
          }));
          return;
        }
        resolve({ statusCode: res.statusCode, headers: res.headers, body: data });
      });
    });
    req.on("timeout", () => {
      req.destroy(new UpstreamError("Auth0 token acquisition timed out", {
        deterministic: true,
        mappedCode: "AUTH0_TOKEN_ACQUISITION_FAILED",
        preSend: true,
        timeout: true
      }));
    });
    req.on("error", (error) => {
      if (error instanceof UpstreamError) {
        reject(error);
        return;
      }
      logger.event("gqb.auth0.token.failed", {
        traceId,
        level: "warn",
        diagnosis: "AUTH0_TOKEN_ACQUISITION_FAILED",
        details: { token_endpoint: auth0.tokenUrl, code: error.code ?? null, message: error.message }
      });
      reject(new UpstreamError("Auth0 token acquisition failed", {
        deterministic: true,
        mappedCode: "AUTH0_TOKEN_ACQUISITION_FAILED",
        preSend: true,
        cause: error
      }));
    });
    req.write(body);
    req.end();
  });
}

function parseAuth0TokenResponse(response, auth0, now) {
  let parsed;
  try {
    parsed = JSON.parse(response.body);
  } catch (error) {
    throw new UpstreamError("Auth0 token response is not valid JSON", {
      deterministic: true,
      mappedCode: "AUTH0_INVALID_TOKEN_RESPONSE",
      preSend: true,
      cause: error
    });
  }
  const accessToken = parsed?.access_token;
  const expiresIn = Number(parsed?.expires_in);
  const tokenType = parsed?.token_type;
  if (typeof accessToken !== "string" || accessToken.length === 0 || !Number.isFinite(expiresIn) || expiresIn <= 0 || (tokenType && String(tokenType).toLowerCase() !== "bearer")) {
    throw new UpstreamError("Auth0 token response is missing required fields", {
      deterministic: true,
      mappedCode: "AUTH0_INVALID_TOKEN_RESPONSE",
      preSend: true
    });
  }
  if (!scopeIncludes(parsed.scope, REQUIRED_CONTROLLER_SCOPE)) {
    throw new UpstreamError("Auth0 token response is missing required controller scope", {
      deterministic: true,
      mappedCode: "AUTH0_TOKEN_MISSING_REQUIRED_SCOPE",
      preSend: true
    });
  }
  const issuedAt = now();
  const lifetimeMs = expiresIn * 1000;
  return {
    accessToken,
    expiresAt: issuedAt + lifetimeMs,
    refreshAt: issuedAt + Math.floor(lifetimeMs * TOKEN_REFRESH_FRACTION),
    audience: auth0.audience,
    scope: parsed.scope ?? auth0.scope
  };
}

function scopeIncludes(scope, requiredScope) {
  return String(scope || "").split(/\s+/).includes(requiredScope);
}

function parseControllerResponse(response, requestId) {
  const contentType = String(response.headers?.["content-type"] || "").toLowerCase();
  if (contentType.includes("text/event-stream")) return parseSseJsonRpc(response.body, requestId);
  return parseJsonRpcText(response.body, requestId);
}

function parseJsonRpcText(text, requestId) {
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new UpstreamError("malformed upstream JSON response", {
      indeterminate: true,
      mappedCode: "CONTROLLER_MALFORMED_JSON_RESPONSE",
      cause: error
    });
  }
  validateJsonRpcResponse(parsed, requestId, "CONTROLLER_MALFORMED_JSON_RESPONSE");
  return parsed;
}

function parseSseJsonRpc(text, requestId) {
  const events = parseSseEvents(text);
  if (!events.length) {
    throw new UpstreamError("malformed upstream SSE response", {
      indeterminate: true,
      mappedCode: "CONTROLLER_MALFORMED_SSE_RESPONSE"
    });
  }
  const parsedEvents = [];
  for (const event of events) {
    if (!event.data.length) continue;
    const payload = event.data.join("\n").trim();
    if (!payload || payload === "[DONE]") continue;
    try {
      const parsed = JSON.parse(payload);
      parsedEvents.push(parsed);
      if (parsed?.id === requestId) {
        validateJsonRpcResponse(parsed, requestId, "CONTROLLER_MALFORMED_SSE_RESPONSE");
        return parsed;
      }
    } catch (error) {
      continue;
    }
  }
  if (parsedEvents.length === 1) {
    validateJsonRpcResponse(parsedEvents[0], requestId, "CONTROLLER_MALFORMED_SSE_RESPONSE");
    return parsedEvents[0];
  }
  throw new UpstreamError("SSE response did not contain a usable JSON-RPC payload", {
    indeterminate: true,
    mappedCode: "CONTROLLER_MALFORMED_SSE_RESPONSE"
  });
}

function parseSseEvents(text) {
  const events = [];
  let event = { data: [] };
  for (const rawLine of String(text).split(/\r?\n/)) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (line === "") {
      if (event.data.length) events.push(event);
      event = { data: [] };
      continue;
    }
    if (line.startsWith(":")) continue;
    const separator = line.indexOf(":");
    const field = separator >= 0 ? line.slice(0, separator) : line;
    let value = separator >= 0 ? line.slice(separator + 1) : "";
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "data") event.data.push(value);
  }
  if (event.data.length) events.push(event);
  return events;
}

function validateJsonRpcResponse(response, requestId, malformedCode) {
  if (!response || typeof response !== "object") {
    throw new UpstreamError("malformed upstream result", { indeterminate: true, mappedCode: malformedCode });
  }
  if (Object.hasOwn(response, "id") && response.id !== requestId) {
    throw new UpstreamError("JSON-RPC response ID mismatch", {
      indeterminate: true,
      mappedCode: "CONTROLLER_RESPONSE_ID_MISMATCH"
    });
  }
  if (!Object.hasOwn(response, "result") && !Object.hasOwn(response, "error")) {
    throw new UpstreamError("malformed upstream result", { indeterminate: true, mappedCode: malformedCode });
  }
}

function normalizeControllerToolResult(name, result) {
  let payload = result;
  if (result?.isError === true) {
    throw new UpstreamError(mcpErrorMessage(result), {
      indeterminate: true,
      mappedCode: "UPSTREAM_INDETERMINATE",
      upstreamError: result
    });
  }
  if (isMcpCallToolResult(result)) {
    if (result?.structuredContent && typeof result.structuredContent === "object" && !Array.isArray(result.structuredContent)) {
      payload = result.structuredContent;
    } else {
      throwUnexpectedResultShape("MCP tools/call response did not include structuredContent");
    }
  }
  validateControllerToolPayload(name, payload);
  return payload;
}

function isMcpCallToolResult(result) {
  return Boolean(result && typeof result === "object" && (
    Array.isArray(result.content)
    || Object.hasOwn(result, "structuredContent")
    || Object.hasOwn(result, "isError")
  ));
}

function validateControllerToolPayload(name, payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throwUnexpectedResultShape("controller tool result is not an object");
  }
  if (name === "get_goal_status") {
    if (!Object.hasOwn(payload, "goal") || !Array.isArray(payload.tasks) || !Array.isArray(payload.events)) {
      throwUnexpectedResultShape("get_goal_status returned an unexpected result shape");
    }
    return;
  }
  if (name === "submit_goal") {
    if (typeof payload.goal_id !== "string" || payload.goal_id.length === 0) {
      throwUnexpectedResultShape("submit_goal returned an unexpected result shape");
    }
    return;
  }
  if (name === "submit_owner_decision") {
    if (typeof payload.goal_state !== "string" || payload.goal_state.length === 0) {
      throwUnexpectedResultShape("submit_owner_decision returned an unexpected result shape");
    }
  }
}

function mcpErrorMessage(result) {
  const text = Array.isArray(result?.content)
    ? result.content.find((item) => item?.type === "text" && typeof item.text === "string")?.text
    : null;
  return text || "upstream tool reported an error";
}

function throwUnexpectedResultShape(message) {
  throw new UpstreamError(message, {
    indeterminate: true,
    mappedCode: "CONTROLLER_UNEXPECTED_RESULT_SHAPE"
  });
}

function httpStatusDiagnosis(statusCode) {
  if (statusCode === 401) return "CONTROLLER_AUTH_REJECTED";
  if (statusCode === 403) return "CONTROLLER_AUTHORIZATION_REJECTED";
  if (statusCode === 404) return "CONTROLLER_ENDPOINT_NOT_FOUND";
  if (statusCode === 406) return "CONTROLLER_MEDIA_NEGOTIATION_FAILED";
  if (statusCode === 415) return "CONTROLLER_CONTENT_TYPE_REJECTED";
  if (statusCode >= 500) return "CONTROLLER_UPSTREAM_ERROR";
  if (statusCode >= 400) return "CONTROLLER_REQUEST_REJECTED";
  return "CONTROLLER_REDIRECTED";
}

function validateCredentialTransport(transport, url) {
  if (transport.kind !== "http") return;
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    throw new UpstreamError("controller URL is invalid", {
      deterministic: true,
      mappedCode: "CONTROLLER_INVALID_URL",
      preSend: true,
      transport
    });
  }
  if (isLoopbackHost(parsed.hostname)) return;
  throw new UpstreamError("controller credentials require HTTPS unless the controller is loopback", {
    deterministic: true,
    mappedCode: "CONTROLLER_INVALID_URL",
    preSend: true,
    transport
  });
}

function isLoopbackHost(hostname) {
  const normalized = String(hostname || "").toLowerCase();
  return normalized === "localhost"
    || normalized === "127.0.0.1"
    || normalized === "::1"
    || normalized === "[::1]";
}

function networkDiagnosis(error) {
  const code = error?.code;
  if (code === "ENOTFOUND" || code === "EAI_AGAIN") return "CONTROLLER_DNS_FAILURE";
  if (code === "ECONNREFUSED" || code === "ECONNRESET" || code === "ENETUNREACH" || code === "EHOSTUNREACH") return "CONTROLLER_CONNECTIVITY_FAILURE";
  if (code === "CERT_HAS_EXPIRED" || code === "DEPTH_ZERO_SELF_SIGNED_CERT" || code === "UNABLE_TO_VERIFY_LEAF_SIGNATURE" || /certificate|tls|ssl/i.test(error?.message ?? "")) return "CONTROLLER_TLS_FAILURE";
  return "CONTROLLER_CONNECTIVITY_FAILURE";
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
