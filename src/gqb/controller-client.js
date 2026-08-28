import http from "node:http";
import { DETERMINISTIC_UPSTREAM_ERROR_MAP } from "./constants.js";

export class UpstreamError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "UpstreamError";
    Object.assign(this, details);
  }
}

export class ControllerClient {
  constructor({ socketPath = "/home/runner/workspace/.mcp-local/controller.sock", timeoutMs = 30000 } = {}) {
    this.socketPath = socketPath;
    this.timeoutMs = timeoutMs;
  }

  async callTool(name, args, { timeoutMs = this.timeoutMs } = {}) {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      method: "tools/call",
      params: { name, arguments: args }
    });

    const responseText = await new Promise((resolve, reject) => {
      const req = http.request(
        {
          socketPath: this.socketPath,
          path: "/mcp",
          method: "POST",
          headers: {
            "content-type": "application/json",
            "content-length": Buffer.byteLength(body)
          },
          timeout: timeoutMs
        },
        (res) => {
          let data = "";
          res.setEncoding("utf8");
          res.on("data", (chunk) => {
            data += chunk;
          });
          res.on("end", () => resolve(data));
        }
      );
      req.on("timeout", () => {
        req.destroy(new UpstreamError("upstream timeout after send", { indeterminate: true, timeout: true }));
      });
      req.on("error", (error) => {
        if (error instanceof UpstreamError) reject(error);
        else reject(new UpstreamError(error.message, { indeterminate: true, cause: error }));
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
}
