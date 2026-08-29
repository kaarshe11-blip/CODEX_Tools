import { stdin, stdout } from "node:process";
import { GigTrackQueueBridge } from "./bridge.js";

const TOOLS = [
  "queue_submit",
  "queue_status",
  "queue_preflight",
  "queue_resume",
  "queue_control",
  "queue_handoff",
  "queue_reconcile",
  "queue_channel_health"
];

export function listTools() {
  return TOOLS.map((name) => ({
    name,
    description: `GigTrack Queue Bridge ${name}`,
    inputSchema: { type: "object", additionalProperties: true }
  }));
}

export class StdioMcpServer {
  constructor({ bridge = GigTrackQueueBridge.fromEnv() } = {}) {
    this.bridge = bridge;
    this.buffer = "";
    this.bridge.logger.event("gqb.mcp.process.start", {
      details: {
        launch_origin: this.bridge.launchSource,
        pid: process.pid,
        node_version: process.version,
        argv: process.argv.slice(0, 2)
      }
    });
  }

  start() {
    stdin.setEncoding("utf8");
    stdin.on("data", (chunk) => this.onData(chunk));
  }

  onData(chunk) {
    this.buffer += chunk;
    let newline;
    while ((newline = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, newline).trim();
      this.buffer = this.buffer.slice(newline + 1);
      if (line) void this.handleLine(line);
    }
  }

  async handleLine(line) {
    let request;
    try {
      request = JSON.parse(line);
      const result = await this.handleRequest(request);
      this.write({ jsonrpc: "2.0", id: request.id ?? null, result });
    } catch (error) {
      this.bridge.logger.event("gqb.mcp.lifecycle.failed", {
        level: "error",
        diagnosis: "MCP_REQUEST_FAILED",
        details: { message: error?.message ?? String(error) }
      });
      this.write({
        jsonrpc: "2.0",
        id: request?.id ?? null,
        error: { code: -32603, message: error?.message ?? String(error) }
      });
    }
  }

  async handleRequest(request) {
    if (request.method === "initialize") {
      this.bridge.logger.event("gqb.mcp.lifecycle.ready", {
        details: {
          protocol_version: "2024-11-05",
          server_version: "0.1.0",
          tool_count: TOOLS.length
        }
      });
      return { protocolVersion: "2024-11-05", serverInfo: { name: "gigtrack-queue-bridge", version: "0.1.0" }, capabilities: { tools: {} } };
    }
    if (request.method === "tools/list") return { tools: listTools() };
    if (request.method === "tools/call") {
      const name = request.params?.name;
      const args = request.params?.arguments ?? {};
      if (!TOOLS.includes(name)) throw new Error(`unknown tool: ${name}`);
      const result = await this.bridge[name](args);
      return { content: [{ type: "text", text: JSON.stringify(result) }], structuredContent: result };
    }
    return {};
  }

  write(message) {
    stdout.write(`${JSON.stringify(message)}\n`);
  }
}

export function startMcpServerFromEnv() {
  new StdioMcpServer().start();
}
