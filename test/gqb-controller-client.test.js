import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { once } from "node:events";
import { ControllerClient, GigTrackQueueBridge } from "../src/gqb/index.js";

const SUCCESS_RESULT = { goal: null, tasks: [], current_dispatch: null, events: [] };

async function withServer(handler, fn) {
  const server = http.createServer(handler);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    return await fn(`http://127.0.0.1:${server.address().port}`, server);
  } finally {
    server.close();
    await once(server, "close");
  }
}

async function readBody(request) {
  let body = "";
  request.setEncoding("utf8");
  request.on("data", (chunk) => {
    body += chunk;
  });
  await once(request, "end");
  return body;
}

function jsonRpcSuccess(id, result = SUCCESS_RESULT) {
  return JSON.stringify({ jsonrpc: "2.0", id, result });
}

async function sendJsonSuccess(request, response) {
  const body = JSON.parse(await readBody(request));
  response.writeHead(200, { "content-type": "application/json" });
  response.end(jsonRpcSuccess(body.id));
  return body;
}

function auth0Config(baseUrl, overrides = {}) {
  return {
    tokenUrl: `${baseUrl}/oauth/token`,
    clientId: "client-id",
    clientSecret: "client-secret",
    audience: "https://mcp-dev.kaarsheapps.online",
    ...overrides
  };
}

test("HTTPS-compatible controller requests send JSON, Accept, and bearer headers", async () => {
  await withServer(async (request, response) => {
    assert.equal(request.headers["content-type"], "application/json");
    assert.equal(request.headers.accept, "application/json, text/event-stream");
    assert.equal(request.headers.authorization, "Bearer static-token");
    await sendJsonSuccess(request, response);
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, bearerToken: "static-token" });
    const result = await client.callTool("get_goal_status", {});
    assert.deepEqual(result, SUCCESS_RESULT);
  });
});

test("controller client parses ordinary JSON success responses", async () => {
  await withServer(sendJsonSuccess, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    assert.deepEqual(await client.callTool("get_goal_status", {}), SUCCESS_RESULT);
  });
});

test("controller client unwraps MCP structuredContent results", async () => {
  await withServer(async (request, response) => {
    const body = JSON.parse(await readBody(request));
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      jsonrpc: "2.0",
      id: body.id,
      result: {
        content: [{ type: "text", text: "ok" }],
        structuredContent: SUCCESS_RESULT
      }
    }));
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    assert.deepEqual(await client.callTool("get_goal_status", {}), SUCCESS_RESULT);
  });
});

test("controller client rejects MCP content-only results", async () => {
  await withServer(async (request, response) => {
    const body = JSON.parse(await readBody(request));
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      jsonrpc: "2.0",
      id: body.id,
      result: { content: [{ type: "text", text: "{\"goal_id\":\"goal-1\"}" }] }
    }));
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    await assert.rejects(
      () => client.callTool("submit_goal", { requestId: "gqb:req-content-only" }),
      (error) => {
        assert.equal(error.mappedCode, "CONTROLLER_UNEXPECTED_RESULT_SHAPE");
        assert.equal(error.indeterminate, true);
        return true;
      }
    );
  });
});

for (const structuredContent of [null, { goal_id: "should-not-apply", created: true }]) {
  test(`controller client rejects MCP isError results${structuredContent ? " with structuredContent" : ""}`, async () => {
    await withServer(async (request, response) => {
      const body = JSON.parse(await readBody(request));
      const result = {
        content: [{ type: "text", text: "upstream refused" }],
        isError: true
      };
      if (structuredContent) result.structuredContent = structuredContent;
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, result }));
    }, async (baseUrl) => {
      const client = new ControllerClient({ url: `${baseUrl}/mcp` });
      await assert.rejects(
        () => client.callTool("submit_goal", { requestId: "gqb:req-is-error" }),
        (error) => {
          assert.equal(error.mappedCode, "UPSTREAM_INDETERMINATE");
          assert.equal(error.indeterminate, true);
          assert.equal(error.upstreamError.isError, true);
          return true;
        }
      );
    });
  });
}

test("controller client parses SSE success responses and tolerates comments", async () => {
  await withServer(async (request, response) => {
    const body = JSON.parse(await readBody(request));
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end(`: keepalive\n\nevent: message\ndata: ${jsonRpcSuccess(body.id)}\n\n`);
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    assert.deepEqual(await client.callTool("get_goal_status", {}), SUCCESS_RESULT);
  });
});

test("controller client skips SSE non-JSON keepalives before a valid response", async () => {
  await withServer(async (request, response) => {
    const body = JSON.parse(await readBody(request));
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end(`event: ping\ndata: ping\n\nevent: message\ndata: ${jsonRpcSuccess(body.id)}\n\n`);
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    assert.deepEqual(await client.callTool("get_goal_status", {}), SUCCESS_RESULT);
  });
});

test("controller client rejects malformed SSE cleanly", async () => {
  await withServer(async (_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end("event: message\ndata: not-json\n\n");
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    await assert.rejects(
      () => client.callTool("get_goal_status", {}),
      (error) => {
        assert.equal(error.mappedCode, "CONTROLLER_MALFORMED_SSE_RESPONSE");
        assert.equal(error.indeterminate, true);
        return true;
      }
    );
  });
});

test("controller client rejects mismatched JSON-RPC response IDs", async () => {
  await withServer(async (_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(jsonRpcSuccess("wrong-id"));
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    await assert.rejects(
      () => client.callTool("get_goal_status", {}),
      (error) => {
        assert.equal(error.mappedCode, "CONTROLLER_RESPONSE_ID_MISMATCH");
        assert.equal(error.indeterminate, true);
        return true;
      }
    );
  });
});

test("controller HTTP 5xx after send is indeterminate", async () => {
  await withServer(async (_request, response) => {
    response.writeHead(504).end("gateway timeout");
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp` });
    await assert.rejects(
      () => client.callTool("submit_goal", { requestId: "gqb:req-504" }),
      (error) => {
        assert.equal(error.mappedCode, "CONTROLLER_UPSTREAM_ERROR");
        assert.equal(error.indeterminate, true);
        assert.equal(error.deterministic, false);
        return true;
      }
    );
  });
});

for (const [status, mappedCode] of [
  [401, "CONTROLLER_AUTH_REJECTED"],
  [403, "CONTROLLER_AUTHORIZATION_REJECTED"],
  [406, "CONTROLLER_MEDIA_NEGOTIATION_FAILED"],
  [415, "CONTROLLER_CONTENT_TYPE_REJECTED"]
]) {
  test(`controller HTTP ${status} is classified distinctly`, async () => {
    await withServer((_request, response) => {
      response.writeHead(status).end("rejected");
    }, async (baseUrl) => {
      const client = new ControllerClient({ url: `${baseUrl}/mcp` });
      const ping = await client.ping();
      assert.equal(ping.diagnosis, mappedCode);
      assert.equal(ping.ping_attempted, true);
    });
  });
}

test("Auth0 client credentials acquisition works and acquired tokens are cached", async () => {
  let tokenRequests = 0;
  await withServer(async (request, response) => {
    if (request.url === "/oauth/token") {
      tokenRequests += 1;
      const body = JSON.parse(await readBody(request));
      assert.equal(body.grant_type, "client_credentials");
      assert.equal(body.audience, "https://mcp-dev.kaarsheapps.online");
      assert.equal(body.scope, "mcp:controller");
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ access_token: "token-1", token_type: "Bearer", expires_in: 100, scope: "mcp:controller" }));
      return;
    }
    assert.equal(request.headers.authorization, "Bearer token-1");
    await sendJsonSuccess(request, response);
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl) });
    await client.callTool("get_goal_status", {});
    await client.callTool("get_goal_status", {});
    assert.equal(tokenRequests, 1);
  });
});

test("concurrent calls share one in-flight Auth0 token acquisition", async () => {
  let tokenRequests = 0;
  let controllerRequests = 0;
  await withServer(async (request, response) => {
    if (request.url === "/oauth/token") {
      tokenRequests += 1;
      await readBody(request);
      setTimeout(() => {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ access_token: "shared-token", token_type: "Bearer", expires_in: 100, scope: "mcp:controller" }));
      }, 25);
      return;
    }
    controllerRequests += 1;
    assert.equal(request.headers.authorization, "Bearer shared-token");
    await sendJsonSuccess(request, response);
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl) });
    await Promise.all([
      client.callTool("get_goal_status", {}),
      client.callTool("get_goal_status", {})
    ]);
    assert.equal(tokenRequests, 1);
    assert.equal(controllerRequests, 2);
  });
});

test("Auth0 token refresh occurs before hard expiry", async () => {
  let nowMs = 0;
  let tokenRequests = 0;
  await withServer(async (request, response) => {
    if (request.url === "/oauth/token") {
      tokenRequests += 1;
      await readBody(request);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ access_token: `token-${tokenRequests}`, token_type: "Bearer", expires_in: 100, scope: "mcp:controller" }));
      return;
    }
    await sendJsonSuccess(request, response);
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl), now: () => nowMs });
    await client.callTool("get_goal_status", {});
    nowMs = 79_000;
    await client.callTool("get_goal_status", {});
    assert.equal(tokenRequests, 1);
    nowMs = 81_000;
    await client.callTool("get_goal_status", {});
    assert.equal(tokenRequests, 2);
  });
});

test("HTTP 401 invalidates cached Auth0 token and retries exactly once", async () => {
  let tokenRequests = 0;
  let controllerRequests = 0;
  await withServer(async (request, response) => {
    if (request.url === "/oauth/token") {
      tokenRequests += 1;
      await readBody(request);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ access_token: `token-${tokenRequests}`, token_type: "Bearer", expires_in: 100, scope: "mcp:controller" }));
      return;
    }
    controllerRequests += 1;
    if (controllerRequests === 1) {
      assert.equal(request.headers.authorization, "Bearer token-1");
      response.writeHead(401).end("expired");
      return;
    }
    assert.equal(request.headers.authorization, "Bearer token-2");
    await sendJsonSuccess(request, response);
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl) });
    assert.deepEqual(await client.callTool("get_goal_status", {}), SUCCESS_RESULT);
    assert.equal(tokenRequests, 2);
    assert.equal(controllerRequests, 2);
  });
});

test("a second HTTP 401 is surfaced rather than retried indefinitely", async () => {
  let tokenRequests = 0;
  let controllerRequests = 0;
  await withServer(async (request, response) => {
    if (request.url === "/oauth/token") {
      tokenRequests += 1;
      await readBody(request);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ access_token: `token-${tokenRequests}`, token_type: "Bearer", expires_in: 100, scope: "mcp:controller" }));
      return;
    }
    controllerRequests += 1;
    response.writeHead(401).end("still rejected");
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl) });
    await assert.rejects(
      () => client.callTool("get_goal_status", {}),
      (error) => {
        assert.equal(error.mappedCode, "CONTROLLER_AUTH_REJECTED");
        return true;
      }
    );
    assert.equal(tokenRequests, 2);
    assert.equal(controllerRequests, 2);
  });
});

test("missing required M2M configuration fails closed", async () => {
  const client = new ControllerClient({
    url: "http://127.0.0.1:1/mcp",
    auth0: { clientId: "client-id", clientSecret: "client-secret", audience: "https://mcp-dev.kaarsheapps.online" }
  });
  await assert.rejects(
    () => client.callTool("get_goal_status", {}),
    (error) => {
      assert.equal(error.mappedCode, "AUTH0_M2M_CONFIG_INCOMPLETE");
      assert.equal(error.preSend, true);
      return true;
    }
  );
});

test("credentialed controller requests require HTTPS unless loopback", async () => {
  const client = new ControllerClient({ url: "http://controller.example/mcp", bearerToken: "static-token" });
  await assert.rejects(
    () => client.callTool("get_goal_status", {}),
    (error) => {
      assert.equal(error.mappedCode, "CONTROLLER_INVALID_URL");
      assert.equal(error.preSend, true);
      return true;
    }
  );
});

test("Auth0 token endpoint requires HTTPS unless loopback", async () => {
  const client = new ControllerClient({
    url: "https://controller.example/mcp",
    auth0: {
      tokenUrl: "http://auth.example/oauth/token",
      clientId: "client-id",
      clientSecret: "client-secret",
      audience: "https://mcp-dev.kaarsheapps.online"
    }
  });
  await assert.rejects(
    () => client.callTool("get_goal_status", {}),
    (error) => {
      assert.equal(error.mappedCode, "AUTH0_M2M_CONFIG_INCOMPLETE");
      assert.equal(error.preSend, true);
      return true;
    }
  );
});

test("Auth0 token responses missing required fields fail closed", async () => {
  await withServer(async (_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ token_type: "Bearer", expires_in: 100, scope: "mcp:controller" }));
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl) });
    await assert.rejects(
      () => client.callTool("get_goal_status", {}),
      (error) => {
        assert.equal(error.mappedCode, "AUTH0_INVALID_TOKEN_RESPONSE");
        assert.equal(error.preSend, true);
        return true;
      }
    );
  });
});

test("Auth0 token response missing mcp:controller fails closed when scope is supplied", async () => {
  await withServer(async (_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ access_token: "wrong-scope", token_type: "Bearer", expires_in: 100, scope: "openid profile" }));
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl) });
    await assert.rejects(
      () => client.callTool("get_goal_status", {}),
      (error) => {
        assert.equal(error.mappedCode, "AUTH0_TOKEN_MISSING_REQUIRED_SCOPE");
        assert.equal(error.preSend, true);
        return true;
      }
    );
  });
});

test("Auth0 token response missing scope fails closed", async () => {
  await withServer(async (_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ access_token: "no-scope", token_type: "Bearer", expires_in: 100 }));
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl) });
    await assert.rejects(
      () => client.callTool("get_goal_status", {}),
      (error) => {
        assert.equal(error.mappedCode, "AUTH0_TOKEN_MISSING_REQUIRED_SCOPE");
        assert.equal(error.preSend, true);
        return true;
      }
    );
  });
});

test("Auth0 token acquisition honors per-call timeout", async () => {
  await withServer(async (request, response) => {
    if (request.url === "/oauth/token") {
      await readBody(request);
      return;
    }
    response.writeHead(500).end("controller should not be called");
  }, async (baseUrl) => {
    const client = new ControllerClient({ url: `${baseUrl}/mcp`, auth0: auth0Config(baseUrl), timeoutMs: 30000 });
    const startedAt = Date.now();
    const ping = await client.ping({ timeoutMs: 50 });
    assert.equal(ping.diagnosis, "AUTH0_TOKEN_ACQUISITION_FAILED");
    assert.equal(ping.ping_attempted, false);
    assert.ok(Date.now() - startedAt < 1000);
  });
});

test("static bearer takes precedence over M2M credentials", async () => {
  let tokenRequests = 0;
  await withServer(async (request, response) => {
    if (request.url === "/oauth/token") {
      tokenRequests += 1;
      response.writeHead(500).end("should not be used");
      return;
    }
    assert.equal(request.headers.authorization, "Bearer manual-token");
    await sendJsonSuccess(request, response);
  }, async (baseUrl) => {
    const client = new ControllerClient({
      url: `${baseUrl}/mcp`,
      bearerToken: "manual-token",
      auth0: auth0Config(baseUrl, { clientSecret: "ignored-secret" })
    });
    await client.callTool("get_goal_status", {});
    assert.equal(tokenRequests, 0);
  });
});

test("queue_channel_health reports configured remote transport and authentication state", async () => {
  await withServer(async (request, response) => {
    assert.equal(request.headers.authorization, "Bearer health-token");
    await sendJsonSuccess(request, response);
  }, async (baseUrl) => {
    const controller = new ControllerClient({ url: `${baseUrl}/mcp`, bearerToken: "health-token" });
    const journal = { ensureOpen() {}, listOpenIntents() { return []; } };
    const bridge = new GigTrackQueueBridge({ controller, journal });
    const health = await bridge.queue_channel_health();
    assert.equal(health.ok, true);
    assert.equal(health.data.controller_transport_configured, true);
    assert.equal(health.data.controller_authentication.configured, true);
    assert.equal(health.data.controller_authentication.method, "static_bearer");
    assert.equal(health.data.controller_ready, true);
  });
});

test("controller diagnostics do not emit secrets or raw tokens", async () => {
  const events = [];
  await withServer(async (_request, response) => {
    response.writeHead(500).end("token endpoint failed");
  }, async (baseUrl) => {
    const client = new ControllerClient({
      url: `${baseUrl}/mcp`,
      auth0: auth0Config(baseUrl, { clientSecret: "super-secret-value" }),
      logger: { event: (event, payload) => events.push({ event, payload }) }
    });
    await assert.rejects(() => client.callTool("get_goal_status", {}));
  });
  const serialized = JSON.stringify(events);
  assert.equal(serialized.includes("super-secret-value"), false);
  assert.equal(serialized.includes("Bearer "), false);
});
