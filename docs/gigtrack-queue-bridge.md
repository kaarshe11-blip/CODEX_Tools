# GigTrack Queue Bridge

This package contains the GigTrack Queue Bridge MCP server for `KAN-142`.

The bridge is a queue-management facade over the authoritative GigTrack controller MCP. The upstream controller stays authoritative for goals, tasks, attempts, dispatches, owner decisions, and worker transitions. The bridge adds request-id normalization, local payload hashing, durable journaling, stale-decision fingerprint checks, next-safe-action derivation, health/preflight surfaces, and deliberately gated recovery handoff.

## Runtime

- `GQB_CONTROLLER_SOCKET`: controller Unix socket path. Use only inside a trusted Linux environment where the bridge process can see that socket.
- `GQB_CONTROLLER_URL`: HTTP(S) controller MCP endpoint. If neither URL nor socket is explicitly configured, the bridge reports `CONTROLLER_UNCONFIGURED`. Windows remote operation should use `https://mcp-dev.kaarsheapps.online/mcp`.
- `GQB_CONTROLLER_BEARER_TOKEN`: optional explicit static bearer token. This is useful for manual/temporary operation and takes precedence over automatic Auth0 M2M credentials when both are configured.
- `GQB_CONTROLLER_AUTH0_TOKEN_URL`: Auth0 OAuth token endpoint for automatic Client Credentials, for example `https://<tenant>.auth0.com/oauth/token`.
- `GQB_CONTROLLER_AUTH0_DOMAIN`: optional Auth0 domain alternative to `GQB_CONTROLLER_AUTH0_TOKEN_URL`; the bridge derives `/oauth/token`.
- `GQB_CONTROLLER_AUTH0_CLIENT_ID`: Auth0 M2M application client ID.
- `GQB_CONTROLLER_AUTH0_CLIENT_SECRET`: Auth0 M2M application client secret.
- `GQB_CONTROLLER_AUTH0_AUDIENCE`: Auth0 API/resource audience. For the public GigTrack controller this is `https://mcp-dev.kaarsheapps.online`.
- `GQB_CONTROLLER_AUTH0_SCOPE`: optional scope override. The required controller permission is `mcp:controller`.
- `GQB_CONTROLLER_MODE=embedded_local`: dev/test-only local controller. This mode is not production behavior, does not contact Replit, and must not be used as proof that the authoritative queue accepted work.
- `GQB_ALLOW_DEV_LOCAL_CONTROLLER=true`: required opt-in before `embedded_local` can become healthy or accept mutating calls.
- `GQB_LOCAL_CONTROLLER_STATE_PATH`: optional JSON state file for `embedded_local`; defaults to the local user state directory.
- Do not combine `GQB_CONTROLLER_MODE=embedded_local` with `GQB_CONTROLLER_SOCKET` or `GQB_CONTROLLER_URL`, and do not set both external controller variables. The bridge reports `CONTROLLER_CONFIG_AMBIGUOUS` rather than choosing silently.
- `GQB_JOURNAL_PATH`: required for production mutators. Points at the SQLite journal database.
- `GQB_DIAG_DIR`: optional diagnostics directory for append-only JSONL events.
- `GQB_LAUNCH_SOURCE`: optional launch origin label, for example `codex_client`.
- `GQB_NODE_PATH`: optional absolute Node executable used by `gqb doctor` and the configured Codex launch entry when `node` is not on `PATH`.

The stdio entry point is:

```sh
node ./bin/gqb-mcp.js
```

On Windows, configure Codex with the absolute Node executable and bridge path rather than relying on `node` being on `PATH`:

```toml
[mcp_servers.gigtrack_queue_bridge]
command = 'C:\path\to\node.exe'
args = ['C:\path\to\CODEX_Tools\bin\gqb-mcp.js']
startup_timeout_sec = 120

[mcp_servers.gigtrack_queue_bridge.env]
GQB_NODE_PATH = 'C:\path\to\node.exe'
GQB_JOURNAL_PATH = 'C:\Users\<user>\AppData\Local\gqb\queue.sqlite'
GQB_DIAG_DIR = 'C:\Users\<user>\AppData\Local\gqb\diagnostics'
GQB_LAUNCH_SOURCE = 'codex_client'
GQB_CONTROLLER_URL = 'https://mcp-dev.kaarsheapps.online/mcp'
GQB_CONTROLLER_AUTH0_TOKEN_URL = 'https://<tenant>.auth0.com/oauth/token'
GQB_CONTROLLER_AUTH0_CLIENT_ID = '<client-id>'
GQB_CONTROLLER_AUTH0_CLIENT_SECRET = '<client-secret>'
GQB_CONTROLLER_AUTH0_AUDIENCE = 'https://mcp-dev.kaarsheapps.online'
GQB_CONTROLLER_AUTH0_SCOPE = 'mcp:controller'
```

Do not use a Unix socket from Windows Codex. Windows cannot open a Unix-domain socket inside a remote Linux filesystem. The Windows production path is:

```text
Codex -> local Queue Bridge MCP over stdio -> authenticated HTTPS -> Cloudflare ingress -> authoritative controller MCP
```

The HTTP MCP endpoint and Auth0 audience are intentionally different:

- MCP endpoint: `https://mcp-dev.kaarsheapps.online/mcp`
- Auth0 audience: `https://mcp-dev.kaarsheapps.online`

The bridge sends `Content-Type: application/json` and `Accept: application/json, text/event-stream` for HTTP(S) controller requests. It supports ordinary JSON and Streamable HTTP SSE responses that carry JSON-RPC payloads in `data:` events. The bridge keeps the narrow controller tool contract and calls only `get_goal_status`, `submit_goal`, and `submit_owner_decision`.

Authentication precedence is deterministic:

1. `GQB_CONTROLLER_BEARER_TOKEN` static bearer, when configured.
2. Auth0 Client Credentials M2M using the `GQB_CONTROLLER_AUTH0_*` variables.

Automatic M2M uses `grant_type=client_credentials`, requests the configured audience and `mcp:controller` scope, caches the access token in memory only, refreshes at about 80% of the reported token lifetime, shares one in-flight token request across concurrent callers, and retries a controller request exactly once after HTTP 401 with a fresh token. Partial or invalid M2M configuration fails closed; it does not silently downgrade to unauthenticated controller access.

Never store real bearer tokens, client secrets, access tokens, or Authorization headers in docs, source, examples, journal records, or diagnostics.

For local dev/unit testing only:

```toml
[mcp_servers.gigtrack_queue_bridge.env]
GQB_CONTROLLER_MODE = 'embedded_local'
GQB_ALLOW_DEV_LOCAL_CONTROLLER = 'true'
GQB_LOCAL_CONTROLLER_STATE_PATH = 'C:\Users\<user>\AppData\Local\gqb\controller-state.json'
GQB_JOURNAL_PATH = 'C:\Users\<user>\AppData\Local\gqb\queue.sqlite'
```

Diagnostics:

```sh
node ./bin/gqb-doctor.js --json
node ./bin/gqb-doctor.js --json --write-report ./gqb-doctor-report.json
```

The doctor command scans Codex MCP config for `gigtrack_queue_bridge` or `gqb`, probes stdio launch/tool listing, checks whether plain `node` is on `PATH`, runs `queue_channel_health`, and reports tri-state RCA assumptions with `confirmed`, `refuted`, or `unknown`.

Health and doctor output distinguish bridge process readiness, journal availability, controller transport configuration, HTTPS transport selection, authentication configuration, controller reachability, and controller readiness. A successful authenticated `get_goal_status` remains the strongest proof that the channel is ready; local MCP startup alone is not controller readiness.

Common controller diagnoses:

- `CONTROLLER_UNCONFIGURED`: neither socket nor URL was configured.
- `CONTROLLER_CONFIG_AMBIGUOUS`: socket and URL, or local mode plus external controller, were configured together.
- `CONTROLLER_INVALID_URL`: the controller URL is malformed or uses an unsupported scheme.
- `CONTROLLER_DNS_FAILURE`, `CONTROLLER_CONNECTIVITY_FAILURE`, `CONTROLLER_TLS_FAILURE`: network, routing, or TLS connection failures.
- `UPSTREAM_TIMEOUT_INDETERMINATE`: the controller request timed out after send.
- `CONTROLLER_AUTH_REJECTED`: HTTP 401 authentication rejection.
- `CONTROLLER_AUTHORIZATION_REJECTED`: HTTP 403 authorization/scope rejection.
- `CONTROLLER_MEDIA_NEGOTIATION_FAILED`: HTTP 406, usually missing or unsupported `Accept` media types.
- `CONTROLLER_CONTENT_TYPE_REJECTED`: HTTP 415, usually unsupported request `Content-Type`.
- `CONTROLLER_ENDPOINT_NOT_FOUND`: HTTP 404; confirm the path is `/mcp`.
- `CONTROLLER_UPSTREAM_ERROR`: upstream HTTP 5xx, including Cloudflare/origin failures.
- `CONTROLLER_MALFORMED_JSON_RESPONSE`, `CONTROLLER_MALFORMED_SSE_RESPONSE`, `CONTROLLER_RESPONSE_ID_MISMATCH`: response parsing or JSON-RPC correlation failures.
- `AUTH0_M2M_CONFIG_INCOMPLETE`, `AUTH0_TOKEN_ACQUISITION_FAILED`, `AUTH0_INVALID_TOKEN_RESPONSE`, `AUTH0_TOKEN_MISSING_REQUIRED_SCOPE`: automatic M2M configuration or token failures.

The MCP server implements `tools/list` and `tools/call` for:

- `queue_submit`
- `queue_status`
- `queue_preflight`
- `queue_resume`
- `queue_control`
- `queue_handoff`
- `queue_reconcile`
- `queue_channel_health`

## Safety Posture

`queue_handoff` is present, but mutating recovery handoff returns `UPSTREAM_CAPABILITY_REQUIRED` until the upstream controller exposes expected-dispatch CAS or event dispatch correlation. This matches the approved v12 design and prevents the bridge from pretending it can safely recover a live worker-owned dispatch.

