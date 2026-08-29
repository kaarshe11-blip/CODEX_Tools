# GigTrack Queue Bridge

This package contains the first build of the GigTrack Queue Bridge MCP server for `KAN-142`.

The bridge is a queue-management facade over the private GigTrack controller socket. The upstream controller stays authoritative for goals, tasks, attempts, dispatches, owner decisions, and worker transitions. The bridge adds request-id normalization, local payload hashing, durable journaling, stale-decision fingerprint checks, next-safe-action derivation, health/preflight surfaces, and deliberately gated recovery handoff.

## Runtime

- `GQB_CONTROLLER_SOCKET`: controller Unix socket path. Use only when the bridge process can see that socket. In the GigTrack Replit workspace, the intended controller socket is `/home/runner/workspace/.mcp-local/controller.sock`.
- `GQB_CONTROLLER_URL`: HTTP(S) controller MCP endpoint. If neither URL nor socket is explicitly configured, the bridge reports `CONTROLLER_UNCONFIGURED`.
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
GQB_CONTROLLER_SOCKET = '/home/runner/workspace/.mcp-local/controller.sock'
```

Do not use the socket example above from Windows Codex. Windows cannot open a Unix-domain socket inside the Replit Linux filesystem. If Codex runs on Windows, the bridge should fail closed until a real private control ingress exists or until the bridge itself runs in Replit beside the authoritative controller.

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

