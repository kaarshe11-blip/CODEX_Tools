# GigTrack Queue Bridge

This package contains the first build of the GigTrack Queue Bridge MCP server for `KAN-142`.

The bridge is a queue-management facade over the private GigTrack controller socket. The upstream controller stays authoritative for goals, tasks, attempts, dispatches, owner decisions, and worker transitions. The bridge adds request-id normalization, local payload hashing, durable journaling, stale-decision fingerprint checks, next-safe-action derivation, health/preflight surfaces, and deliberately gated recovery handoff.

## Runtime

- `GQB_CONTROLLER_SOCKET`: controller Unix socket path. Use only when the bridge process can see that socket.
- `GQB_CONTROLLER_URL`: HTTP(S) controller MCP endpoint. If neither URL nor socket is explicitly configured, the bridge reports `CONTROLLER_UNCONFIGURED`.
- Do not set both controller variables. The bridge reports `CONTROLLER_CONFIG_AMBIGUOUS` rather than choosing silently.
- `GQB_JOURNAL_PATH`: required for production mutators. Points at the SQLite journal database.
- `GQB_DIAG_DIR`: optional diagnostics directory for append-only JSONL events.
- `GQB_LAUNCH_SOURCE`: optional launch origin label, for example `codex_client`.

The stdio entry point is:

```sh
node ./bin/gqb-mcp.js
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
