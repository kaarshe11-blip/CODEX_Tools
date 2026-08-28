# GigTrack Queue Bridge

This package contains the first build of the GigTrack Queue Bridge MCP server for `KAN-142`.

The bridge is a queue-management facade over the private GigTrack controller socket. The upstream controller stays authoritative for goals, tasks, attempts, dispatches, owner decisions, and worker transitions. The bridge adds request-id normalization, local payload hashing, durable journaling, stale-decision fingerprint checks, next-safe-action derivation, health/preflight surfaces, and deliberately gated recovery handoff.

## Runtime

- `GQB_CONTROLLER_SOCKET`: controller Unix socket path. Defaults to `/home/runner/workspace/.mcp-local/controller.sock`.
- `GQB_JOURNAL_PATH`: required for production mutators. Points at the SQLite journal database.

The stdio entry point is:

```sh
node ./bin/gqb-mcp.js
```

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
