import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { CHANNEL, EFFECT_STATUS, JOURNAL_PHASE } from "./constants.js";

function nowIso() {
  return new Date().toISOString();
}

function encode(value) {
  return value == null ? null : JSON.stringify(value);
}

function decode(value) {
  return value == null ? null : JSON.parse(value);
}

export class JournalError extends Error {
  constructor(message, code) {
    super(message);
    this.name = "JournalError";
    this.code = code;
  }
}

export class SqliteJournal {
  constructor({ path, acquireTimeoutMs = 5000, leaseMs = 120000 } = {}) {
    this.path = path;
    this.acquireTimeoutMs = acquireTimeoutMs;
    this.leaseMs = leaseMs;
    this.db = null;
  }

  open() {
    if (!this.path) throw new JournalError("GQB_JOURNAL_PATH is required", "JOURNAL_UNAVAILABLE");
    mkdirSync(dirname(this.path), { recursive: true });
    this.db = new DatabaseSync(this.path);
    this.db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS gqb_journal (
        goal_key TEXT NOT NULL,
        goal_id TEXT,
        upstream_request_id TEXT NOT NULL,
        caller_request_id TEXT,
        tool TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        channel TEXT NOT NULL,
        phase TEXT NOT NULL,
        pre_status_snapshot TEXT,
        post_status_snapshot TEXT,
        upstream_arguments TEXT NOT NULL,
        upstream_result TEXT,
        effect_status TEXT,
        trace_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (goal_key, upstream_request_id)
      );
      CREATE TABLE IF NOT EXISTS gqb_locks (
        lock_key TEXT PRIMARY KEY,
        owner_token TEXT NOT NULL,
        acquired_at TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        heartbeat_at TEXT NOT NULL
      );
    `);
    return this;
  }

  ensureOpen() {
    if (!this.db) this.open();
  }

  rowToEntry(row) {
    if (!row) return null;
    return {
      ...row,
      pre_status_snapshot: decode(row.pre_status_snapshot),
      post_status_snapshot: decode(row.post_status_snapshot),
      upstream_arguments: decode(row.upstream_arguments),
      upstream_result: decode(row.upstream_result)
    };
  }

  getEntry(goalKey, upstreamRequestId) {
    this.ensureOpen();
    const row = this.db.prepare("SELECT * FROM gqb_journal WHERE goal_key = ? AND upstream_request_id = ?").get(goalKey, upstreamRequestId);
    return this.rowToEntry(row);
  }

  createIntent(entry) {
    this.ensureOpen();
    const existing = this.getEntry(entry.goal_key, entry.upstream_request_id);
    if (existing) {
      if (existing.tool !== entry.tool || existing.payload_hash !== entry.payload_hash) {
        throw new JournalError("same journal key reused with different tool or payload hash", "IDEMPOTENCY_KEY_REUSE");
      }
      return { created: false, entry: existing };
    }

    const createdAt = nowIso();
    this.db.prepare(`
      INSERT INTO gqb_journal (
        goal_key, goal_id, upstream_request_id, caller_request_id, tool, payload_hash, channel, phase,
        pre_status_snapshot, post_status_snapshot, upstream_arguments, upstream_result, effect_status,
        trace_id, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      entry.goal_key,
      entry.goal_id ?? null,
      entry.upstream_request_id,
      entry.caller_request_id ?? null,
      entry.tool,
      entry.payload_hash,
      entry.channel ?? CHANNEL.LOCAL_SOCKET,
      JOURNAL_PHASE.INTENT,
      encode(entry.pre_status_snapshot ?? null),
      null,
      encode(entry.upstream_arguments),
      null,
      EFFECT_STATUS.INDETERMINATE,
      entry.trace_id,
      createdAt,
      createdAt
    );
    return { created: true, entry: this.getEntry(entry.goal_key, entry.upstream_request_id) };
  }

  writeOutcome(goalKey, upstreamRequestId, outcome, lock = null) {
    this.ensureOpen();
    if (lock && !this.ownsLiveLock(lock.lockKey, lock.ownerToken)) {
      throw new JournalError("lock lease lost before writing journal outcome", "LOCK_LEASE_LOST");
    }
    const phase = outcome.phase ?? JOURNAL_PHASE.OUTCOME;
    const updatedAt = nowIso();
    this.db.prepare(`
      UPDATE gqb_journal
      SET goal_id = COALESCE(?, goal_id),
          phase = ?,
          post_status_snapshot = ?,
          upstream_result = ?,
          effect_status = ?,
          updated_at = ?
      WHERE goal_key = ? AND upstream_request_id = ?
    `).run(
      outcome.goal_id ?? null,
      phase,
      encode(outcome.post_status_snapshot ?? null),
      encode(outcome.upstream_result ?? null),
      outcome.effect_status,
      updatedAt,
      goalKey,
      upstreamRequestId
    );
    return this.getEntry(goalKey, upstreamRequestId);
  }

  listOpenIntents(goalKey = null) {
    this.ensureOpen();
    const rows = goalKey
      ? this.db.prepare("SELECT * FROM gqb_journal WHERE goal_key = ? AND phase = ? ORDER BY created_at").all(goalKey, JOURNAL_PHASE.INTENT)
      : this.db.prepare("SELECT * FROM gqb_journal WHERE phase = ? ORDER BY created_at").all(JOURNAL_PHASE.INTENT);
    return rows.map((row) => this.rowToEntry(row));
  }

  async acquireLock(lockKey, { timeoutMs = this.acquireTimeoutMs, leaseMs = this.leaseMs } = {}) {
    this.ensureOpen();
    const ownerToken = randomUUID();
    const deadline = Date.now() + timeoutMs;
    while (Date.now() <= deadline) {
      const now = Date.now();
      const acquiredAt = nowIso();
      const expiresAt = now + leaseMs;
      this.db.exec("BEGIN IMMEDIATE");
      try {
        const row = this.db.prepare("SELECT owner_token, expires_at FROM gqb_locks WHERE lock_key = ?").get(lockKey);
        if (!row) {
          this.db.prepare("INSERT INTO gqb_locks (lock_key, owner_token, acquired_at, expires_at, heartbeat_at) VALUES (?, ?, ?, ?, ?)")
            .run(lockKey, ownerToken, acquiredAt, expiresAt, acquiredAt);
          this.db.exec("COMMIT");
          return { lockKey, ownerToken, expiresAt };
        }
        if (Number(row.expires_at) <= now) {
          this.db.prepare("UPDATE gqb_locks SET owner_token = ?, acquired_at = ?, expires_at = ?, heartbeat_at = ? WHERE lock_key = ?")
            .run(ownerToken, acquiredAt, expiresAt, acquiredAt, lockKey);
          this.db.exec("COMMIT");
          return { lockKey, ownerToken, expiresAt };
        }
        this.db.exec("ROLLBACK");
      } catch (error) {
        this.db.exec("ROLLBACK");
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new JournalError(`lock unavailable: ${lockKey}`, "LOCK_UNAVAILABLE");
  }

  ownsLiveLock(lockKey, ownerToken) {
    this.ensureOpen();
    const row = this.db.prepare("SELECT owner_token, expires_at FROM gqb_locks WHERE lock_key = ?").get(lockKey);
    return row?.owner_token === ownerToken && Number(row.expires_at) > Date.now();
  }

  releaseLock(lock) {
    this.ensureOpen();
    if (!lock) return;
    this.db.prepare("DELETE FROM gqb_locks WHERE lock_key = ? AND owner_token = ?").run(lock.lockKey, lock.ownerToken);
  }

  close() {
    this.db?.close();
    this.db = null;
  }
}
