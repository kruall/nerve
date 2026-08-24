"""Trusted SSH inventory dynamic state and globally-exclusive leases."""
import aiosqlite

SQL = """
CREATE TABLE resource_hosts (
 id TEXT PRIMARY KEY, connection_ref TEXT NOT NULL, display_name TEXT NOT NULL,
 labels JSON NOT NULL DEFAULT '{}', capabilities JSON NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1,
 draining INTEGER NOT NULL DEFAULT 0, offline INTEGER NOT NULL DEFAULT 0, quarantined INTEGER NOT NULL DEFAULT 0,
 quarantine_reason TEXT, fencing_token INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE resource_leases (
 id TEXT PRIMARY KEY, host_id TEXT NOT NULL REFERENCES resource_hosts(id), execution_id TEXT NOT NULL,
 session_id TEXT NOT NULL, pool TEXT NOT NULL, fencing_token INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('active','revoking','released','quarantined')),
 requested_at TEXT NOT NULL, acquired_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL, revoking_at TEXT, released_at TEXT, quarantine_reason TEXT
);
CREATE UNIQUE INDEX uq_resource_host_active_exclusive ON resource_leases(host_id) WHERE state IN ('active','revoking','quarantined');
CREATE INDEX idx_resource_leases_execution ON resource_leases(execution_id, state);
"""
async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
