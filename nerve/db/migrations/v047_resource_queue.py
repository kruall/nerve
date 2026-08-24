"""FIFO resource requests, lease expiry, and durable audit events."""
import aiosqlite

SQL = """
ALTER TABLE resource_leases ADD COLUMN expires_at TEXT;

CREATE TABLE resource_lease_requests (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    execution_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    slot TEXT NOT NULL,
    pool TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode = 'exclusive'),
    state TEXT NOT NULL CHECK (state IN ('queued', 'acquired', 'cancelled')),
    lease_id TEXT REFERENCES resource_leases(id),
    requested_at TEXT NOT NULL,
    settled_at TEXT
);
CREATE UNIQUE INDEX uq_resource_request_active_slot
    ON resource_lease_requests(execution_id, slot)
    WHERE state = 'queued';
CREATE INDEX idx_resource_request_fifo
    ON resource_lease_requests(pool, state, sequence);

CREATE TABLE resource_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    host_id TEXT,
    execution_id TEXT,
    lease_id TEXT,
    detail TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX idx_resource_events_created
    ON resource_events(created_at DESC, sequence DESC);
"""

async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
