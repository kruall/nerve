"""V52: Durable, session-scoped reservations over the existing host lease pool."""
import aiosqlite

SQL = """
CREATE TABLE session_resource_reservations (
 session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
 pool TEXT NOT NULL,
 host_id TEXT NOT NULL REFERENCES resource_hosts(id),
 lease_id TEXT NOT NULL UNIQUE REFERENCES resource_leases(id),
 worktree_identity TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('active','released','quarantined')),
 created_at TEXT NOT NULL,
 released_at TEXT,
 quarantine_reason TEXT
);
CREATE UNIQUE INDEX uq_session_reservation_active_host
 ON session_resource_reservations(host_id) WHERE state IN ('active','quarantined');
CREATE INDEX idx_session_reservation_lease ON session_resource_reservations(lease_id);
"""

async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
