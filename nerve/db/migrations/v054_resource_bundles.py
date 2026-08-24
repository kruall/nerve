"""Durable all-or-none resource bundle allocation."""
import aiosqlite

SQL = """
CREATE TABLE resource_lease_bundles (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued', 'acquired', 'released', 'cancelled')),
    requested_at TEXT NOT NULL,
    settled_at TEXT
);
CREATE INDEX idx_resource_lease_bundles_fifo ON resource_lease_bundles(state, requested_at, id);
CREATE INDEX idx_resource_lease_bundles_execution ON resource_lease_bundles(execution_id, state);
ALTER TABLE resource_lease_requests ADD COLUMN bundle_id TEXT REFERENCES resource_lease_bundles(id);
CREATE INDEX idx_resource_request_bundle ON resource_lease_requests(bundle_id, state, sequence);
"""

async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
