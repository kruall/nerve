"""Durable retained resource handles, wait operations, and recovery intents."""

import aiosqlite


SQL = """
CREATE TABLE session_resource_handles (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    pool TEXT NOT NULL,
    host_id TEXT NOT NULL REFERENCES resource_hosts(id),
    lease_id TEXT NOT NULL REFERENCES resource_leases(id),
    fencing_token INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active', 'releasing', 'released', 'quarantined')),
    release_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    released_at TEXT
);
CREATE UNIQUE INDEX uq_session_resource_handles_active_lease
    ON session_resource_handles(lease_id)
    WHERE state IN ('active', 'releasing', 'quarantined');
CREATE INDEX idx_session_resource_handles_active_session
    ON session_resource_handles(session_id, state)
    WHERE state IN ('active', 'releasing', 'quarantined');

CREATE TABLE operation_resource_refs (
    operation_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    handle_id TEXT NOT NULL REFERENCES session_resource_handles(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    PRIMARY KEY(operation_id, handle_id)
);
CREATE INDEX idx_operation_resource_refs_handle
    ON operation_resource_refs(handle_id, operation_id);

CREATE TABLE resource_wait_operations (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    request_kind TEXT NOT NULL CHECK(request_kind IN ('host', 'pool', 'bundle')),
    requested_hosts_json JSON NOT NULL,
    pool TEXT NOT NULL,
    queue_ticket INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending', 'granted', 'cancelled', 'failed')),
    outcome TEXT,
    wakeup_generation INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    settled_at TEXT
);
CREATE INDEX idx_resource_wait_operations_pending_ticket
    ON resource_wait_operations(queue_ticket, id)
    WHERE state = 'pending';

CREATE TABLE resource_recovery_intents (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK(kind IN ('acquire', 'release', 'reconcile')),
    session_id TEXT REFERENCES sessions(id) ON DELETE SET NULL,
    handle_id TEXT REFERENCES session_resource_handles(id) ON DELETE SET NULL,
    operation_id TEXT REFERENCES executions(id) ON DELETE SET NULL,
    payload_json JSON NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('prepared', 'processing', 'completed', 'failed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX idx_resource_recovery_intents_pending
    ON resource_recovery_intents(state, created_at, id)
    WHERE state IN ('prepared', 'processing');
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
