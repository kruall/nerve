"""Enforce the durable resource-wait outcome vocabulary without rewriting V053."""

import aiosqlite
_OUTCOMES = "'LEASE_GRANTED', 'HOST_PERMANENTLY_UNAVAILABLE', 'REQUEST_CANCELLED', 'DEADLOCK_REPLAN_REQUIRED'"
SQL = f"""
CREATE TRIGGER resource_wait_operations_outcome_insert
BEFORE INSERT ON resource_wait_operations
WHEN NEW.outcome IS NOT NULL AND NEW.outcome NOT IN ({_OUTCOMES})
BEGIN
    SELECT RAISE(ABORT, 'invalid resource wait outcome');
END;
CREATE TRIGGER resource_wait_operations_outcome_update
BEFORE UPDATE OF outcome ON resource_wait_operations
WHEN NEW.outcome IS NOT NULL AND NEW.outcome NOT IN ({_OUTCOMES})
BEGIN
    SELECT RAISE(ABORT, 'invalid resource wait outcome');
END;
"""
async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
