"""Give every queued resource entity one durable, shared queue ticket."""

import aiosqlite


SQL = """
ALTER TABLE resource_lease_requests ADD COLUMN queue_ticket INTEGER NOT NULL DEFAULT 0;
CREATE TABLE resource_wait_allocator (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    next_ticket INTEGER NOT NULL
);
INSERT INTO resource_wait_allocator(singleton, next_ticket) VALUES (1, 1);
CREATE INDEX idx_resource_wait_operations_pending_ticket_host
    ON resource_wait_operations(state, queue_ticket, id)
    WHERE state = 'pending';
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    entities: list[tuple[str, str, str, list[str]]] = []
    async with db.execute("SELECT id, created_at FROM resource_wait_operations WHERE state='pending'") as cursor:
        async for row in cursor:
            entities.append((str(row[1]), "wait", str(row[0]), []))
    async with db.execute("""SELECT COALESCE(bundle_id, id), MIN(requested_at), GROUP_CONCAT(id)
                                  FROM resource_lease_requests WHERE state='queued'
                                  GROUP BY COALESCE(bundle_id, id)""") as cursor:
        async for row in cursor:
            entities.append((str(row[1]), "request", str(row[0]), str(row[2]).split(",")))
    # Runtime tickets are unique.  This is solely a deterministic upgrade
    # tie-breaker, with no queue-type priority in normal operation.
    entities.sort(key=lambda item: (item[0], item[2], item[1]))
    for ticket, (_created, kind, entity_id, request_ids) in enumerate(entities, start=1):
        if kind == "wait":
            await db.execute("UPDATE resource_wait_operations SET queue_ticket=? WHERE id=?", (ticket, entity_id))
        else:
            await db.executemany("UPDATE resource_lease_requests SET queue_ticket=? WHERE id=?", ((ticket, request_id) for request_id in request_ids))
    await db.execute("UPDATE resource_wait_allocator SET next_ticket=? WHERE singleton=1", (len(entities) + 1,))
