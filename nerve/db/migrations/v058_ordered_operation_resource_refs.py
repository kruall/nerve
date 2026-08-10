"""Persist operation handle order and legacy idle-release ownership."""


async def up(db):
    await db.execute(
        "ALTER TABLE session_resource_handles ADD COLUMN auto_release_when_session_idle INTEGER NOT NULL DEFAULT 0"
    )
    # SQLite cannot add a NOT NULL column without a default. Rebuild so every
    # reference has a durable ordinal.  V53 did not record ordering; its only
    # stable inputs are timestamp and handle id, so use that historical order.
    await db.execute("ALTER TABLE operation_resource_refs RENAME TO operation_resource_refs_v057")
    await db.execute("""CREATE TABLE operation_resource_refs (
        operation_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
        handle_id TEXT NOT NULL REFERENCES session_resource_handles(id) ON DELETE CASCADE,
        position INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY(operation_id, handle_id),
        UNIQUE(operation_id, position)
    )""")
    await db.execute("""INSERT INTO operation_resource_refs(operation_id, handle_id, position, created_at)
        SELECT target.operation_id, target.handle_id, COUNT(prior.handle_id), target.created_at
          FROM operation_resource_refs_v057 AS target
          LEFT JOIN operation_resource_refs_v057 AS prior
            ON prior.operation_id=target.operation_id
           AND (prior.created_at < target.created_at
                OR (prior.created_at=target.created_at AND prior.handle_id < target.handle_id))
         GROUP BY target.operation_id, target.handle_id, target.created_at""")
    await db.execute("DROP TABLE operation_resource_refs_v057")
    await db.execute("CREATE INDEX idx_operation_resource_refs_order ON operation_resource_refs(operation_id, position)")
    await db.execute("CREATE INDEX idx_operation_resource_refs_handle ON operation_resource_refs(handle_id, operation_id)")
