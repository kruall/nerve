"""Persist exact hosts on ordinary resource queue requests."""

import json

import aiosqlite


SQL = """
ALTER TABLE resource_lease_requests ADD COLUMN requested_host TEXT;
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)

    updates: list[tuple[str, str]] = []
    async with db.execute(
        """SELECT request.id, request.slot, execution.plan
           FROM resource_lease_requests AS request
           JOIN executions AS execution ON execution.id = request.execution_id
           WHERE request.state IN ('queued', 'acquired')
             AND request.requested_host IS NULL"""
    ) as cursor:
        async for row in cursor:
            try:
                plan = json.loads(row[2])
                resource_hosts = plan.get("resource_hosts")
                host = resource_hosts.get(str(row[1])) if isinstance(resource_hosts, dict) else None
            except (TypeError, ValueError, AttributeError):
                continue
            if isinstance(host, str) and host and "\x00" not in host:
                updates.append((host, str(row[0])))

    if updates:
        await db.executemany(
            """UPDATE resource_lease_requests
               SET requested_host=?
               WHERE id=? AND state IN ('queued', 'acquired')
                 AND requested_host IS NULL""",
            updates,
        )
