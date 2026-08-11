"""Persist the latest reviewed remote-supervisor provisioning result."""


async def up(db):
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN supervisor_provisioned_at TEXT")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN supervisor_provision_status TEXT CHECK(supervisor_provision_status IN ('ready', 'failed'))")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN supervisor_provision_error TEXT")
