"""Durable fencing for quarantined-host reconciliation."""


async def up(db):
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN recovery_generation INTEGER NOT NULL DEFAULT 0")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN recovery_claimed_generation INTEGER NOT NULL DEFAULT 0")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN permanently_unavailable INTEGER NOT NULL DEFAULT 0")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN recovery_claim_state TEXT NOT NULL DEFAULT 'idle' CHECK(recovery_claim_state IN ('idle','claimed','retry'))")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN recovery_claimed_at TEXT")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN recovery_claim_expires_at TEXT")
    await db.execute("ALTER TABLE resource_hosts ADD COLUMN recovery_retry_at TEXT")
    await db.execute("CREATE INDEX idx_resource_hosts_recovery_claim ON resource_hosts(quarantined, permanently_unavailable, recovery_claim_state, recovery_claim_expires_at, recovery_retry_at)")
