"""Remember the local YDB checkout pinned by a retained session handle."""


async def up(db):
    # Generic handles intentionally have no worktree affinity.  Only the
    # reviewed YDB builder adapter writes this private local identity.
    await db.execute(
        "ALTER TABLE session_resource_handles ADD COLUMN worktree_identity TEXT"
    )
