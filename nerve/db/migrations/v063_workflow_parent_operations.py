"""Link durable workflow parents to private, non-dispatchable Operations."""


async def up(db):
    await db.execute(
        "ALTER TABLE preset_workflows ADD COLUMN parent_operation_id TEXT "
        "REFERENCES executions(id)"
    )
    await db.execute(
        "CREATE UNIQUE INDEX uq_preset_workflows_parent_operation "
        "ON preset_workflows(parent_operation_id) WHERE parent_operation_id IS NOT NULL"
    )
    await db.execute(
        "ALTER TABLE executions ADD COLUMN parent_operation_id TEXT "
        "REFERENCES executions(id)"
    )
    await db.execute(
        "CREATE INDEX idx_executions_parent_operation "
        "ON executions(parent_operation_id) WHERE parent_operation_id IS NOT NULL"
    )
    # A workflow parent owns handles but has no backend work.  Keep this
    # explicit rather than relying on a mutable JSON plan convention.
    await db.execute(
        "ALTER TABLE executions ADD COLUMN private_operation INTEGER NOT NULL DEFAULT 0 "
        "CHECK(private_operation IN (0, 1))"
    )
    # Allocation happens after the durable parent intent is written.  A
    # restart must be able to distinguish that incomplete intent from a safe
    # workflow ready for dispatch.
    await db.execute(
        "ALTER TABLE preset_workflows ADD COLUMN allocation_state TEXT NOT NULL "
        "DEFAULT 'finalized' CHECK(allocation_state IN ('pending', 'finalized', 'failed'))"
    )
    await db.execute(
        "CREATE INDEX idx_preset_workflows_allocation_state "
        "ON preset_workflows(status, allocation_state, created_at)"
    )
