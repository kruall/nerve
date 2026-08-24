"""Allow concurrent Operations while fencing non-shareable retained handles.

V060 made ``operation_resource_refs`` durable and ordered.  There is no
kind/profile shareability metadata in the retained-handle contract, so each
handle is deliberately non-shareable.  The unique index is the cross-process
serialization point: terminal transitions detach refs before a handle can be
used by another Operation.
"""


async def up(db):
    # V060 databases could contain references left behind by terminal
    # executions. They are no longer ownership proofs and would make the new
    # unique fence reject an otherwise valid schema upgrade. Active refs are
    # deliberately untouched: a corrupt active overlap must fail migration
    # rather than silently selecting a winner.
    await db.execute(
        """DELETE FROM operation_resource_refs
           WHERE operation_id IN (
               SELECT id FROM executions
               WHERE status IN ('succeeded', 'failed', 'cancelled', 'lost')
           )"""
    )
    # Install the per-handle fence first.  If this DDL ever fails, the old
    # per-session invariant remains in force rather than leaving a gap.
    await db.execute(
        "CREATE UNIQUE INDEX uq_operation_resource_refs_one_active_per_handle "
        "ON operation_resource_refs(handle_id)"
    )
    await db.execute("DROP INDEX IF EXISTS uq_executions_one_active_per_session")
