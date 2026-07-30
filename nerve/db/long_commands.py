"""Persistence for detached build/test commands that resume a session."""

from __future__ import annotations


class LongCommandStore:
    async def add_long_command(self, job: dict) -> None:
        await self._write(
            """INSERT INTO session_long_commands
               (id, session_id, command_json, cwd, output_path, status_path,
                process_pid, timeout_at, prompt, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running')""",
            (job["id"], job["session_id"], job["command_json"], job["cwd"],
             job["output_path"], job["status_path"], job["process_pid"],
             job["timeout_at"], job["prompt"]),
        )

    async def list_running_long_commands(self) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM session_long_commands WHERE status = 'running' "
            "ORDER BY created_at ASC"
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def set_long_command_pid(self, command_id: str, process_pid: int) -> None:
        await self._write(
            "UPDATE session_long_commands SET process_pid = ? WHERE id = ?",
            (process_pid, command_id),
        )

    async def list_pending_long_command_resumes(self) -> list[dict]:
        async with self.db.execute(
            """SELECT * FROM session_long_commands
               WHERE resume_state = 'pending' ORDER BY created_at ASC"""
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def finish_long_command(
        self, command_id: str, status: str, exit_code: int | None, details: str,
    ) -> bool:
        result = await self._write(
            """UPDATE session_long_commands
               SET status = ?, exit_code = ?, details = ?, finished_at = CURRENT_TIMESTAMP
               WHERE id = ? AND status = 'running'""",
            (status, exit_code, details, command_id),
        )
        return bool(result.rowcount)

    async def claim_long_command_resume(self, command_id: str) -> bool:
        result = await self._write(
            """UPDATE session_long_commands SET resume_state = 'dispatched'
               WHERE id = ? AND resume_state = 'pending'""",
            (command_id,),
        )
        return bool(result.rowcount)

    async def complete_long_command_resume(self, command_id: str) -> None:
        await self._write(
            "UPDATE session_long_commands SET resume_state = 'completed' WHERE id = ?",
            (command_id,),
        )

    async def requeue_dispatched_long_command_resumes(self) -> None:
        await self._write(
            """UPDATE session_long_commands SET resume_state = 'pending'
               WHERE resume_state = 'dispatched'"""
        )
