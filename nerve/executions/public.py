"""Public execution/resource contracts for the web and MCP facades.

The execution and inventory services are allowed to retain transport details,
connection references, and backend handles.  This module is the deliberate
allowlist between those trusted internals and user-facing payloads.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable


ACTIVE_EXECUTION_STATES = frozenset({
    "queued", "starting", "running", "cancelling",
})
TERMINAL_EXECUTION_STATES = frozenset({
    "succeeded", "failed", "cancelled", "lost",
})
MAX_LOG_TAIL_LINES = 500
DEFAULT_LOG_TAIL_LINES = 200
MAX_LOG_LINE_CHARS = 16_384
MAX_LOG_TAIL_CHARS = 256 * 1024


@runtime_checkable
class ExecutionUiService(Protocol):
    """Frontend-facing slice implemented by the detached lifecycle service."""

    async def session_activity(
        self, *, session_ids: Sequence[str],
    ) -> Mapping[str, Mapping[str, Any]]: ...

    async def list_executions(
        self, *, session_id: str, include_terminal: bool, limit: int,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def get_execution(self, *, execution_id: str) -> Mapping[str, Any] | None: ...

    async def tail_logs(
        self, *, execution_id: str, limit: int, before: int | None,
    ) -> Mapping[str, Any]: ...

    async def cancel_execution(
        self, *, execution_id: str, requested_by: str, reason: str | None,
    ) -> Mapping[str, Any]: ...

    async def retry_execution(
        self, *, execution_id: str, profile_mode: str, requested_by: str,
    ) -> Mapping[str, Any]: ...

    async def dismiss_execution(
        self, *, execution_id: str, session_id: str, requested_by: str,
    ) -> Mapping[str, Any]: ...


@runtime_checkable
class ResourceUiService(Protocol):
    """Frontend-facing slice implemented by inventory/lease management."""

    async def resource_snapshot(self) -> Mapping[str, Any]: ...

    async def set_host_draining(
        self, *, host_id: str, draining: bool, requested_by: str,
    ) -> Mapping[str, Any]: ...

    async def recover_host(
        self, *, host_id: str, requested_by: str,
    ) -> Mapping[str, Any]: ...

    async def permanently_lose_host(
        self, *, host_id: str, confirm_host_id: str, requested_by: str,
    ) -> Mapping[str, Any]: ...


def _text(value: Any, *, maximum: int = 4096) -> str | None:
    if not isinstance(value, str):
        return None
    return value[:maximum]


def _scalar(value: Any) -> str | int | float | bool | None:
    if isinstance(value, str):
        return value[:4096]
    return value if isinstance(value, (int, float, bool)) else None


def _public_lease(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    result: dict[str, Any] = {}
    for key in (
        "id", "state", "host_id", "execution_id", "session_id", "pool",
        "fencing_token", "requested_at", "acquired_at", "heartbeat_at",
        "revoking_at", "released_at", "quarantine_reason",
    ):
        value = _scalar(raw.get(key))
        if value is not None:
            result[key] = value
    return result


def public_execution(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return the stable, secret-free execution representation."""
    result: dict[str, Any] = {}
    for key in (
        "id", "session_id", "kind", "profile_version", "profile_hash",
        "status", "revision", "auto_continue", "requested_pool", "queue_position",
        "created_at", "queued_at", "started_at", "finished_at", "updated_at",
        "duration_ms", "cancel_requested_at", "cancel_reason", "dismissed_at",
    ):
        value = _scalar(raw.get(key))
        if value is not None:
            if key == "auto_continue":
                value = bool(value)
            result[key] = value

    host = raw.get("selected_host")
    if isinstance(host, Mapping):
        selected = {
            key: value
            for key in ("id", "display_name", "state")
            if (value := _scalar(host.get(key))) is not None
        }
        if selected:
            result["selected_host"] = selected
    elif isinstance(host, str):
        result["selected_host"] = {"id": host}

    requests = raw.get("resource_requests")
    if isinstance(requests, Sequence) and not isinstance(requests, (str, bytes)):
        public_requests: list[dict[str, Any]] = []
        for request in requests:
            if not isinstance(request, Mapping):
                continue
            item = {
                key: value
                for key in ("slot", "pool", "mode", "state")
                if (value := _scalar(request.get(key))) is not None
            }
            if item:
                public_requests.append(item)
        result["resource_requests"] = public_requests

    lease = _public_lease(raw.get("lease"))
    if lease is not None:
        result["lease"] = lease

    terminal = raw.get("result")
    if isinstance(terminal, Mapping):
        public_result: dict[str, Any] = {}
        for key in ("outcome", "exit_code", "signal", "verification_status", "spin_version"):
            value = _scalar(terminal.get(key))
            if value is not None:
                public_result[key] = value
        summary = _text(terminal.get("summary"))
        if summary is not None:
            public_result["summary"] = summary
        error = _text(terminal.get("error"))
        if error is not None:
            public_result["error"] = error
        result["result"] = public_result

    continuation = raw.get("continuation")
    if isinstance(continuation, Mapping):
        public_continuation: dict[str, Any] = {}
        for key in (
            "state", "session_id", "message_id", "attempted_at", "completed_at",
        ):
            value = _scalar(continuation.get(key))
            if value is not None:
                if key == "state":
                    value = {
                        "none": "not_requested",
                        "claimed": "running",
                        "completed": "succeeded",
                    }.get(str(value), value)
                public_continuation[key] = value
        error = _text(continuation.get("error"))
        if error is not None:
            public_continuation["error"] = error
        result["continuation"] = public_continuation

    return result


def public_log_tail(raw: Mapping[str, Any], *, requested_limit: int) -> dict[str, Any]:
    """Bound line count, per-line size, and aggregate response size."""
    limit = max(1, min(int(requested_limit), MAX_LOG_TAIL_LINES))
    entries = raw.get("entries", raw.get("lines", []))
    public_entries: list[dict[str, Any]] = []
    used = 0
    if isinstance(entries, Sequence) and not isinstance(entries, (str, bytes)):
        for entry in list(entries)[-limit:]:
            if isinstance(entry, Mapping):
                text = _text(entry.get("text", entry.get("content", "")), maximum=MAX_LOG_LINE_CHARS) or ""
                item = {
                    key: value
                    for key in ("sequence", "timestamp", "stream")
                    if (value := _scalar(entry.get(key))) is not None
                }
            else:
                text = _text(entry, maximum=MAX_LOG_LINE_CHARS) or ""
                item = {}
            remaining = MAX_LOG_TAIL_CHARS - used
            if remaining <= 0:
                break
            if len(text) > remaining:
                text = text[:remaining]
                item["truncated"] = True
            item["text"] = text
            used += len(text)
            public_entries.append(item)

    result: dict[str, Any] = {"entries": public_entries, "limit": limit}
    for key in ("next_before", "oldest_sequence", "newest_sequence", "has_more", "truncated"):
        value = _scalar(raw.get(key))
        if value is not None:
            result[key] = value
    if (
        isinstance(entries, Sequence)
        and not isinstance(entries, (str, bytes))
        and len(public_entries) < len(entries)
    ):
        result["truncated"] = True
    return result


def public_resource_snapshot(raw: Mapping[str, Any]) -> dict[str, Any]:
    pools: list[dict[str, Any]] = []
    for pool in raw.get("pools", []):
        if not isinstance(pool, Mapping):
            continue
        item: dict[str, Any] = {}
        for key in (
            "id", "title", "description", "selector_summary", "total_hosts",
            "available_hosts", "queue_depth", "enabled",
        ):
            value = _scalar(pool.get(key))
            if value is not None:
                item[key] = value
        members = pool.get("member_ids")
        if isinstance(members, Sequence) and not isinstance(members, (str, bytes)):
            item["member_ids"] = [str(member) for member in members]
        pools.append(item)

    hosts: list[dict[str, Any]] = []
    for host in raw.get("hosts", []):
        if not isinstance(host, Mapping):
            continue
        item = {}
        for key in (
            "id", "display_name", "state", "enabled", "draining", "offline",
            "quarantined", "quarantine_reason", "last_seen_at",
        ):
            value = _scalar(host.get(key))
            if value is not None:
                item[key] = value
        for key in ("labels", "capabilities", "pools"):
            values = host.get(key)
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
                item[key] = [str(value) for value in values]
            elif isinstance(values, Mapping):
                item[key] = [
                    f"{name}={value}"[:512]
                    for name, value in values.items()
                    if isinstance(name, str) and isinstance(value, (str, int, float, bool))
                ]
        lease = _public_lease(host.get("current_lease"))
        if lease is not None:
            item["current_lease"] = lease
        hosts.append(item)

    leases = [
        public for lease in raw.get("leases", [])
        if (public := _public_lease(lease)) is not None
    ]
    queue: list[dict[str, Any]] = []
    for request in raw.get("queue", []):
        if not isinstance(request, Mapping):
            continue
        queue.append({
            key: value
            for key in (
                "id", "execution_id", "session_id", "pool", "position",
                "state", "requested_at",
            )
            if (value := _scalar(request.get(key))) is not None
        })
    reservations: list[dict[str, Any]] = []
    for reservation in raw.get("session_reservations", []):
        if not isinstance(reservation, Mapping):
            continue
        reservations.append({
            key: value
            for key in ("session_id", "pool", "host_id", "lease_id", "state", "created_at")
            if (value := _scalar(reservation.get(key))) is not None
        })
    return {
        "pools": pools,
        "hosts": hosts,
        "leases": leases,
        "queue": queue,
        "session_reservations": reservations,
        "updated_at": _scalar(raw.get("updated_at")),
    }
