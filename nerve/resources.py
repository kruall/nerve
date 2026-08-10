"""Trusted static inventory and global physical-host leases.

Connection references are names only.  Endpoint fields are intentionally not
accepted here: a future SSH transport resolves the named trusted connection.
"""
from __future__ import annotations
import asyncio
import contextlib
import json
from pathlib import Path
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from nerve.utils.time import utc_now_iso


class ResourceInventoryError(ValueError): pass


class ResourceHandleOwnershipError(PermissionError):
    """The stable response for an absent or foreign retained handle."""

    def __init__(self) -> None:
        super().__init__("resource handle does not belong to this session")


class ResourceHandleConflictError(RuntimeError):
    """A retained handle cannot be released while Operations reference it."""

    def __init__(self, operation_ids: Sequence[str]) -> None:
        self.operation_ids = tuple(operation_ids)
        super().__init__("resource handle is referenced by operations: " + ", ".join(self.operation_ids))


class ResourceRecoveryUnavailable(RuntimeError):
    """The retryable response while durable startup recovery owns the subsystem."""

    def __init__(self) -> None:
        super().__init__("resource recovery is in progress; retry shortly")


class ResourceRecoveryGate:
    """One exclusive startup owner for resource intents and execution recovery."""

    def __init__(self, *, ready: bool = True) -> None:
        self._ready = ready
        self._lock = asyncio.Lock()
        self._recovery_task: asyncio.Task[Any] | None = None

    @property
    def ready(self) -> bool:
        return self._ready

    def require_ready(self) -> None:
        if not self._ready and asyncio.current_task() is not self._recovery_task:
            raise ResourceRecoveryUnavailable()

    async def open(self, resources: Any, executions: Any, *, dispatch_continuations: bool) -> None:
        async with self._lock:
            if self._ready:
                return
            self._recovery_task = asyncio.current_task()
            try:
                await resources._recover_startup()
                for intent in await resources.db.list_resource_recovery_intents():
                    if intent["state"] in {"prepared", "processing"}:
                        await resources._replay_recovery_intent(intent)
                await resources._start_reconcile_loop()
                # Reattachment can issue fresh backend calls.  The durable
                # intent replay is complete now, so open before it can run.
                self._ready = True
                await resources._reattach_pending_waits()
                await executions._recover_startup(dispatch_continuations=dispatch_continuations)
            finally:
                self._recovery_task = None


class ResourceInventory:
    def __init__(self, db: Any, config: Mapping[str, Any] | None = None):
        self.db = db
        raw = dict(config or {})
        self.config = raw
        self.connections = set(raw.get("connections", []))
        # Local artifacts are an explicit deployment-owned allowlist.  Keep
        # the canonical paths private to the backend; plans carry only ids.
        roots = raw.get("local_artifact_roots", {})
        if not isinstance(roots, Mapping):
            raise ResourceInventoryError("local_artifact_roots must be a mapping")
        self.local_artifact_roots: dict[str, Path] = {}
        for ident, value in roots.items():
            if (not isinstance(ident, str) or not ident or not isinstance(value, str)
                    or not value or "\0" in value):
                raise ResourceInventoryError("local artifact roots must have non-empty ids and paths")
            path = Path(value)
            if not path.is_absolute():
                raise ResourceInventoryError("local artifact roots must be absolute paths")
            self.local_artifact_roots[ident] = path.resolve(strict=False)
        host_rows = [dict(h) for h in raw.get("hosts", []) if isinstance(h, Mapping) and h.get("id")]
        pool_rows = [dict(p) for p in raw.get("pools", []) if isinstance(p, Mapping) and p.get("id")]
        if len({str(h["id"]) for h in host_rows}) != len(host_rows):
            raise ResourceInventoryError("duplicate host id")
        if len({str(p["id"]) for p in pool_rows}) != len(pool_rows):
            raise ResourceInventoryError("duplicate pool id")
        self.hosts = {str(h["id"]): h for h in host_rows}
        self.pools = {str(p["id"]): p for p in pool_rows}
        self._validate()

    def _validate(self) -> None:
        for host in self.hosts.values():
            if set(host) & {"hostname", "host", "user", "port", "ssh_options"}:
                raise ResourceInventoryError("hosts must use a named connection_ref, not raw SSH coordinates")
            if not isinstance(host.get("connection_ref"), str) or host["connection_ref"] not in self.connections:
                raise ResourceInventoryError("host references an unknown connection")
        for pool_id, pool in self.pools.items():
            members = pool.get("members", [])
            selector = pool.get("selector", {})
            if not isinstance(members, list) or not isinstance(selector, Mapping):
                raise ResourceInventoryError(f"pool {pool_id} has invalid members or selector")
            if any(member not in self.hosts for member in members):
                raise ResourceInventoryError(f"pool {pool_id} references an unknown host")
            if not members and not selector:
                raise ResourceInventoryError(f"pool {pool_id} is empty")

    async def initialize(self) -> None:
        for host in self.hosts.values(): await self.db.seed_resource_host(host)

    def members(self, pool: str) -> list[str]:
        item = self.pools.get(pool)
        if item is None: raise ResourceInventoryError("unknown host pool")
        selector = dict(item.get("selector", {}))
        result = set(item.get("members", []))
        for ident, host in self.hosts.items():
            labels = host.get("labels", {})
            if selector and isinstance(labels, Mapping) and all(labels.get(k) == v for k, v in selector.items()): result.add(ident)
        return sorted(result)


class LeaseService:
    def __init__(self, *, db: Any, inventory: ResourceInventory,
                 recovery_gate: ResourceRecoveryGate | None = None):
        self.db, self.inventory = db, inventory
        self.recovery_gate = recovery_gate or ResourceRecoveryGate()
        raw = getattr(inventory, "config", {})
        self.ttl_seconds = max(5, int(raw.get("lease_ttl_seconds", 90)))
        self.poll_seconds = max(0.01, float(raw.get("queue_poll_seconds", 0.25)))
        self._reconciler: asyncio.Task[Any] | None = None
        self._wait_tasks: dict[str, asyncio.Task[Any]] = {}
        self._wait_continuation_publisher: Any = None
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._service_started_at = utc_now_iso()

    @staticmethod
    def canonical_worktree_identity(worktree: str | Path) -> str:
        """A stable local identity; callers never supply a remote location."""
        return str(Path(worktree).expanduser().resolve(strict=False))

    @staticmethod
    def _reservation_execution_id(session_id: str) -> str:
        return f"session-reservation:{session_id}"

    def _is_recovered_session_reservation(self, reservation: Mapping[str, Any]) -> bool:
        created_at = reservation.get("created_at")
        if created_at is None:
            return False
        return str(created_at) < self._service_started_at

    async def reserve_for_session(
        self, *, session_id: str, pool: str, worktree: str | Path,
    ) -> Mapping[str, Any]:
        """Lazily reserve one pooled host and pin it to one local worktree."""
        identity = self.canonical_worktree_identity(worktree)
        existing = await self.db.get_session_resource_reservation(session_id)
        if existing is not None and existing["state"] == "active":
            if existing["worktree_identity"] != identity:
                raise ResourceInventoryError("session reservation is pinned to a different worktree")
            if existing["pool"] != pool:
                raise ResourceInventoryError("session reservation is pinned to a different pool")
            lease = await self.db.get_resource_lease(existing["lease_id"])
            if lease is None or lease["state"] != "active":
                # The durable reservation row can outlive a stale session lease
                # across a restart. Re-acquire it rather than failing execution
                # scheduling with a hard error.
                await self.db.cancel_resource_requests(self._reservation_execution_id(session_id))
                await self.db.settle_session_resource_reservation(
                    session_id=session_id,
                    state="released",
                    reason="session reservation lease is no longer active",
                )
                existing = None
            else:
                return {**existing, "lease": lease}
        if existing is not None and existing["state"] != "released":
            raise ResourceInventoryError("session reservation has already been settled")
        execution_id = self._reservation_execution_id(session_id)
        leases = await self.acquire(
            execution_id=execution_id, session_id=session_id,
            requests=[{"slot": "session", "pool": pool}],
        )
        lease = leases[0]
        try:
            reservation = await self.db.create_session_resource_reservation(
                session_id=session_id, pool=pool, worktree_identity=identity, lease=lease,
            )
        except Exception:
            await self.release(execution_id=execution_id, leases=leases)
            raise
        return {**reservation, "lease": lease}

    @contextlib.asynccontextmanager
    async def use_session_reservation(
        self, *, session_id: str, pool: str, worktree: str | Path,
    ):
        """Serialize future remote commands sharing a session reservation."""
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            yield await self.reserve_for_session(session_id=session_id, pool=pool, worktree=worktree)

    @contextlib.asynccontextmanager
    async def use_active_session_reservation(self, *, session_id: str):
        """Serialize inspection with commands on an existing reservation."""
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            reservation = await self.db.get_session_resource_reservation(session_id)
            if reservation is None or reservation["state"] != "active":
                raise ResourceInventoryError("no active YDB session reservation")
            lease = await self.db.get_resource_lease(reservation["lease_id"])
            if lease is None or lease["state"] != "active":
                raise ResourceInventoryError("session reservation is no longer usable")
            yield {**reservation, "lease": lease}

    async def release_session_reservation(
        self, *, session_id: str, remote_quiescence_confirmed: bool,
        reason: str = "session reservation cleanup",
    ) -> bool:
        """Release only after quiescence; uncertainty deliberately quarantines."""
        self.recovery_gate.require_ready()
        reservation = await self.db.get_session_resource_reservation(session_id)
        if reservation is None:
            return bool(await self.db.cancel_resource_requests(
                self._reservation_execution_id(session_id),
            ))
        if reservation["state"] != "active":
            return False
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        uncertain = lock.locked() or not remote_quiescence_confirmed
        lease = await self.db.get_resource_lease(reservation["lease_id"])
        if lease is not None:
            if uncertain:
                await self.quarantine(execution_id=lease["execution_id"], leases=[lease], reason=reason)
                await self.db.settle_session_resource_reservation(session_id=session_id, state="quarantined", reason=reason)
            else:
                await self.release(execution_id=lease["execution_id"], leases=[lease])
                await self.db.settle_session_resource_reservation(session_id=session_id, state="released")
        return True

    async def cleanup_session_reservation(self, session_id: str) -> bool:
        """Lifecycle hook: when quiescence is known, release; otherwise quarantine."""
        reservation = await self.db.get_session_resource_reservation(session_id)
        confirmed = (
            reservation is not None
            and not self._is_recovered_session_reservation(reservation)
        )
        return await self.release_session_reservation(
            session_id=session_id, remote_quiescence_confirmed=confirmed,
            reason="session lifecycle ended before remote quiescence was confirmed",
        )

    async def initialize(self) -> None:
        await self._recover_startup()
        await self._start_reconcile_loop()
        await self._reattach_pending_waits()

    async def _recover_startup(self) -> None:
        await self.cancel_orphaned_queued_session_reservations()
        await self.release_idle_recovered_session_reservations()
        await self.reconcile_expired()

    async def _reattach_pending_waits(self) -> None:
        if not self.recovery_gate.ready:
            return
        for wait in await self.db.list_resource_wait_operations(state="pending"):
            self._reattach_wait(wait)

    async def _start_reconcile_loop(self) -> None:
        if self._reconciler is None:
            self._reconciler = asyncio.create_task(self._reconcile_loop())

    async def _replay_recovery_intent(self, intent: Mapping[str, Any]) -> None:
        """Terminally resolve an interrupted intent; ambiguity quarantines first."""
        intent_id = str(intent["id"])
        if intent["state"] == "prepared" and not await self.db.update_resource_recovery_intent(
            intent_id, expected_state="prepared", state="processing",
        ):
            return
        try:
            payload = json.loads(str(intent.get("payload_json") or "{}"))
        except (TypeError, ValueError):
            payload = {}
        lease_ids = {str(x) for x in payload.get("lease_ids", []) if x}
        if payload.get("lease_id"):
            lease_ids.add(str(payload["lease_id"]))
        if intent.get("handle_id"):
            handle = await self.db.get_session_resource_handle(str(intent["handle_id"]))
            if handle:
                lease_ids.add(str(handle["lease_id"]))
        operation_id = intent.get("operation_id") or payload.get("allocator_execution_id")
        leases = [lease for lease in await self.db.list_resource_leases()
                  if str(lease["id"]) in lease_ids or (
                      operation_id is not None and str(lease["execution_id"]) == str(operation_id)
                  )]
        # No crash-interrupted remote action proves quiescence. Quarantine all
        # live associated leases before making the durable intent terminal.
        for lease in leases:
            if lease["state"] in {"active", "revoking"}:
                await self.quarantine(execution_id=str(lease["execution_id"]), leases=[lease],
                                      reason="startup recovery intent has ambiguous remote quiescence")
        await self.db.update_resource_recovery_intent(
            intent_id, expected_state="processing", state="failed" if leases else "completed",
        )

    async def shutdown(self) -> None:
        # Wait rows and allocator queue rows are durable.  Do not cancel or
        # release them on an ordinary agent/service stop; a new service simply
        # attaches another local poller to the same queued bundle.
        for task in self._wait_tasks.values():
            task.cancel()
        await asyncio.gather(*self._wait_tasks.values(), return_exceptions=True)
        self._wait_tasks.clear()
        if self._reconciler is not None:
            self._reconciler.cancel()
            await asyncio.gather(self._reconciler, return_exceptions=True)
            self._reconciler = None

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(min(30.0, max(1.0, self.ttl_seconds / 3)))
            await self.reconcile_expired()

    async def reconcile_expired(self) -> Sequence[Mapping[str, Any]]:
        # Expiry is evidence that ownership is stale, never that the remote is
        # stopped.  REVOKING remains covered by the global unique index.
        # A durable session reservation intentionally outlives an individual
        # command.  Renew it after restart as well as during normal service.
        for reservation in await self.db.list_active_session_resource_reservations():
            lease = await self.db.get_resource_lease(reservation["lease_id"])
            if lease is not None:
                await self.heartbeat(execution_id=lease["execution_id"], lease=lease)
        return await self.db.revoke_expired_resource_leases()

    async def release_idle_recovered_session_reservations(self) -> None:
        """Drop pre-restart cache reservations that do not back live work.

        A session reservation is an optimization for consecutive YDB operations,
        not durable work in its own right.  The previous implementation renewed
        every reservation during startup, including reservations whose owner had
        no active execution.  Such a lease then survived indefinitely and could
        starve the builder pool after a daemon restart.

        An active execution remains the durable ownership proof, so its
        reservation is retained for execution recovery.  Otherwise the daemon
        shutdown boundary makes the cached host safely releasable.
        """
        for reservation in await self.db.list_active_session_resource_reservations():
            if not self._is_recovered_session_reservation(reservation):
                continue
            active = await self.db.list_session_executions(
                str(reservation["session_id"]), include_terminal=False, limit=1,
            )
            if not active:
                await self.release_session_reservation(
                    session_id=str(reservation["session_id"]),
                    remote_quiescence_confirmed=True,
                    reason="idle session reservation released after daemon restart",
                )

    async def cancel_orphaned_queued_session_reservations(self) -> None:
        """Cancel abandoned reservation queue entries left by a prior daemon.

        A queued reservation has no reservation row or lease yet, so terminal
        execution cleanup cannot find it through ``selected_leases``.  It also
        cannot be reused by recovered work: the resumed execution acquires a
        fresh reservation request.  Cancel it before execution recovery so it
        cannot become an unowned FIFO head-of-line blocker.
        """
        prefix = "session-reservation:"
        for request in await self.db.list_resource_requests():
            execution_id = str(request["execution_id"])
            if not execution_id.startswith(prefix):
                continue
            session_id = execution_id.removeprefix(prefix)
            if not session_id or session_id != str(request["session_id"]):
                continue
            await self.db.cancel_resource_requests(execution_id)

    async def acquire(self, *, execution_id: str, session_id: str, requests: Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
        self.recovery_gate.require_ready()
        if not requests:
            return []
        normalized = []
        seen_slots = set()
        for request in requests:
            pool = str(request.get("pool") or "")
            slot = str(request.get("slot") or "resource")
            host = request.get("host")
            if not pool: raise ResourceInventoryError("resource request must select a pool")
            if host is not None and (not isinstance(host, str) or host not in self.inventory.members(pool)):
                raise ResourceInventoryError("resource request host is not a member of its pool")
            if slot in seen_slots: raise ResourceInventoryError("resource request slot is duplicated")
            seen_slots.add(slot); normalized.append((slot, pool, host))
        bundle_id = f"bundle-{uuid.uuid4().hex[:12]}"
        queued = [{"id": f"request-{uuid.uuid4().hex[:12]}", "slot": slot, "pool": pool} for slot, pool, _host in normalized]
        bundle = await self.db.enqueue_resource_bundle(
            bundle_id=bundle_id, execution_id=execution_id,
            session_id=session_id, requests=queued,
        )
        bundle_id = str(bundle["id"])
        queued = list(bundle["requests"])
        try:
            while True:
                execution = await self.db.get_execution(execution_id)
                if execution is not None and execution.get("status") == "cancelling":
                    await self.db.cancel_resource_requests(execution_id)
                    raise ResourceInventoryError("resource request was cancelled")
                requested = {slot: host for slot, _pool, host in normalized}
                acquired = await self.db.try_acquire_resource_bundle(bundle_id=bundle_id, candidates={row['id']: ([requested[row['slot']]] if requested[row['slot']] is not None else self.inventory.members(row['pool'])) for row in queued}, ttl_seconds=self.ttl_seconds)
                if acquired is not None:
                    return [
                        {**lease, "slot": request["slot"]}
                        for lease, request in zip(acquired, queued, strict=True)
                    ]
                await asyncio.sleep(self.poll_seconds)
        except asyncio.CancelledError:
            execution = await self.db.get_execution(execution_id)
            if execution is not None and execution.get("status") == "cancelling":
                await self.db.cancel_resource_requests(execution_id)
            else:
                await self.db.requeue_resource_requests(execution_id)
            raise
        except Exception:
            await self.db.cancel_resource_requests(execution_id)
            raise

    @staticmethod
    def _opaque_handle(handle: Mapping[str, Any]) -> Mapping[str, str]:
        """Keep transport identity and lease/fence lineage inside the server."""
        return {"id": str(handle["id"])}

    def _normalize_handle_spec(self, spec: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> list[tuple[str, str | None]]:
        requests = spec.get("requests") if isinstance(spec, Mapping) else spec
        if not isinstance(requests, Sequence) or isinstance(requests, (str, bytes)) or not requests:
            raise ResourceInventoryError("handle request spec must contain requests")
        normalized: list[tuple[str, str | None]] = []
        for request in requests:
            if not isinstance(request, Mapping):
                raise ResourceInventoryError("handle request must be an object")
            pool = request.get("pool")
            host = request.get("host")
            if not isinstance(pool, str) or not pool:
                raise ResourceInventoryError("handle request must select a pool")
            if host is not None and (not isinstance(host, str) or host not in self.inventory.members(pool)):
                raise ResourceInventoryError("handle request host is not a member of its pool")
            normalized.append((pool, host))
        return normalized

    async def _active_session_handles(self, session_id: str) -> list[Mapping[str, Any]]:
        handles = await self.db.list_session_resource_handles(session_id, states=("active",))
        result = []
        for handle in handles:
            lease = await self.db.get_resource_lease(str(handle["lease_id"]))
            if lease is not None and lease["state"] == "active":
                result.append({**handle, "_lease": lease})
        return result

    def set_wait_continuation_publisher(self, publisher: Any) -> None:
        """Install ExecutionService's in-process nudge for durable outbox rows."""
        self._wait_continuation_publisher = publisher

    async def start_or_reattach_wait(
        self, *, session_id: str, operation_id: str,
        spec: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        """Create one durable retained-handle wait, or reattach its poller."""
        self.recovery_gate.require_ready()
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            normalized = self._normalize_handle_spec(spec)
            existing = await self.db.find_pending_resource_wait_operation(operation_id)
            if existing is not None:
                if str(existing["session_id"]) != session_id:
                    raise ResourceInventoryError("resource wait operation belongs to a different session")
                self._reattach_wait(existing)
                return existing
            operation = await self.db.get_execution(operation_id)
            if await self.db.get_session(session_id) is None or operation is None:
                raise ResourceInventoryError("unknown session or operation for resource wait")
            if str(operation["session_id"]) != session_id:
                raise ResourceInventoryError("resource wait operation belongs to a different session")
            requests = [{"pool": pool, "host": host} for pool, host in normalized]
            wait = await self.db.create_resource_wait_operation({
                "id": f"wait-{uuid.uuid4().hex}", "session_id": session_id,
                "operation_id": operation_id,
                "request_kind": "bundle" if len(requests) > 1 else ("host" if requests[0]["host"] else "pool"),
                "requested_hosts": requests, "pool": requests[0]["pool"], "queue_ticket": 0,
            })
            self._reattach_wait(wait)
            return wait

    def _reattach_wait(self, wait: Mapping[str, Any]) -> None:
        wait_id = str(wait["id"])
        if wait_id not in self._wait_tasks:
            task = asyncio.create_task(self._run_wait(wait_id))
            self._wait_tasks[wait_id] = task
            task.add_done_callback(lambda _task: self._wait_tasks.pop(wait_id, None))

    async def _run_wait(self, wait_id: str) -> None:
        wait = await self.db.get_resource_wait_operation(wait_id)
        if wait is None or wait["state"] != "pending":
            return
        requests = wait.get("requested_hosts") or []
        if not requests:
            return
        requests = [
            item if isinstance(item, Mapping) else {
                "pool": wait["pool"], "host": item if wait["request_kind"] == "host" else None,
            }
            for item in requests
        ]
        try:
            leases = await self.acquire(
                execution_id=str(wait["operation_id"]), session_id=str(wait["session_id"]),
                requests=[{"slot": f"wait-{index}", "pool": item["pool"], "host": item.get("host")}
                          for index, item in enumerate(requests)],
            )
        except asyncio.CancelledError:
            # ``acquire`` restores queued rows.  It must not terminalize this
            # durable Operation merely because this process is going away.
            raise
        except Exception:
            return
        handles = [
            {"id": f"handle-{uuid.uuid4().hex}", "session_id": wait["session_id"],
             "pool": item["pool"], "host_id": lease["host_id"], "lease_id": lease["id"],
             "fencing_token": lease["fencing_token"]}
            for item, lease in zip(requests, leases, strict=True)
        ]
        if not await self.complete_wait_grant(wait_id, handles):
            await self.release(execution_id=str(wait["operation_id"]), leases=leases)

    async def complete_wait_grant(self, wait_id: str, handles: Sequence[Mapping[str, Any]]) -> bool:
        """Persist granted handles before atomically waking the owning session."""
        won = await self.db.terminalize_resource_wait_operation(
            wait_id=wait_id, outcome="LEASE_GRANTED", handles=list(handles),
        )
        if won and self._wait_continuation_publisher is not None:
            wait = await self.db.get_resource_wait_operation(wait_id)
            if wait is not None and wait["outcome"] == "LEASE_GRANTED":
                self._wait_continuation_publisher(str(wait["operation_id"]))
        return won

    async def cancel_wait(self, wait_id: str) -> bool:
        """Cancel queue edges and terminalize REQUEST_CANCELLED once."""
        task = self._wait_tasks.get(wait_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        wait = await self.db.get_resource_wait_operation(wait_id)
        if wait is None:
            return False
        won = await self.db.terminalize_resource_wait_operation(
            wait_id=wait_id, outcome="REQUEST_CANCELLED",
        )
        await self.db.cancel_resource_requests(str(wait["operation_id"]))
        if won and self._wait_continuation_publisher is not None:
            self._wait_continuation_publisher(str(wait["operation_id"]))
        return won

    async def terminalize_exact_host_waits(self, host_id: str) -> list[str]:
        """Apply authoritative permanent loss only to explicitly pinned waits."""
        settled: list[str] = []
        for wait in await self.db.list_resource_wait_operations(state="pending"):
            requested = wait.get("requested_hosts") or []
            if not any(item.get("host") == host_id for item in requested if isinstance(item, Mapping)):
                continue
            task = self._wait_tasks.get(str(wait["id"]))
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if await self.db.terminalize_resource_wait_operation(
                wait_id=str(wait["id"]), outcome="HOST_PERMANENTLY_UNAVAILABLE",
            ):
                await self.db.cancel_resource_requests(str(wait["operation_id"]))
                settled.append(str(wait["id"]))
                if self._wait_continuation_publisher is not None:
                    self._wait_continuation_publisher(str(wait["operation_id"]))
        return settled

    async def mark_wait_deadlock_replan_required(self, wait_id: str) -> bool:
        """R7's policy hook; R6 deliberately performs no deadlock analysis."""
        task = self._wait_tasks.get(wait_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        wait = await self.db.get_resource_wait_operation(wait_id)
        if wait is None:
            return False
        won = await self.db.terminalize_resource_wait_operation(
            wait_id=wait_id, outcome="DEADLOCK_REPLAN_REQUIRED",
        )
        if won:
            await self.db.cancel_resource_requests(str(wait["operation_id"]))
            if self._wait_continuation_publisher is not None:
                self._wait_continuation_publisher(str(wait["operation_id"]))
        return won

    async def acquire_handles(self, session_id: str, spec: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, str]]:
        """Retain an all-or-none handle bundle, acquiring only its missing slots.

        Existing active handles satisfy matching pool/host requests first.  A
        post-recovery partial bundle therefore retains its healthy members and
        allocates one atomic bundle for only the unavailable remainder.
        """
        self.recovery_gate.require_ready()
        if await self.db.get_session(session_id) is None:
            raise ResourceInventoryError("unknown session for resource handles")
        normalized = self._normalize_handle_spec(spec)
        retained = await self._active_session_handles(session_id)
        available = list(retained)
        selected: list[Mapping[str, Any] | None] = [None] * len(normalized)
        missing_by_index: dict[int, tuple[str, str | None]] = {}
        # Reserve exact-host slots first: an any-host slot may overlap their
        # pool, but consuming that candidate would make a later exact slot
        # spuriously wait for a lease we already own.
        order = [index for index, (_pool, host) in enumerate(normalized) if host is not None]
        order += [index for index, (_pool, host) in enumerate(normalized) if host is None]
        for index in order:
            pool, host = normalized[index]
            match = next((handle for handle in available if handle["pool"] == pool and (host is None or handle["host_id"] == host)), None)
            if match is None:
                missing_by_index[index] = (pool, host)
            else:
                available.remove(match)
                selected[index] = match
        missing = [missing_by_index[index] for index in sorted(missing_by_index)]
        if not missing:
            return [self._opaque_handle(handle) for handle in selected if handle is not None]

        execution_id = f"session-handles:{session_id}:{uuid.uuid4().hex}"
        intent = await self.db.create_resource_recovery_intent({
            "id": f"intent-{uuid.uuid4().hex}", "kind": "acquire",
            "session_id": session_id,
            "payload": {"operation": "session_handle_bundle",
                        "allocator_execution_id": execution_id},
        })
        leases: Sequence[Mapping[str, Any]] = []
        try:
            leases = await self.acquire(
                execution_id=execution_id, session_id=session_id,
                requests=[{"slot": f"handle-{index}", "pool": pool, "host": host}
                          for index, (pool, host) in enumerate(missing)],
            )
            handles = [
                {"id": f"handle-{uuid.uuid4().hex}", "session_id": session_id,
                 "pool": pool, "host_id": lease["host_id"], "lease_id": lease["id"],
                 "fencing_token": lease["fencing_token"]}
                for (pool, _host), lease in zip(missing, leases, strict=True)
            ]
            await self.db.create_session_resource_handles(handles)
        except Exception:
            await self.db.update_resource_recovery_intent(
                str(intent["id"]), expected_state="prepared", state="processing",
            )
            if leases:
                await self.release(execution_id=execution_id, leases=leases)
            await self.db.update_resource_recovery_intent(
                str(intent["id"]), expected_state="processing", state="failed",
            )
            raise
        await self.db.update_resource_recovery_intent(
            str(intent["id"]), expected_state="prepared", state="completed",
        )
        selected_by_index = iter(handles)
        ordered = [next(selected_by_index) if handle is None else handle for handle in selected]
        return [self._opaque_handle(handle) for handle in ordered]

    async def list_session_handles(self, session_id: str) -> Sequence[Mapping[str, str]]:
        """List only opaque active handle identifiers owned by a session."""
        return [self._opaque_handle(handle) for handle in await self._active_session_handles(session_id)]

    async def resolve_handle(self, session_id: str, handle_id: str) -> Mapping[str, str]:
        """Authorize a handle without revealing lease or transport details."""
        handle = await self._resolve_handle_lease(session_id, handle_id)
        return self._opaque_handle(handle)

    async def _session_resource_live(self, session_id: str, *, agent_turn_active: bool = False) -> bool:
        """One predicate for retained-handle ownership.

        The in-memory turn marker is intentionally supplied by SessionManager;
        all other liveness is durable so restart/reconciliation cannot discard
        a handle needed by an Operation or its continuation.
        """
        return agent_turn_active or await self.db.session_resource_is_live(session_id)

    async def _reconcile_handle_quiescence(self, handle: Mapping[str, Any]) -> bool:
        """Establish the fenced durable proof required before releasing a handle.

        Operation completion removes its reference only after the execution
        supervisor has reconciled the remote process.  The matching active
        lease/fence is therefore the remaining local proof; a stale, absent,
        or changed lease is deliberately treated as ambiguous.
        """
        lease = await self.db.get_resource_lease(str(handle["lease_id"]))
        return bool(lease and lease["state"] == "active"
                    and int(lease["fencing_token"]) == int(handle["fencing_token"]))

    async def release_handle(self, session_id: str, handle_id: str) -> bool:
        """Release one idle handle, or quarantine it when quiescence is unknown."""
        self.recovery_gate.require_ready()
        handle = await self.db.get_session_resource_handle(handle_id)
        if handle is None or handle["session_id"] != session_id:
            raise ResourceHandleOwnershipError()
        if handle["state"] in {"released", "quarantined"}:
            return False
        if handle["state"] != "active":
            return False
        intent = {
            "id": f"intent-{uuid.uuid4().hex}", "kind": "release",
            "session_id": session_id, "handle_id": handle_id,
            "payload": {"lease_id": handle["lease_id"]},
        }
        intent_id, operation_ids = await self.db.begin_release_handle(
            session_id, handle_id, intent,
        )
        if operation_ids:
            raise ResourceHandleConflictError(operation_ids)
        if intent_id is None:
            return False
        lease = await self.db.get_resource_lease(str(handle["lease_id"]))
        if not await self._reconcile_handle_quiescence(handle):
            if lease is not None and lease["state"] in {"active", "revoking"}:
                await self.quarantine(execution_id=str(lease["execution_id"]), leases=[lease],
                                      reason="handle release could not prove remote quiescence")
            else:
                await self.db.set_resource_host_state(
                    str(handle["host_id"]), quarantined=True,
                    reason="handle release could not prove remote quiescence",
                )
            await self.db.update_session_resource_handle(
                handle_id, expected_state="releasing", state="quarantined",
                release_reason="handle release could not prove remote quiescence",
            )
            await self.db.update_resource_recovery_intent(
                intent_id, expected_state="processing", state="failed",
            )
            return True
        assert lease is not None
        await self.release(execution_id=str(lease["execution_id"]), leases=[lease])
        await self.db.update_session_resource_handle(
            handle_id, expected_state="releasing", state="released",
        )
        await self.db.update_resource_recovery_intent(
            intent_id, expected_state="processing", state="completed",
        )
        return True

    async def release_all_session_handles(
        self, session_id: str, *, agent_turn_active: bool = False,
    ) -> list[str]:
        """Release every handle only after the session is resource-idle.

        This is intentionally a reconciliation action rather than a turn-end
        action.  It is safe to call repeatedly from both stop and archive:
        terminal handles are ignored, while a newly-live session simply keeps
        every remaining active handle.  R6 will own waiter outcomes; this
        method only observes their durable pending state.
        """
        self.recovery_gate.require_ready()
        if await self._session_resource_live(session_id, agent_turn_active=agent_turn_active):
            return []
        released: list[str] = []
        for handle in await self.db.list_session_resource_handles(session_id, states=("active",)):
            try:
                if await self.release_handle(session_id, str(handle["id"])):
                    released.append(str(handle["id"]))
            except ResourceHandleConflictError:
                # A concurrent operation attachment wins; its handle remains
                # retained and the next definitive lifecycle boundary retries.
                continue
        return released

    async def _resolve_handle_lease(self, session_id: str, handle_id: str) -> Mapping[str, Any]:
        """Trusted execution-only resolution, including the current lease fence."""
        handle = await self.db.get_session_resource_handle(handle_id)
        if handle is None or handle["session_id"] != session_id or handle["state"] != "active":
            raise ResourceHandleOwnershipError()
        lease = await self.db.get_resource_lease(str(handle["lease_id"]))
        if lease is None or lease["state"] != "active":
            raise ResourceHandleOwnershipError()
        return {**handle, "lease": lease}

    async def heartbeat(self, *, execution_id: str, lease: Mapping[str, Any]) -> bool:
        # Fencing predicates make stale messages harmless.
        return await self.db.heartbeat_resource_lease(
            lease_id=str(lease.get("id")), execution_id=execution_id,
            fencing_token=int(lease.get("fencing_token", -1)),
            ttl_seconds=self.ttl_seconds,
        )

    async def release(self, *, execution_id: str, leases: Sequence[Mapping[str, Any]]) -> None:
        self.recovery_gate.require_ready()
        for lease in leases:
            await self.db.release_resource_lease(lease_id=str(lease.get("id")), execution_id=execution_id, fencing_token=int(lease.get("fencing_token", -1)))
        await self.db.settle_resource_bundles(execution_id)
        await self.db.cancel_resource_requests(execution_id)

    async def quarantine(self, *, execution_id: str, leases: Sequence[Mapping[str, Any]], reason: str) -> None:
        for lease in leases:
            await self.db.quarantine_resource_lease(lease_id=str(lease.get("id")), execution_id=execution_id, fencing_token=int(lease.get("fencing_token", -1)), reason=reason)
        await self.db.settle_resource_bundles(execution_id)

    async def resource_snapshot(self) -> Mapping[str, Any]:
        await self.reconcile_expired()
        hosts=await self.db.list_resource_hosts(); leases=await self.db.list_resource_leases()
        queue=await self.db.list_resource_requests()
        active={str(x["host_id"]):x for x in leases if x["state"] in {"active","revoking","quarantined"}}
        for h in hosts:
            h["state"]="quarantined" if h["quarantined"] else "draining" if h["draining"] else "offline" if h["offline"] or not h["enabled"] else "leased" if h["id"] in active else "healthy"
            h["pools"]=[p for p in self.inventory.pools if h["id"] in self.inventory.members(p)]
            if h["id"] in active: h["current_lease"]=active[h["id"]]
        pools=[]
        for ident,p in self.inventory.pools.items():
            members=self.inventory.members(ident); available=sum(1 for h in hosts if h["id"] in members and h["state"]=="healthy")
            pools.append({"id":ident,"title":p.get("title",ident),"member_ids":members,"total_hosts":len(members),"available_hosts":available,"enabled":True,"queue_depth":sum(1 for request in queue if request["pool"] == ident)})
        reservations = await self.db.list_active_session_resource_reservations()
        return {"hosts":hosts,"pools":pools,"leases":leases,"queue":queue,
                "session_reservations": reservations}

    async def diagnostics(self, *, limit: int = 100) -> Mapping[str, Any]:
        return {"snapshot": await self.resource_snapshot(),
                "events": await self.db.list_resource_events(limit)}

    async def set_host_draining(self, *, host_id: str, draining: bool, requested_by: str) -> Mapping[str, Any]: return await self.db.set_resource_host_state(host_id, draining=draining)
    async def quarantine_host(self, *, host_id: str, reason: str,
                              requested_by: str) -> Mapping[str, Any]:
        if not reason.strip():
            raise ResourceInventoryError("quarantine reason is required")
        host = await self.db.get_resource_host(host_id)
        if host is None:
            raise KeyError(host_id)
        active = [lease for lease in await self.db.list_resource_leases()
                  if lease["host_id"] == host_id and lease["state"] in {"active", "revoking"}]
        for lease in active:
            await self.db.quarantine_resource_lease(
                lease_id=lease["id"], execution_id=lease["execution_id"],
                fencing_token=lease["fencing_token"], reason=reason,
            )
        if not active:
            await self.db.set_resource_host_state(
                host_id, quarantined=True, reason=reason,
            )
        current = await self.db.get_resource_host(host_id)
        assert current is not None
        return current
    async def recover_host(self, *, host_id: str, requested_by: str, remote_quiescence_confirmed: bool) -> Mapping[str, Any]:
        if not remote_quiescence_confirmed: raise ResourceInventoryError("remote quiescence confirmation is required")
        return await self.db.recover_resource_host(host_id)
