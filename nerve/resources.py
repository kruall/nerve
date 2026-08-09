"""Trusted static inventory and global physical-host leases.

Connection references are names only.  Endpoint fields are intentionally not
accepted here: a future SSH transport resolves the named trusted connection.
"""
from __future__ import annotations
import asyncio
import contextlib
from pathlib import Path
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from nerve.utils.time import utc_now_iso


class ResourceInventoryError(ValueError): pass


class ResourceInventory:
    def __init__(self, db: Any, config: Mapping[str, Any] | None = None):
        self.db = db
        raw = dict(config or {})
        self.config = raw
        self.connections = set(raw.get("connections", []))
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
    def __init__(self, *, db: Any, inventory: ResourceInventory):
        self.db, self.inventory = db, inventory
        raw = getattr(inventory, "config", {})
        self.ttl_seconds = max(5, int(raw.get("lease_ttl_seconds", 90)))
        self.poll_seconds = max(0.01, float(raw.get("queue_poll_seconds", 0.25)))
        self._reconciler: asyncio.Task[Any] | None = None
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
                raise ResourceInventoryError("session reservation is no longer usable")
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
        reservation = await self.db.get_session_resource_reservation(session_id)
        if reservation is None or reservation["state"] != "active":
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
        await self.reconcile_expired()
        if self._reconciler is None:
            self._reconciler = asyncio.create_task(self._reconcile_loop())

    async def shutdown(self) -> None:
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

    async def acquire(self, *, execution_id: str, session_id: str, requests: Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
        leases=[]
        # Profiles currently have one remote slot in normal use. Multiple slots
        # are acquired deterministically and rolled back on partial failure.
        try:
            for request in requests:
                pool=str(request.get("pool") or "")
                if not pool: raise ResourceInventoryError("resource request must select a pool")
                slot = str(request.get("slot") or "resource")
                queued = await self.db.enqueue_resource_request(
                    request_id=f"request-{uuid.uuid4().hex[:12]}", execution_id=execution_id,
                    session_id=session_id, slot=slot, pool=pool,
                )
                acquired=None
                while acquired is None:
                    execution = await self.db.get_execution(execution_id)
                    if execution is not None and execution.get("status") == "cancelling":
                        await self.db.cancel_resource_requests(execution_id)
                        raise ResourceInventoryError("resource request was cancelled")
                    acquired = await self.db.try_acquire_resource_request(
                        request_id=str(queued["id"]),
                        lease_id=f"lease-{uuid.uuid4().hex[:12]}",
                        host_ids=self.inventory.members(pool),
                        ttl_seconds=self.ttl_seconds,
                    )
                    if acquired is None:
                        await asyncio.sleep(self.poll_seconds)
                leases.append(acquired)
            return leases
        except asyncio.CancelledError:
            execution = await self.db.get_execution(execution_id)
            for lease in leases:
                await self.db.release_resource_lease(
                    lease_id=str(lease.get("id")), execution_id=execution_id,
                    fencing_token=int(lease.get("fencing_token", -1)),
                )
            if execution is not None and execution.get("status") == "cancelling":
                await self.db.cancel_resource_requests(execution_id)
            else:
                await self.db.requeue_resource_requests(execution_id)
            raise
        except Exception:
            await self.release(execution_id=execution_id, leases=leases)
            raise

    async def heartbeat(self, *, execution_id: str, lease: Mapping[str, Any]) -> bool:
        # Fencing predicates make stale messages harmless.
        return await self.db.heartbeat_resource_lease(
            lease_id=str(lease.get("id")), execution_id=execution_id,
            fencing_token=int(lease.get("fencing_token", -1)),
            ttl_seconds=self.ttl_seconds,
        )

    async def release(self, *, execution_id: str, leases: Sequence[Mapping[str, Any]]) -> None:
        for lease in leases:
            await self.db.release_resource_lease(lease_id=str(lease.get("id")), execution_id=execution_id, fencing_token=int(lease.get("fencing_token", -1)))
        await self.db.cancel_resource_requests(execution_id)

    async def quarantine(self, *, execution_id: str, leases: Sequence[Mapping[str, Any]], reason: str) -> None:
        for lease in leases:
            await self.db.quarantine_resource_lease(lease_id=str(lease.get("id")), execution_id=execution_id, fencing_token=int(lease.get("fencing_token", -1)), reason=reason)

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
