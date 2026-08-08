"""Trusted static inventory and global physical-host leases.

Connection references are names only.  Endpoint fields are intentionally not
accepted here: a future SSH transport resolves the named trusted connection.
"""
from __future__ import annotations
import asyncio
import uuid
from collections.abc import Mapping, Sequence
from typing import Any


class ResourceInventoryError(ValueError): pass


class ResourceInventory:
    def __init__(self, db: Any, config: Mapping[str, Any] | None = None):
        self.db = db
        raw = dict(config or {})
        self.connections = set(raw.get("connections", []))
        self.hosts = {str(h["id"]): dict(h) for h in raw.get("hosts", []) if isinstance(h, Mapping) and h.get("id")}
        self.pools = {str(p["id"]): dict(p) for p in raw.get("pools", []) if isinstance(p, Mapping) and p.get("id")}
        self._validate()

    def _validate(self) -> None:
        if len(self.hosts) != len([h for h in self.hosts]): raise ResourceInventoryError("duplicate host id")
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
        self._waiters: dict[str, asyncio.Condition] = {}

    async def acquire(self, *, execution_id: str, session_id: str, requests: Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
        leases=[]
        # Profiles currently have one remote slot in normal use. Multiple slots
        # are acquired deterministically and rolled back on partial failure.
        try:
            for request in requests:
                pool=str(request.get("pool") or "")
                if not pool: raise ResourceInventoryError("resource request must select a pool")
                acquired=None
                for host_id in self.inventory.members(pool):
                    acquired=await self.db.acquire_resource_lease(lease_id=f"lease-{uuid.uuid4().hex[:12]}", execution_id=execution_id, session_id=session_id, pool=pool, host_id=host_id)
                    if acquired: break
                if not acquired: raise ResourceInventoryError("no healthy host is currently available")
                leases.append(acquired)
            return leases
        except Exception:
            await self.release(execution_id=execution_id, leases=leases)
            raise

    async def heartbeat(self, *, execution_id: str, lease: Mapping[str, Any]) -> bool:
        # Fencing predicates make stale messages harmless.
        result=await self.db._write("UPDATE resource_leases SET heartbeat_at=datetime('now') WHERE id=? AND execution_id=? AND fencing_token=? AND state='active'", (lease.get("id"), execution_id, lease.get("fencing_token")))
        return bool(result.rowcount)

    async def release(self, *, execution_id: str, leases: Sequence[Mapping[str, Any]]) -> None:
        for lease in leases:
            await self.db.release_resource_lease(lease_id=str(lease.get("id")), execution_id=execution_id, fencing_token=int(lease.get("fencing_token", -1)))

    async def quarantine(self, *, execution_id: str, leases: Sequence[Mapping[str, Any]], reason: str) -> None:
        for lease in leases:
            await self.db.quarantine_resource_lease(lease_id=str(lease.get("id")), execution_id=execution_id, fencing_token=int(lease.get("fencing_token", -1)), reason=reason)

    async def resource_snapshot(self) -> Mapping[str, Any]:
        hosts=await self.db.list_resource_hosts(); leases=await self.db.list_resource_leases()
        active={str(x["host_id"]):x for x in leases if x["state"] in {"active","revoking","quarantined"}}
        for h in hosts:
            h["state"]="quarantined" if h["quarantined"] else "draining" if h["draining"] else "offline" if h["offline"] or not h["enabled"] else "leased" if h["id"] in active else "healthy"
            h["pools"]=[p for p in self.inventory.pools if h["id"] in self.inventory.members(p)]
            if h["id"] in active: h["current_lease"]=active[h["id"]]
        pools=[]
        for ident,p in self.inventory.pools.items():
            members=self.inventory.members(ident); available=sum(1 for h in hosts if h["id"] in members and h["state"]=="healthy")
            pools.append({"id":ident,"title":p.get("title",ident),"member_ids":members,"total_hosts":len(members),"available_hosts":available,"enabled":True,"queue_depth":0})
        return {"hosts":hosts,"pools":pools,"leases":leases,"queue":[]}

    async def set_host_draining(self, *, host_id: str, draining: bool, requested_by: str) -> Mapping[str, Any]: return await self.db.set_resource_host_state(host_id, draining=draining)
    async def recover_host(self, *, host_id: str, requested_by: str, remote_quiescence_confirmed: bool) -> Mapping[str, Any]:
        if not remote_quiescence_confirmed: raise ResourceInventoryError("remote quiescence confirmation is required")
        return await self.db.recover_resource_host(host_id)
