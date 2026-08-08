"""End-to-end Langfuse canary for a freshly started Codex app-server."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from nerve.agent.backends import BackendDeps, SessionSpec
from nerve.agent.backends.base import TurnInput

from .backend import CodexBackend
from .langfuse_plugin import ensure_installed, installation_status


class _CanaryBackend(CodexBackend):
    """Isolate the canary from daemon-only persisted MCP configuration."""

    def build_config_overrides(self, spec: SessionSpec) -> list[str]:
        return [
            value for value in super().build_config_overrides(spec)
            if not value.startswith("mcp_servers.")
        ]


def _validate_usage(observations: list[Any]) -> list[str]:
    errors: list[str] = []
    if not observations:
        return ["Langfuse did not return a canary generation"]
    for observation in observations:
        usage = observation.usage_details or {}
        cost = observation.cost_details or {}
        if "cache_read_input_tokens" in usage or "reasoning_tokens" in usage:
            errors.append(f"{observation.id}: legacy usage keys are present")
            continue
        required = {
            "input", "output", "input_cached_tokens",
            "output_reasoning_tokens", "total",
        }
        missing = sorted(required - usage.keys())
        if missing:
            errors.append(f"{observation.id}: missing usage keys {missing}")
            continue
        bucket_total = sum(float(usage[key]) for key in required - {"total"})
        if bucket_total != float(usage["total"]):
            errors.append(
                f"{observation.id}: exclusive usage buckets do not sum to total"
            )
        for key in ("input_cached_tokens", "output_reasoning_tokens"):
            if float(usage[key]) > 0 and float(cost.get(key, 0)) <= 0:
                errors.append(f"{observation.id}: non-zero {key} has zero cost")
    return errors


async def run_langfuse_canary(config: Any, *, timeout: float = 60.0) -> dict[str, Any]:
    """Start a real Codex turn and verify its immutable Langfuse observation."""
    before = await ensure_installed(config)
    if not before.get("ready"):
        return {"ok": False, "phase": "preflight", "errors": [
            "managed Langfuse plugin is not ready before app-server startup",
        ]}

    deps = BackendDeps(
        config=lambda: config,
        db=None,
        registry=None,
        tool_ctx_factory=lambda _session_id: None,
        external_mcp_servers=lambda: [],
    )
    backend = _CanaryBackend(deps)
    canary_id = f"langfuse-canary-{uuid.uuid4().hex}"
    workspace = Path(config.workspace).expanduser()
    workspace.mkdir(parents=True, exist_ok=True)
    spec = SessionSpec(
        session_id=canary_id,
        source="hook",
        model=config.codex.model,
        effort="low",
        system_prompt=(
            "This is an observability canary. Reply with exactly OK and do not "
            "use tools."
        ),
        cwd=str(workspace),
        interactive=None,
        idle_timeout=timeout,
    )
    started = datetime.now(timezone.utc) - timedelta(seconds=5)
    client = await backend.create_client(spec)
    native_id = client.native_session_id
    try:
        after_start = installation_status(config)
        if not after_start.get("ready"):
            return {"ok": False, "phase": "appserver_start", "errors": [
                "app-server startup invalidated the managed Langfuse plugin",
            ]}
        await client.start_turn(TurnInput(text="Reply with exactly OK."))
        async for _event in client.receive_turn():
            pass
    finally:
        await client.disconnect()

    if not native_id:
        return {"ok": False, "phase": "generation", "errors": [
            "Codex app-server returned no native thread id",
        ]}

    from langfuse import Langfuse

    langfuse = Langfuse(
        public_key=config.langfuse.public_key,
        secret_key=config.langfuse.secret_key,
        base_url=config.langfuse.effective_base_url,
    )
    deadline = time.monotonic() + timeout
    generations: list[Any] = []
    while time.monotonic() < deadline:
        response = await asyncio.to_thread(
            langfuse.api.observations.get_many,
            fields="core,basic,usage",
            limit=100,
            type="GENERATION",
            session_id=native_id,
            from_start_time=started,
        )
        generations = list(response.data)
        if generations:
            break
        await asyncio.sleep(1.0)

    errors = _validate_usage(generations)
    return {
        "ok": not errors,
        "phase": "langfuse_ingestion",
        "native_session_id": native_id,
        "generation_count": len(generations),
        "trace_ids": sorted({item.trace_id for item in generations}),
        "errors": errors,
    }
