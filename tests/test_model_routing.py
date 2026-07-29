"""Codex model-tier configuration and session routing."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers.model_routing import (
    change_model_tier_handler,
    complete_model_routing_audit_handler,
    model_routing_audit_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig


def _config(tmp_path, **codex_overrides) -> NerveConfig:
    codex = {
        "home_dir": str(tmp_path / "codex-home"),
        "default_tier": "luna-high",
        **codex_overrides,
    }
    return NerveConfig.from_dict({
        "workspace": str(tmp_path / "workspace"),
        "agent": {"backend": "codex"},
        "codex": codex,
    })


def test_default_ladder_and_adjacency(tmp_path):
    cfg = _config(tmp_path)
    assert cfg.codex.resolved_default_tier.id == "luna-high"
    assert cfg.codex.resolved_default_tier.model == "gpt-5.6-luna"
    assert cfg.codex.adjacent_tier("luna-high", "up").id == "terra-high"
    assert cfg.codex.adjacent_tier("terra-high", "down").id == "luna-high"
    assert cfg.codex.adjacent_tier("sol-xhigh", "up") is None


def test_explicit_empty_default_tier_preserves_legacy_model(tmp_path):
    cfg = _config(tmp_path, default_tier="")
    assert cfg.codex.resolved_default_tier is None
    assert cfg.codex.model == "gpt-5.6-sol"


def test_invalid_default_tier_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="codex.default_tier"):
        _config(tmp_path, default_tier="missing")


def test_duplicate_model_effort_profile_is_rejected(tmp_path):
    tiers = [
        {"id": "one", "model": "gpt-5.6-luna", "effort": "high"},
        {"id": "two", "model": "gpt-5.6-luna", "effort": "high"},
    ]
    with pytest.raises(ValueError, match="model/effort pairs"):
        _config(tmp_path, default_tier="one", model_tiers=tiers)


@pytest.mark.asyncio
async def test_new_codex_session_stamps_default_profile(tmp_path, db):
    engine = AgentEngine(_config(tmp_path), db)
    session = await engine.sessions.get_or_create("s1", source="discord")
    assert session["model"] == "gpt-5.6-luna"
    assert session["model_tier"] == "luna-high"
    assert session["reasoning_effort"] == "high"
    assert session["model_pinned"] == 0


@pytest.mark.asyncio
async def test_named_tier_repairs_fallback_model_and_effort_change_rebuilds(
    tmp_path, db,
):
    from nerve.agent.backends.base import BackendCapabilities

    engine = AgentEngine(_config(tmp_path), db)
    await db.create_session(
        "s1",
        source="web",
        backend="codex",
        model="temporary-serving-fallback",
        model_tier="sol-medium",
        reasoning_effort="medium",
    )
    specs = []

    class StubClient:
        native_session_id = None
        resume_dropped = False

        def __init__(self, model):
            self.model = model
            self.disconnected = False

        def is_alive(self):
            return True

        async def disconnect(self):
            self.disconnected = True

    class StubBackend:
        name = "codex"
        capabilities = BackendCapabilities(
            cost_is_cumulative=False,
            supports_idle_stream=False,
            supports_cache_ttl=False,
            interactive_builtins=False,
            reports_context_window=True,
        )

        def default_model(self, source):
            return "gpt-5.6-luna"

        def excluded_tools(self):
            return set()

        def validate_resume_target(self, native_id, cwd):
            return True

        async def create_client(self, spec):
            specs.append(spec)
            return StubClient(spec.model)

    engine._backends["codex"] = StubBackend()
    first = await engine._get_or_create_client("s1", "web", None)
    assert specs[0].model == "gpt-5.6-sol"
    assert specs[0].effort == "medium"
    row = await db.get_session("s1")
    assert row["model"] == "gpt-5.6-sol"

    await db.update_session_fields("s1", {
        "model_tier": "sol-xhigh",
        "reasoning_effort": "xhigh",
    })
    second = await engine._get_or_create_client("s1", "web", None)
    assert second is not first
    assert first.disconnected is True
    assert len(specs) == 2
    assert specs[1].model == "gpt-5.6-sol"
    assert specs[1].effort == "xhigh"


@pytest.mark.asyncio
async def test_upgrade_persists_history_and_queues_continuation(tmp_path, db):
    engine = AgentEngine(_config(tmp_path), db)
    await engine.sessions.get_or_create("s1", source="discord")

    with patch("nerve.agent.engine.broadcaster") as broadcaster:
        broadcaster.broadcast = AsyncMock()
        changed = await engine.change_model_tier(
            "s1",
            direction="up",
            reason="The task needs repository-wide debugging.",
        )

    assert changed == {
        "from_tier": "luna-high",
        "to_tier": "terra-high",
        "model": "gpt-5.6-terra",
        "effort": "high",
        "automatic_continuation": True,
    }
    row = await db.get_session("s1")
    assert row["model"] == "gpt-5.6-terra"
    assert row["model_tier"] == "terra-high"
    assert row["reasoning_effort"] == "high"
    metadata = json.loads(row["metadata"])
    assert metadata["initial_model_tier"] == "luna-high"
    assert metadata["model_tier_history"][-1]["to"] == "terra-high"
    assert "s1" in engine._pending_model_tier_continuations


@pytest.mark.asyncio
async def test_downgrade_has_no_automatic_continuation(tmp_path, db):
    engine = AgentEngine(_config(tmp_path), db)
    await db.create_session(
        "s1",
        source="web",
        backend="codex",
        model="gpt-5.6-sol",
        model_tier="sol-xhigh",
        reasoning_effort="xhigh",
    )
    with patch("nerve.agent.engine.broadcaster") as broadcaster:
        broadcaster.broadcast = AsyncMock()
        changed = await engine.change_model_tier(
            "s1", direction="down", reason="Only mechanical follow-up remains.",
        )
    assert changed["to_tier"] == "sol-medium"
    assert changed["automatic_continuation"] is False
    assert "s1" not in engine._pending_model_tier_continuations


@pytest.mark.asyncio
async def test_pinned_session_rejects_automatic_change(tmp_path, db):
    engine = AgentEngine(_config(tmp_path), db)
    await db.create_session(
        "s1",
        source="web",
        backend="codex",
        model="gpt-5.6-luna",
        model_tier="luna-high",
        reasoning_effort="high",
        model_pinned=True,
    )
    with pytest.raises(ValueError, match="pinned"):
        await engine.change_model_tier(
            "s1", direction="up", reason="Try a stronger tier.",
        )


@pytest.mark.asyncio
async def test_tool_handler_reports_upgrade(tmp_path, db):
    engine = AgentEngine(_config(tmp_path), db)
    await engine.sessions.get_or_create("s1", source="web")
    ctx = ToolContext(
        session_id="s1",
        db=db,
        engine=engine,
        config=engine.config,
    )
    with patch("nerve.agent.engine.broadcaster") as broadcaster:
        broadcaster.broadcast = AsyncMock()
        result = await change_model_tier_handler(ctx, {
            "direction": "up",
            "reason": "The problem needs stronger synthesis.",
        })
    assert result.is_error is False
    assert "luna-high → terra-high" in result.content[0]["text"]
    assert "Stop this turn now" in result.content[0]["text"]


@pytest.mark.asyncio
async def test_tool_handler_rejects_missing_reason(tmp_path, db):
    ctx = ToolContext(
        session_id="s1",
        db=db,
        engine=SimpleNamespace(),
    )
    result = await change_model_tier_handler(
        ctx, {"direction": "up", "reason": " "},
    )
    assert result.is_error is True


@pytest.mark.asyncio
async def test_run_drains_upgrade_as_internal_continuation(tmp_path, db):
    engine = AgentEngine(_config(tmp_path), db)
    await engine.sessions.get_or_create("s1", source="web")
    calls: list[dict] = []

    async def fake_run_inner(
        session_id,
        user_message,
        source,
        channel,
        model,
        **kwargs,
    ):
        calls.append({
            "message": user_message,
            "model": model,
            "internal": kwargs.get("internal"),
        })
        if len(calls) == 1:
            engine._pending_model_tier_continuations[session_id] = (
                "Continue after upgrading."
            )
        return f"result-{len(calls)}"

    engine._run_inner = fake_run_inner
    with patch("nerve.agent.engine.broadcaster") as broadcaster:
        broadcaster.broadcast = AsyncMock()
        broadcaster.broadcast_done = AsyncMock()
        broadcaster.is_turn_open.return_value = False
        result = await engine.run("s1", "Do the task", source="web")

    assert result == "result-2"
    assert calls == [
        {"message": "Do the task", "model": None, "internal": False},
        {
            "message": "Continue after upgrading.",
            "model": None,
            "internal": True,
        },
    ]


@pytest.mark.asyncio
async def test_auditor_reads_batch_updates_policy_and_advances_cursor(
    tmp_path, db,
):
    policy_path = tmp_path / "model-routing-policy.md"
    engine = AgentEngine(
        _config(tmp_path, routing_policy_file=str(policy_path)),
        db,
    )
    auditor_id = "cron:model-routing-auditor:20260729-043000"
    await db.create_session(
        auditor_id, source="cron", backend="codex",
    )
    await db.create_session(
        "review-me",
        source="discord",
        backend="codex",
        model="gpt-5.6-luna",
        model_tier="luna-high",
        reasoning_effort="high",
    )
    await db.add_message("review-me", "user", "Investigate a test failure")
    await db.add_message("review-me", "assistant", "The failure is fixed.")
    ctx = ToolContext(
        session_id=auditor_id,
        db=db,
        engine=engine,
        config=engine.config,
    )

    preview = await model_routing_audit_handler(ctx, {})
    assert preview.is_error is False
    payload = json.loads(preview.content[0]["text"])
    assert [item["id"] for item in payload["sessions"]] == ["review-me"]
    assert payload["sessions"][0]["first_user_message_untrusted"].startswith(
        "Investigate"
    )

    through = payload["through"]
    complete = await complete_model_routing_audit_handler(ctx, {
        "decision": "update",
        "through_updated_at": through["updated_at"],
        "through_session_id": through["session_id"],
        "summary": "One high-risk example justified a narrow rule.",
        "evidence_session_ids": ["review-me"],
        "high_risk_failure": True,
        "policy": "Escalate before modifying authentication boundaries.",
    })
    assert complete.is_error is False
    assert policy_path.read_text().strip() == (
        "Escalate before modifying authentication boundaries."
    )

    _state, remaining = await db.get_model_routing_audit_batch()
    assert remaining == []
    await db.add_message(
        "review-me", "user", "A later turn should be audited too.",
    )
    _state, changed = await db.get_model_routing_audit_batch()
    assert [item["id"] for item in changed] == ["review-me"]


@pytest.mark.asyncio
async def test_auditor_cannot_skip_sessions_or_weakly_update_policy(
    tmp_path, db,
):
    engine = AgentEngine(_config(tmp_path), db)
    auditor_id = "cron:model-routing-auditor:20260729-043000"
    await db.create_session(auditor_id, source="cron", backend="codex")
    for session_id in ("first", "second"):
        await db.create_session(
            session_id,
            source="discord",
            backend="codex",
            model="gpt-5.6-luna",
            model_tier="luna-high",
            reasoning_effort="high",
        )
    ctx = ToolContext(
        session_id=auditor_id,
        db=db,
        engine=engine,
        config=engine.config,
    )
    _state, batch = await db.get_model_routing_audit_batch()

    skipped = await complete_model_routing_audit_handler(ctx, {
        "decision": "keep",
        "through_updated_at": batch[0]["updated_at"],
        "through_session_id": batch[0]["id"],
        "summary": "Skip ahead.",
    })
    assert skipped.is_error is True
    assert "cannot be skipped" in skipped.content[0]["text"]

    weak_update = await complete_model_routing_audit_handler(ctx, {
        "decision": "update",
        "through_updated_at": batch[-1]["updated_at"],
        "through_session_id": batch[-1]["id"],
        "summary": "One ordinary example.",
        "evidence_session_ids": ["first"],
        "policy": "Change the policy.",
    })
    assert weak_update.is_error is True
    assert "two independent" in weak_update.content[0]["text"]


@pytest.mark.asyncio
async def test_audit_tools_reject_non_auditor_session(tmp_path, db):
    engine = AgentEngine(_config(tmp_path), db)
    await db.create_session("ordinary", source="web", backend="codex")
    ctx = ToolContext(
        session_id="ordinary",
        db=db,
        engine=engine,
        config=engine.config,
    )
    result = await model_routing_audit_handler(ctx, {})
    assert result.is_error is True
    assert "restricted" in result.content[0]["text"]


@pytest.mark.asyncio
async def test_session_api_pins_tier_and_can_clear_pin(
    tmp_path, db, monkeypatch,
):
    from nerve.gateway.routes import sessions as routes

    engine = AgentEngine(_config(tmp_path), db)
    await engine.sessions.get_or_create("s1", source="web")
    monkeypatch.setattr(
        routes,
        "get_deps",
        lambda: SimpleNamespace(engine=engine, db=db),
    )

    updated = await routes.update_session(
        "s1", {"model_tier": "sol-medium"}, user={"sub": "user"},
    )
    assert updated["model"] == "gpt-5.6-sol"
    assert updated["reasoning_effort"] == "medium"
    assert updated["model_pinned"] == 1

    unpinned = await routes.update_session(
        "s1", {"model_pinned": False}, user={"sub": "user"},
    )
    assert unpinned["model_pinned"] == 0
