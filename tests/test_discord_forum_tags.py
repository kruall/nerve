"""Discord forum-tag tools and approval dispatcher."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from nerve.agent.tools.handlers.discord import (
    discord_forum_tag_action_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import DiscordConfig, NerveConfig
from nerve.discord_tags import (
    DISCORD_FORUM_TAG_METADATA_KEY,
    DiscordForumTagManager,
    dispatch_discord_forum_tag_action,
)


FORUM_ID = 100
THREAD_ID = 200
TAG_TODO = "300"
TAG_DONE = "301"


def _config() -> NerveConfig:
    config = NerveConfig()
    config.discord = DiscordConfig(
        enabled=True,
        bot_token="test-token",
        guild_id=1,
        task_forums={"NERVE": FORUM_ID},
        allowed_author_ids=[2],
    )
    return config


def _forum(tags: list[dict] | None = None) -> dict:
    return {
        "id": str(FORUM_ID),
        "guild_id": "1",
        "type": 15,
        "name": "nerve",
        "available_tags": (
            tags
            if tags is not None
            else [
                {
                    "id": TAG_TODO,
                    "name": "Todo",
                    "moderated": False,
                    "emoji_id": None,
                    "emoji_name": "📝",
                },
                {
                    "id": TAG_DONE,
                    "name": "Done",
                    "moderated": True,
                    "emoji_id": None,
                    "emoji_name": "✅",
                },
            ]
        ),
    }


def _thread(applied: list[str] | None = None) -> dict:
    return {
        "id": str(THREAD_ID),
        "guild_id": "1",
        "parent_id": str(FORUM_ID),
        "type": 11,
        "name": "Add tag support",
        "applied_tags": applied if applied is not None else [TAG_TODO],
    }


def _fake_api(monkeypatch, *, applied: list[str] | None = None):
    calls: list[tuple[str, int, dict | None, str]] = []

    def request(config, method, channel_id, payload=None, *, audit_reason=""):
        calls.append((method, channel_id, payload, audit_reason))
        if method == "GET" and channel_id == FORUM_ID:
            return _forum()
        if method == "GET" and channel_id == THREAD_ID:
            return _thread(applied)
        if method == "PATCH" and channel_id == FORUM_ID:
            tags = []
            next_id = 900
            for raw in payload["available_tags"]:
                tag = dict(raw)
                tag.setdefault("id", str(next_id))
                next_id += 1
                tags.append(tag)
            return _forum(tags)
        if method == "PATCH" and channel_id == THREAD_ID:
            return _thread(payload["applied_tags"])
        raise AssertionError((method, channel_id, payload))

    monkeypatch.setattr("nerve.discord_tags._discord_request", request)
    return calls


def _notification(action: dict, *, decision_target: str = "action-1") -> dict:
    action = {**action, "action_id": decision_target}
    return {
        "id": "approval-1",
        "metadata": json.dumps(
            {
                DISCORD_FORUM_TAG_METADATA_KEY: action,
            }
        ),
    }


def test_inspect_lists_available_and_applied_tags(monkeypatch):
    calls = _fake_api(monkeypatch)

    result = DiscordForumTagManager(_config()).inspect(thread_id=THREAD_ID)

    assert result["project"] == "NERVE"
    assert result["thread_id"] == str(THREAD_ID)
    assert result["applied_tag_ids"] == [TAG_TODO]
    assert [(tag["name"], tag["applied"]) for tag in result["available_tags"]] == [
        ("Todo", True),
        ("Done", False),
    ]
    assert [call[:2] for call in calls] == [
        ("GET", THREAD_ID),
        ("GET", FORUM_ID),
    ]


@pytest.mark.asyncio
async def test_mutation_tool_only_creates_approval(monkeypatch):
    calls = _fake_api(monkeypatch)
    service = AsyncMock()
    service.propose_action.return_value = {
        "notification_id": "approval-1",
        "status": "sent",
    }
    ctx = ToolContext(
        session_id="s1",
        config=_config(),
        notification_service=service,
    )

    result = await discord_forum_tag_action_handler(
        ctx,
        {
            "operation": "create_tag",
            "project": "NERVE",
            "name": "In review",
            "moderated": True,
        },
    )

    assert result.is_error is False
    assert "No Discord state was changed" in result.content[0]["text"]
    assert [call[0] for call in calls] == ["GET"]
    kwargs = service.propose_action.await_args.kwargs
    assert kwargs["target_kind"] == "discord-forum-tag"
    assert kwargs["options"] == [
        {"label": "Approve", "value": "approve"},
        {"label": "Decline", "value": "decline"},
    ]
    action = kwargs["metadata"][DISCORD_FORUM_TAG_METADATA_KEY]
    assert action["operation"] == "create_tag"
    assert action["tag"]["name"] == "In review"
    assert action["action_id"].startswith("discord-tag-")


def test_decline_never_contacts_discord(monkeypatch):
    calls = _fake_api(monkeypatch)
    action = {
        "version": 1,
        "operation": "delete_tag",
        "project": "NERVE",
        "forum_id": str(FORUM_ID),
        "tag_id": TAG_TODO,
    }

    result = dispatch_discord_forum_tag_action(
        _notification(action),
        "action-1",
        "decline",
        _config(),
    )

    assert result.ok is True
    assert result.audit_event["executed"] is False
    assert calls == []


def test_approved_create_refetches_then_patches(monkeypatch):
    calls = _fake_api(monkeypatch)
    action = {
        "version": 1,
        "operation": "create_tag",
        "project": "NERVE",
        "forum_id": str(FORUM_ID),
        "tag": {
            "name": "In review",
            "moderated": False,
            "emoji_id": None,
            "emoji_name": "🔎",
        },
    }

    result = dispatch_discord_forum_tag_action(
        _notification(action),
        "action-1",
        "approve",
        _config(),
    )

    assert result.ok is True
    assert result.audit_event["executed"] is True
    assert [call[:2] for call in calls] == [
        ("GET", FORUM_ID),
        ("PATCH", FORUM_ID),
    ]
    patch = calls[1]
    assert patch[2]["available_tags"][-1]["name"] == "In review"
    assert patch[2]["available_tags"][-1].get("id") is None
    assert patch[3] == "Nerve approval approval-1"


def test_approved_action_fails_closed_if_project_mapping_changed(monkeypatch):
    calls = _fake_api(monkeypatch)
    config = _config()
    config.discord.task_forums["NERVE"] = 999
    action = {
        "version": 1,
        "operation": "delete_tag",
        "project": "NERVE",
        "forum_id": str(FORUM_ID),
        "tag_id": TAG_TODO,
    }

    result = dispatch_discord_forum_tag_action(
        _notification(action),
        "action-1",
        "approve",
        config,
    )

    assert result.ok is False
    assert "Configured forum changed" in result.audit_event["error"]
    assert calls == []


def test_approved_thread_tag_add_preserves_existing_tags(monkeypatch):
    calls = _fake_api(monkeypatch, applied=[TAG_TODO])
    action = {
        "version": 1,
        "operation": "add_thread_tag",
        "project": "NERVE",
        "forum_id": str(FORUM_ID),
        "thread_id": str(THREAD_ID),
        "tag_id": TAG_DONE,
    }

    result = dispatch_discord_forum_tag_action(
        _notification(action),
        "action-1",
        "approve",
        _config(),
    )

    assert result.ok is True
    assert [call[:2] for call in calls] == [
        ("GET", FORUM_ID),
        ("GET", THREAD_ID),
        ("PATCH", THREAD_ID),
    ]
    assert calls[-1][2] == {"applied_tags": [TAG_TODO, TAG_DONE]}


def test_target_id_mismatch_fails_without_discord_call(monkeypatch):
    calls = _fake_api(monkeypatch)
    action = {
        "version": 1,
        "operation": "delete_tag",
        "project": "NERVE",
        "forum_id": str(FORUM_ID),
        "tag_id": TAG_TODO,
    }

    result = dispatch_discord_forum_tag_action(
        _notification(action, decision_target="different"),
        "action-1",
        "approve",
        _config(),
    )

    assert result.ok is False
    assert result.audit_event["error"] == "Discord action target_id mismatch"
    assert calls == []
