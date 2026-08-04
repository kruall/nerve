from unittest.mock import AsyncMock

import pytest

from nerve.agent.tools.handlers.memory import memory_update_handler
from nerve.agent.tools.registry import ToolContext
from nerve.agent.tools.schemas import MEMORY_UPDATE_SCHEMA


@pytest.mark.asyncio
async def test_memory_update_preserves_commas_in_category_names():
    bridge = AsyncMock()
    bridge.available = True
    bridge.update_item.return_value = True
    categories = [
        "Nerve: релизы, эксплуатация, миграции и canary-проверки",
        "Codex: модели, навыки и Langfuse-наблюдаемость",
    ]

    result = await memory_update_handler(
        ToolContext(session_id="test", memory_bridge=bridge),
        {"memory_id": "memory-1", "categories": categories},
    )

    assert result.content[0]["text"] == "Memory memory-1 updated."
    bridge.update_item.assert_awaited_once_with(
        memory_id="memory-1",
        content=None,
        memory_type=None,
        categories=categories,
        source="agent_tool",
    )


@pytest.mark.asyncio
async def test_memory_update_accepts_legacy_comma_separated_categories():
    bridge = AsyncMock()
    bridge.available = True
    bridge.update_item.return_value = True

    await memory_update_handler(
        ToolContext(session_id="test", memory_bridge=bridge),
        {"memory_id": "memory-1", "categories": "work, personal"},
    )

    assert bridge.update_item.await_args.kwargs["categories"] == ["work", "personal"]


@pytest.mark.asyncio
async def test_memory_update_empty_category_array_clears_assignments():
    bridge = AsyncMock()
    bridge.available = True
    bridge.update_item.return_value = True

    await memory_update_handler(
        ToolContext(session_id="test", memory_bridge=bridge),
        {"memory_id": "memory-1", "categories": []},
    )

    assert bridge.update_item.await_args.kwargs["categories"] == []


def test_memory_update_schema_uses_category_name_array():
    categories = MEMORY_UPDATE_SCHEMA["properties"]["categories"]

    assert categories["type"] == "array"
    assert categories["items"] == {"type": "string"}
