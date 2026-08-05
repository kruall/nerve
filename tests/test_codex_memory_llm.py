"""Offline tests for the Codex-backed memU LLM adapter."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from nerve.memory.codex_llm import (
    CodexMemoryError,
    CodexMemoryLLMClient,
    CodexMemoryPool,
    CodexMemoryRuntime,
)


FAKE_BIN = str(Path(__file__).parent / "fixtures" / "fake_codex_appserver.py")
MODEL = "gpt-5.6-sol"
MIN_VERSION = "0.144.1"
MAX_VERSION = "0.145.0"


def _home_with_mode(tmp_path: Path, mode: str) -> Path:
    home = tmp_path / "codex-home"
    home.mkdir(parents=True)
    (home / "fake_codex_mode").write_text(mode, encoding="utf-8")
    return home


def _runtime(
    tmp_path: Path,
    mode: str = "memory",
    **runtime_overrides,
) -> CodexMemoryRuntime:
    home = _home_with_mode(tmp_path, mode)
    return CodexMemoryRuntime(
        bin_path=FAKE_BIN,
        min_version=MIN_VERSION,
        max_version=MAX_VERSION,
        home_dir=str(home),
        work_dir=str(tmp_path / "memory-work"),
        request_timeout=2,
        turn_timeout=5,
        idle_timeout=2,
        **runtime_overrides,
    )


def _pool(tmp_path: Path, mode: str = "memory") -> CodexMemoryPool:
    home = _home_with_mode(tmp_path, mode)
    return CodexMemoryPool(
        workers=2,
        bin_path=FAKE_BIN,
        min_version=MIN_VERSION,
        max_version=MAX_VERSION,
        home_dir=str(home),
        work_dir=str(tmp_path / "memory-work"),
        request_timeout=2,
        turn_timeout=5,
        idle_timeout=2,
    )


@pytest.mark.asyncio
async def test_chat_uses_authoritative_output_and_isolated_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = {
        "OPENAI_API_KEY": "openai-secret-for-embeddings",
        "ANTHROPIC_API_KEY": "anthropic-secret",
        "AWS_SECRET_ACCESS_KEY": "aws-secret",
        "NERVE_MCP_TOKEN": "nerve-mcp-secret",
        "GITHUB_TOKEN": "github-secret",
        "CUSTOM_SECRET": "custom-secret",
    }
    for name, value in secrets.items():
        monkeypatch.setenv(name, value)

    runtime = _runtime(tmp_path)
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        text, metadata = await client.chat(
            "extract memory alpha",
            system_prompt="Return exact test data.",
            max_tokens=17,
        )
        payload = json.loads(text)

        # The fake streams non-JSON text first. Successful parsing proves the
        # authoritative item/completed agentMessage won over deltas.
        assert payload["prompt"] == "extract memory alpha"
        assert metadata["provider"] == "codex"
        assert metadata["model"] == MODEL

        thread = payload["threadParams"]
        assert thread["ephemeral"] is True
        assert thread["sandbox"] == "read-only"
        assert thread["approvalPolicy"] == "never"
        assert thread["dynamicTools"] == []
        assert thread["environments"] == []
        assert thread["runtimeWorkspaceRoots"] == []
        assert thread["selectedCapabilityRoots"] == []
        assert "Never call tools" in thread["baseInstructions"]
        assert "Return exact test data." in thread["developerInstructions"]
        assert "approximately 17 tokens" in thread["developerInstructions"]

        turn = payload["turnParams"]
        assert turn["threadId"] == metadata["thread_id"]
        assert turn["input"] == [
            {"type": "text", "text": "extract memory alpha"},
        ]
        assert turn["summary"] == "none"
        assert turn["environments"] == []
        assert turn["runtimeWorkspaceRoots"] == []

        overrides = set(payload["configOverrides"])
        assert "project_doc_max_bytes=0" in overrides
        assert "tools.web_search=false" in overrides
        assert "mcp_servers={}" in overrides
        assert "features.shell_tool=false" in overrides
        assert "features.unified_exec=false" in overrides
        assert "features.browser_use=false" in overrides
        assert "features.computer_use=false" in overrides
        assert "features.plugins=false" in overrides
        assert "features.multi_agent=false" in overrides

        assert payload["configReadParams"] == {
            "cwd": str(tmp_path / "memory-work"),
            "includeLayers": True,
        }
        assert payload["configRequirementsRead"] is True
        assert payload["codexHome"] == str(tmp_path / "codex-home")
        assert not any(payload["sensitiveEnvPresent"].values())
        for value in secrets.values():
            assert value not in text
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_two_worker_pool_keeps_concurrent_calls_separate(
    tmp_path: Path,
) -> None:
    pool = _pool(tmp_path)
    client = CodexMemoryLLMClient(runtime=pool, chat_model=MODEL)
    try:
        first, second = await asyncio.gather(
            client.chat("first prompt"),
            client.chat("second prompt"),
        )
        first_payload = json.loads(first[0])
        second_payload = json.loads(second[0])

        assert first_payload["prompt"] == "first prompt"
        assert second_payload["prompt"] == "second prompt"
        # Each active turn owns a distinct app-server process. Shared queues
        # therefore cannot route one completion into the other request.
        assert first_payload["pid"] != second_payload["pid"]
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_pool_close_wakes_waiting_callers(
    tmp_path: Path,
) -> None:
    home = _home_with_mode(tmp_path, "memory")
    pool = CodexMemoryPool(
        workers=1,
        bin_path=FAKE_BIN,
        min_version=MIN_VERSION,
        max_version=MAX_VERSION,
        home_dir=str(home),
        work_dir=str(tmp_path / "memory-work"),
        request_timeout=2,
        turn_timeout=5,
        idle_timeout=2,
    )
    client = CodexMemoryLLMClient(runtime=pool, chat_model=MODEL)

    active = asyncio.create_task(client.chat("active prompt"))
    await asyncio.sleep(0.02)
    waiting = asyncio.create_task(client.chat("waiting prompt"))
    await asyncio.sleep(0.02)
    closing = asyncio.create_task(pool.close())

    with pytest.raises(CodexMemoryError, match="pool is closed"):
        await asyncio.wait_for(waiting, timeout=1)
    await asyncio.wait_for(active, timeout=2)
    await asyncio.wait_for(closing, timeout=2)


@pytest.mark.asyncio
async def test_unavailable_model_fails_and_drops_runtime(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    client = CodexMemoryLLMClient(
        runtime=runtime,
        chat_model="gpt-model-not-listed",
    )
    try:
        with pytest.raises(CodexMemoryError, match="unavailable"):
            await client.chat("must not start a turn")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_explicit_unlisted_memory_model_is_allowed(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, allow_unlisted_models=True)
    client = CodexMemoryLLMClient(
        runtime=runtime,
        chat_model="local-agents-a1",
    )
    try:
        text, metadata = await client.chat("use the custom model")
        assert json.loads(text)["prompt"] == "use the custom model"
        assert metadata["model"] == "local-agents-a1"
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_auth_mismatch_fails_before_starting_turn(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, mode="account_api_key")
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(CodexMemoryError, match="auth mismatch"):
            await client.chat("must not use API billing")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_failed_turn_surfaces_error_and_drops_runtime(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, mode="failed_turn")
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(CodexMemoryError, match="model exploded"):
            await client.chat("trigger failed turn")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_native_tool_activity_is_rejected(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, mode="tools")
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(CodexMemoryError, match="forbidden tool activity"):
            await client.chat("try to invoke a native tool")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_inherited_mcp_fails_closed_before_starting_thread(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, mode="memory_inherited_mcp")
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(
            CodexMemoryError,
            match=r"inherited MCP server.*danger",
        ):
            await client.chat("must not start a thread")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_malformed_effective_config_fails_closed(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, mode="memory_malformed_config")
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(
            CodexMemoryError,
            match="invalid effective config",
        ):
            await client.chat("must not start a thread")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_disabled_mcp_and_inactive_layer_do_not_block_memory(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, mode="memory_disabled_mcp")
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        text, _ = await client.chat("safe disabled config")
        assert json.loads(text)["prompt"] == "safe disabled config"
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_managed_required_feature_fails_closed_before_thread(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, mode="memory_required_feature")
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(
            CodexMemoryError,
            match=r"managed requirements.*plugins",
        ):
            await client.chat("must not start a thread")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["codex-cli 0.144.0", "codex-cli 0.145"])
async def test_unsupported_cli_version_fails_before_appserver_start(
    tmp_path: Path,
    version: str,
) -> None:
    home = _home_with_mode(tmp_path, "memory")
    (home / "fake_codex_version").write_text(version, encoding="utf-8")
    runtime = CodexMemoryRuntime(
        bin_path=FAKE_BIN,
        min_version=MIN_VERSION,
        max_version=MAX_VERSION,
        home_dir=str(home),
        work_dir=str(tmp_path / "memory-work"),
        request_timeout=2,
        turn_timeout=5,
        idle_timeout=2,
    )
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(CodexMemoryError, match="Unsupported codex-cli"):
            await client.chat("must not start app-server")
        assert runtime.is_alive is False
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_malformed_cli_version_fails_cleanly(tmp_path: Path) -> None:
    home = _home_with_mode(tmp_path, "memory")
    (home / "fake_codex_version").write_text(
        "codex-cli development",
        encoding="utf-8",
    )
    runtime = CodexMemoryRuntime(
        bin_path=FAKE_BIN,
        min_version=MIN_VERSION,
        max_version=MAX_VERSION,
        home_dir=str(home),
        work_dir=str(tmp_path / "memory-work"),
        request_timeout=2,
        turn_timeout=5,
        idle_timeout=2,
    )
    client = CodexMemoryLLMClient(runtime=runtime, chat_model=MODEL)
    try:
        with pytest.raises(CodexMemoryError, match="Could not parse"):
            await client.chat("must not start app-server")
        assert runtime.is_alive is False
    finally:
        await runtime.close()
