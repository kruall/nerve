"""Provider-aware checks for the shared CLI/Telegram doctor report."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

from nerve.cli import doctor_report
from nerve.config import NerveConfig


def _codex_config(tmp_path: Path, *, embed_model: str = "") -> NerveConfig:
    (tmp_path / "config.yaml").write_text("workspace: .\n")
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "agent": {
            "backend": "codex",
            "cron_backend": "codex",
        },
        "memory": {
            "provider": "codex",
            "recall_model": "gpt-5.6-terra",
            "memorize_model": "gpt-5.6-terra",
            "fast_model": "gpt-5.6-luna",
            "embed_model": embed_model,
        },
    })
    config.config_dir = tmp_path
    return config


def test_codex_agent_and_memory_do_not_require_anthropic(tmp_path):
    report = doctor_report(_codex_config(tmp_path), check_api=False)

    assert "[--] Anthropic provider not required" in report
    assert "[OK] Memory chat provider: Codex" in report
    assert "[ERR] Anthropic API key" not in report


def test_inherited_memory_still_requires_anthropic(tmp_path):
    (tmp_path / "config.yaml").write_text("workspace: .\n")
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "agent": {
            "backend": "codex",
            "cron_backend": "codex",
        },
    })
    config.config_dir = tmp_path

    report = doctor_report(config, check_api=False)

    assert "[ERR] Anthropic API key not set" in report


def test_embedding_key_requires_model(tmp_path):
    config = _codex_config(tmp_path)
    config.openai_api_key = "sk-test-embedding"

    report = doctor_report(config, check_api=False)

    assert "[ERR] OpenAI API key is set but memory.embed_model is empty" in report


def test_embedding_key_and_model_are_reported(tmp_path):
    config = _codex_config(
        tmp_path,
        embed_model="text-embedding-3-small",
    )
    config.openai_api_key = "sk-test-embedding"

    report = doctor_report(config, check_api=False)

    assert "vector embeddings: text-embedding-3-small" in report
    assert "[ERR] OpenAI API key" not in report


def test_codex_auth_mismatch_is_an_error(tmp_path):
    config = _codex_config(
        tmp_path,
        embed_model="text-embedding-3-small",
    )
    config.openai_api_key = "sk-test-embedding"
    status = {
        "available": True,
        "version": "codex-cli 0.144.6",
        "auth": "api_key",
        "configured_auth": "chatgpt",
        "auth_mismatch": True,
        "models": [
            config.codex.model,
            config.memory.recall_model,
            config.memory.fast_model,
        ],
    }

    with patch(
        "nerve.agent.backends.codex.backend.CodexBackend.preflight",
        new=AsyncMock(return_value=status),
    ):
        report = doctor_report(config, check_api=True)

    assert (
        "[ERR] Codex auth mismatch: configured chatgpt, "
        "authenticated as api_key"
    ) in report


def test_memory_only_codex_uses_inventory_preflight_and_model_fallbacks(
    tmp_path,
):
    config = _codex_config(tmp_path)
    config.agent.backend = "claude"
    config.agent.cron_backend = "claude"
    config.anthropic_api_key = "test-anthropic-key"
    config.codex.model = "unused-agent-model"
    config.memory.memorize_model = ""
    config.memory.fast_model = "   "
    status = {
        "available": True,
        "version": "codex-cli 0.144.6",
        "auth": "chatgpt",
        "configured_auth": "chatgpt",
        "auth_mismatch": False,
        "models": [config.memory.recall_model],
    }
    preflight = AsyncMock(return_value=status)

    with (
        patch(
            "nerve.agent.backends.codex.backend.CodexBackend.preflight",
            new=preflight,
        ),
        patch(
            "nerve.cli._check_api_connectivity",
            return_value=(True, "ok"),
        ),
    ):
        report = doctor_report(config, check_api=True)

    preflight.assert_awaited_once_with(
        force=True,
        validate_default_model=False,
    )
    assert "[OK] Codex: codex-cli 0.144.6 (chatgpt)" in report
    assert "[ERR] Codex model(s) unavailable" not in report


def test_codex_agent_model_is_still_validated_by_doctor(tmp_path):
    config = _codex_config(tmp_path)
    status = {
        "available": True,
        "version": "codex-cli 0.144.6",
        "auth": "chatgpt",
        "configured_auth": "chatgpt",
        "auth_mismatch": False,
        "models": [
            config.memory.recall_model,
            config.memory.memorize_model,
            config.memory.fast_model,
        ],
    }

    with patch(
        "nerve.agent.backends.codex.backend.CodexBackend.preflight",
        new=AsyncMock(return_value=status),
    ):
        report = doctor_report(config, check_api=True)

    assert (
        f"[ERR] Codex model(s) unavailable: {config.codex.model}"
        in report
    )


def test_doctor_allows_explicit_unlisted_codex_models(tmp_path):
    config = _codex_config(tmp_path)
    config.codex.model = "local-agents-a1"
    config.codex.allow_unlisted_models = True
    status = {
        "available": True,
        "version": "codex-cli 0.144.6",
        "auth": "chatgpt",
        "configured_auth": "chatgpt",
        "auth_mismatch": False,
        "models": ["gpt-5.6-sol"],
    }

    with patch(
        "nerve.agent.backends.codex.backend.CodexBackend.preflight",
        new=AsyncMock(return_value=status),
    ):
        report = doctor_report(config, check_api=True)

    assert "[OK] Codex: codex-cli 0.144.6 (chatgpt)" in report
    assert "[ERR] Codex model(s) unavailable" not in report


def test_doctor_rejects_empty_active_codex_agent_model(tmp_path):
    config = _codex_config(tmp_path)
    config.codex.model = "   "
    status = {
        "available": True,
        "version": "codex-cli 0.144.6",
        "auth": "chatgpt",
        "configured_auth": "chatgpt",
        "auth_mismatch": False,
        "models": [
            config.memory.recall_model,
            config.memory.memorize_model,
            config.memory.fast_model,
        ],
    }

    with patch(
        "nerve.agent.backends.codex.backend.CodexBackend.preflight",
        new=AsyncMock(return_value=status),
    ):
        report = doctor_report(config, check_api=True)

    assert "[ERR] codex.model is empty" in report
    assert "[OK] Codex:" not in report


def test_doctor_rejects_empty_memory_recall_model(tmp_path):
    config = _codex_config(tmp_path)
    config.agent.backend = "claude"
    config.agent.cron_backend = "claude"
    config.anthropic_api_key = "test-anthropic-key"
    config.memory.recall_model = "   "
    status = {
        "available": True,
        "version": "codex-cli 0.144.6",
        "auth": "chatgpt",
        "configured_auth": "chatgpt",
        "auth_mismatch": False,
        "models": [],
    }

    with (
        patch(
            "nerve.agent.backends.codex.backend.CodexBackend.preflight",
            new=AsyncMock(return_value=status),
        ),
        patch(
            "nerve.cli._check_api_connectivity",
            return_value=(True, "ok"),
        ),
    ):
        report = doctor_report(config, check_api=True)

    assert "[ERR] memory.recall_model is empty" in report
    assert "[OK] Codex:" not in report
