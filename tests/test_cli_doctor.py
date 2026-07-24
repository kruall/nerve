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
