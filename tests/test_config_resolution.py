"""Tests for config-dir resolution, the pointer file, unknown-key
validation, and config write-back helpers."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest
import yaml

from nerve.config import (
    DiscordConfig,
    NerveConfig,
    TelegramConfig,
    append_telegram_allowed_user,
    load_config,
    read_config_pointer,
    resolve_config_dir,
    validate_config_keys,
    write_config_pointer,
)


@pytest.fixture
def configured(tmp_path: Path) -> Path:
    """A directory that looks like a real install."""
    d = tmp_path / "install"
    d.mkdir()
    (d / "config.yaml").write_text("workspace: ~/ws\n")
    (d / "config.local.yaml").write_text("anthropic_api_key: sk-ant-test\n")
    return d


@pytest.fixture
def elsewhere(tmp_path: Path) -> Path:
    """An empty directory with no config files."""
    d = tmp_path / "elsewhere"
    d.mkdir()
    return d


@pytest.fixture(autouse=True)
def _no_env_override(monkeypatch):
    monkeypatch.delenv("NERVE_CONFIG_DIR", raising=False)


class TestResolveConfigDir:
    def test_explicit_flag_wins(self, configured, elsewhere, monkeypatch):
        monkeypatch.setenv("NERVE_CONFIG_DIR", str(configured))
        d, source = resolve_config_dir(str(elsewhere))
        assert d == elsewhere
        assert source == "flag"

    def test_env_var(self, configured, elsewhere, monkeypatch):
        monkeypatch.chdir(elsewhere)
        monkeypatch.setenv("NERVE_CONFIG_DIR", str(configured))
        d, source = resolve_config_dir()
        assert d == configured
        assert source == "env"

    def test_cwd_with_config(self, configured, monkeypatch):
        monkeypatch.chdir(configured)
        d, source = resolve_config_dir()
        assert d == configured
        assert source == "cwd"

    def test_pointer_used_from_other_cwd(self, configured, elsewhere, monkeypatch):
        """The classic footgun: install configured in ~/nerve, command run from $HOME."""
        write_config_pointer(configured)
        monkeypatch.chdir(elsewhere)
        d, source = resolve_config_dir()
        assert d == configured.resolve()
        assert source == "pointer"

    def test_cwd_config_beats_pointer(self, configured, tmp_path, monkeypatch):
        """Dev workflow: a checkout with its own config wins over the pointer."""
        write_config_pointer(configured)
        dev = tmp_path / "dev-checkout"
        dev.mkdir()
        (dev / "config.yaml").write_text("workspace: ~/dev-ws\n")
        monkeypatch.chdir(dev)
        d, source = resolve_config_dir()
        assert d == dev
        assert source == "cwd"

    def test_stale_pointer_falls_back(self, tmp_path, elsewhere, monkeypatch):
        gone = tmp_path / "gone"
        gone.mkdir()
        write_config_pointer(gone)
        gone.rmdir()
        monkeypatch.chdir(elsewhere)
        d, source = resolve_config_dir()
        assert d == elsewhere
        assert source == "default"

    def test_pointer_without_config_files_ignored(self, elsewhere, tmp_path, monkeypatch):
        empty_install = tmp_path / "empty-install"
        empty_install.mkdir()
        write_config_pointer(empty_install)
        monkeypatch.chdir(elsewhere)
        d, source = resolve_config_dir()
        assert source == "default"

    def test_fresh_install_default(self, elsewhere, monkeypatch):
        monkeypatch.chdir(elsewhere)
        d, source = resolve_config_dir()
        assert d == elsewhere
        assert source == "default"


class TestConfigPointer:
    def test_round_trip(self, configured):
        write_config_pointer(configured)
        assert read_config_pointer() == configured.resolve()

    def test_missing_returns_none(self):
        assert read_config_pointer() is None

    def test_deleted_dir_returns_none(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        write_config_pointer(d)
        d.rmdir()
        assert read_config_pointer() is None


class TestLoadConfigDir:
    def test_load_config_records_dir(self, configured):
        config = load_config(configured)
        assert config.config_dir == configured

    def test_load_config_uses_resolution(self, configured, elsewhere, monkeypatch):
        """load_config(None) resolves via the waterfall, not bare CWD."""
        write_config_pointer(configured)
        monkeypatch.chdir(elsewhere)
        config = load_config()
        assert config.anthropic_api_key == "sk-ant-test"


class TestValidateConfigKeys:
    def test_clean_config_no_warnings(self):
        merged = {
            "workspace": "~/ws",
            "timezone": "UTC",
            "anthropic_api_key": "sk-ant-x",
            "telegram": {
                "enabled": True,
                "dm_policy": "pairing",
                "allowed_users": [1],
                "stream_mode": "partial",
            },
            "agent": {"model": "claude-opus-4-8"},
            "auth": {"jwt_secret": "s"},
        }
        assert validate_config_keys(merged) == []

    def test_unknown_top_level_key(self):
        warnings = validate_config_keys({"workspaec": "~/ws"})
        assert len(warnings) == 1
        assert "workspaec" in warnings[0]

    def test_unknown_nested_key_with_dotted_path(self):
        warnings = validate_config_keys({"telegram": {"dm_policyy": "pairing"}})
        assert len(warnings) == 1
        assert "telegram.dm_policyy" in warnings[0]

    def test_docker_entrypoint_keys_allowed(self):
        merged = {"claude_oauth_token": "tok", "github_token": "ghp_x"}
        assert validate_config_keys(merged) == []

    def test_opaque_subtrees_not_flagged(self):
        merged = {
            "mcp_servers": {"my-server": {"command": "x", "anything": 1}},
            "memory": {"categories": [{"name": "a", "description": "b"}]},
        }
        assert validate_config_keys(merged) == []

    def test_codex_memory_keys_are_known(self):
        merged = {
            "memory": {
                "provider": "codex",
                "codex_workers": 2,
                "codex_effort": "low",
            },
        }
        assert validate_config_keys(merged) == []


class TestMemoryProviderResolution:
    def test_inherit_preserves_global_anthropic_provider(self):
        config = NerveConfig.from_dict({})
        assert config.resolved_memory_provider == "anthropic"

    def test_inherit_preserves_global_bedrock_provider(self):
        config = NerveConfig.from_dict({
            "provider": {"type": "bedrock"},
        })
        assert config.resolved_memory_provider == "bedrock"

    def test_explicit_codex_is_independent_of_agent_backend(self):
        config = NerveConfig.from_dict({
            "memory": {"provider": "codex"},
        })
        assert config.resolved_memory_provider == "codex"

    def test_rejects_unknown_memory_provider(self):
        with pytest.raises(ValueError, match="memory.provider"):
            NerveConfig.from_dict({
                "memory": {"provider": "mystery"},
            })

    def test_codex_model_names_are_normalized(self):
        config = NerveConfig.from_dict({
            "codex": {
                "model": "  gpt-5.6-sol  ",
                "cron_model": "   ",
            },
        })
        assert config.codex.model == "gpt-5.6-sol"
        assert config.codex.cron_model == ""


class TestTelegramDmPolicy:
    def test_default_is_pairing(self):
        assert TelegramConfig.from_dict({}).dm_policy == "pairing"

    def test_open_accepted(self):
        assert TelegramConfig.from_dict({"dm_policy": "open"}).dm_policy == "open"

    def test_invalid_falls_back_to_pairing(self):
        assert TelegramConfig.from_dict({"dm_policy": "weird"}).dm_policy == "pairing"

    def test_allowed_users_coerced_to_int(self):
        cfg = TelegramConfig.from_dict({"allowed_users": ["123", 456]})
        assert cfg.allowed_users == [123, 456]


class TestDiscordConfig:
    def test_defaults_are_fail_closed(self):
        cfg = DiscordConfig.from_dict({})
        assert cfg.enabled is False
        assert cfg.guild_id == 0
        assert cfg.channel_ids == []
        assert cfg.task_forums == {}
        assert cfg.audit_forum_id == 0
        assert cfg.audit_batch_window_seconds == 60.0
        assert cfg.allowed_author_ids == []
        assert cfg.require_mention is True

    def test_ids_are_coerced_to_int_and_projects_are_preserved(self):
        cfg = DiscordConfig.from_dict({
            "guild_id": "100",
            "channel_ids": ["200"],
            "task_forums": {"YDB": "300"},
            "audit_forum_id": "350",
            "audit_batch_window_seconds": "90",
            "allowed_author_ids": ["400", 500],
        })
        assert cfg.guild_id == 100
        assert cfg.channel_ids == [200]
        assert cfg.task_forums == {"YDB": 300}
        assert cfg.audit_forum_id == 350
        assert cfg.audit_batch_window_seconds == 90.0
        assert cfg.allowed_author_ids == [400, 500]

    def test_known_keys_pass_validation(self):
        merged = {
            "discord": {
                "enabled": True,
                "bot_token_file": "~/.config/nerve/discord-token",
                "guild_id": 100,
                "channel_ids": [200],
                "task_forums": {"YDB": 300},
                "audit_forum_id": 350,
                "audit_batch_window_seconds": 60,
                "allowed_author_ids": [400],
                "require_mention": True,
            },
        }
        assert validate_config_keys(merged) == []


class TestAppendTelegramAllowedUser:
    def test_creates_file_when_missing(self, tmp_path):
        assert append_telegram_allowed_user(tmp_path, 42) is True
        data = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert data["telegram"]["allowed_users"] == [42]

    def test_preserves_existing_keys(self, tmp_path):
        (tmp_path / "config.local.yaml").write_text(
            yaml.safe_dump({
                "anthropic_api_key": "sk-ant-keep",
                "telegram": {"bot_token": "123:abc"},
            })
        )
        assert append_telegram_allowed_user(tmp_path, 42) is True
        data = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert data["anthropic_api_key"] == "sk-ant-keep"
        assert data["telegram"]["bot_token"] == "123:abc"
        assert data["telegram"]["allowed_users"] == [42]

    def test_duplicate_returns_false(self, tmp_path):
        append_telegram_allowed_user(tmp_path, 42)
        assert append_telegram_allowed_user(tmp_path, 42) is False
        data = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert data["telegram"]["allowed_users"] == [42]

    def test_appends_second_user(self, tmp_path):
        append_telegram_allowed_user(tmp_path, 1)
        append_telegram_allowed_user(tmp_path, 2)
        data = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert data["telegram"]["allowed_users"] == [1, 2]

    def test_file_permissions(self, tmp_path):
        append_telegram_allowed_user(tmp_path, 42)
        mode = stat.S_IMODE(os.stat(tmp_path / "config.local.yaml").st_mode)
        assert mode == 0o600
