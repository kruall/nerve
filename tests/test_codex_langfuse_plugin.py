from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from nerve.agent.backends.codex import langfuse_plugin as plugin
from nerve.config import NerveConfig


REVISION = "0123456789abcdef0123456789abcdef01234567"


@pytest.fixture(autouse=True)
def _reset_plugin_error(monkeypatch):
    for name in (
        "TRACE_TO_LANGFUSE", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY",
        "LANGFUSE_BASE_URL", "LANGFUSE_HOST",
    ):
        monkeypatch.delenv(name, raising=False)
    plugin._last_error = None


def _config(tmp_path: Path, **codex_values) -> NerveConfig:
    values = {
        "enabled": True,
        "auto_install": True,
        "version": "0.1.0",
        "revision": REVISION,
        **codex_values,
    }
    return NerveConfig.from_dict({
        "codex": {"home_dir": str(tmp_path), "bin_path": "codex"},
        "langfuse": {
            "public_key": "pk-lf-test",
            "secret_key": "sk-lf-test",
            "base_url": "https://us.cloud.langfuse.com/",
            "codex": values,
        },
    })


def _materialize_marketplace(config: NerveConfig) -> Path:
    root = (
        Path(config.codex.home_dir)
        / ".tmp" / "marketplaces" / "codex-observability-plugin"
    )
    (root / ".git").mkdir(parents=True)
    (root / ".git" / "HEAD").write_text(REVISION)
    plugin_root = root / "plugins" / "tracing"
    entrypoint = plugin_root / "dist" / "index.mjs"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.write_text("\n".join(
        original for original, _ in plugin._BUNDLE_USAGE_REPLACEMENTS
    ))
    source = plugin_root / "src" / "trace.ts"
    source.parent.mkdir()
    source.write_text("\n".join(
        original for original, _ in plugin._SOURCE_USAGE_REPLACEMENTS
    ))
    return root


def _materialize_runtime(config: NerveConfig) -> Path:
    root = (
        Path(config.codex.home_dir)
        / "plugins" / "cache" / "codex-observability-plugin"
        / "tracing" / "0.1.0"
    )
    manifest = root / ".codex-plugin" / "plugin.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"name": "tracing", "version": "0.1.0"}))
    entrypoint = root / "dist" / "index.mjs"
    entrypoint.parent.mkdir()
    entrypoint.write_text("\n".join(
        original for original, _ in plugin._BUNDLE_USAGE_REPLACEMENTS
    ))
    source = root / "src" / "trace.ts"
    source.parent.mkdir()
    source.write_text("\n".join(
        original for original, _ in plugin._SOURCE_USAGE_REPLACEMENTS
    ))
    return root


def _materialize_installed(config: NerveConfig) -> Path:
    _materialize_marketplace(config)
    root = _materialize_runtime(config)
    plugin._apply_usage_patch(plugin._marketplace_plugin_root(config))
    plugin._apply_usage_patch(root)
    plugin._write_install_receipt(config, root, "0.1.0")
    return root


def _write_legacy_receipt(config: NerveConfig, root: Path) -> None:
    plugin._atomic_write(
        plugin.managed_dir(config.codex.home_dir) / "install-receipt.json",
        json.dumps({
            "path": str(root),
            "version": "0.1.0",
            "revision": REVISION,
            "digest": plugin._tree_digest(root),
        }),
    )


def test_status_requires_exact_version_revision_and_credentials(tmp_path):
    config = _config(tmp_path)
    root = _materialize_installed(config)

    status = plugin.installation_status(config)

    assert status["ready"] is True
    assert status["version"] == "0.1.0"
    assert status["revision"] == REVISION
    assert status["path"] == str(root)
    assert status["usage_normalization"] == "exclusive-usage-v2"
    assert "pk-lf-test" not in json.dumps(status)
    assert "sk-lf-test" not in json.dumps(status)


def test_codex_cache_uses_content_bound_install_receipt(tmp_path):
    config = _config(tmp_path)
    root = _materialize_installed(config)

    assert plugin.installation_status(config)["ready"] is True

    (root / "dist" / "index.mjs").write_text("changed after verification")
    assert plugin.installation_status(config)["ready"] is False


def test_usage_patch_makes_flat_langfuse_buckets_exclusive(tmp_path):
    config = _config(tmp_path)
    root = _materialize_runtime(config)

    plugin._apply_usage_patch(root)

    source = (root / "src" / "trace.ts").read_text()
    bundle = (root / "dist" / "index.mjs").read_text()
    for content in (source, bundle):
        assert "usage.input_tokens - cachedInputTokens" in content
        assert "usage.output_tokens - reasoningOutputTokens" in content
        assert "details.input_cached_tokens = cachedInputTokens" in content
        assert "details.output_reasoning_tokens = reasoningOutputTokens" in content
    assert plugin._usage_patch_applied(root) is True

    # Reapplying the managed patch is deterministic and idempotent.
    digest = plugin._tree_digest(root)
    plugin._apply_usage_patch(root)
    assert plugin._tree_digest(root) == digest


@pytest.mark.asyncio
async def test_legacy_verified_install_is_patched_without_network(
    tmp_path, monkeypatch,
):
    config = _config(tmp_path)
    _materialize_marketplace(config)
    root = _materialize_runtime(config)
    _write_legacy_receipt(config, root)

    async def unexpected_run(*args, **kwargs):
        raise AssertionError("legacy repair must not invoke Codex or the network")

    monkeypatch.setattr(plugin, "_run", unexpected_run)
    status = await plugin.ensure_installed(config)

    assert status["ready"] is True
    assert status["usage_normalization"] == "exclusive-usage-v2"
    assert plugin._usage_patch_applied(root) is True


@pytest.mark.asyncio
async def test_verified_unpatched_current_receipt_is_repaired_without_network(
    tmp_path, monkeypatch,
):
    config = _config(tmp_path)
    _materialize_marketplace(config)
    root = _materialize_runtime(config)
    # A previously interrupted or externally reset cache can retain a receipt
    # that claims the normalization patch while its verified bytes are original.
    plugin._write_install_receipt(config, root, "0.1.0")

    async def unexpected_run(*args, **kwargs):
        raise AssertionError(
            "verified cache repair must not invoke Codex or the network"
        )

    monkeypatch.setattr(plugin, "_run", unexpected_run)
    status = await plugin.ensure_installed(config)

    assert status["ready"] is True
    assert status["usage_normalization"] == "exclusive-usage-v2"
    assert plugin._usage_patch_applied(root) is True


@pytest.mark.asyncio
async def test_post_start_repair_restores_cache_rebuilt_by_codex(tmp_path):
    config = _config(tmp_path)
    root = _materialize_installed(config)

    # Codex rebuilds the cache from the already-patched marketplace during
    # app-server start, invalidating only the pre-start receipt.
    marketplace = plugin._marketplace_plugin_root(config)
    for relative in (Path("src/trace.ts"), Path("dist/index.mjs")):
        source = marketplace / relative
        (root / relative).write_bytes(source.read_bytes())

    status = await plugin.repair_after_appserver_start(config)

    assert status["ready"] is True
    assert status["usage_normalization"] == "exclusive-usage-v2"
    assert plugin._usage_patch_applied(root) is True


def test_marketplace_patch_survives_runtime_rematerialization(tmp_path):
    config = _config(tmp_path)
    root = _materialize_installed(config)
    marketplace = plugin._marketplace_plugin_root(config)

    for relative in (Path("src/trace.ts"), Path("dist/index.mjs")):
        (root / relative).write_bytes((marketplace / relative).read_bytes())

    assert plugin._usage_patch_applied(marketplace) is True
    assert plugin._usage_patch_applied(root) is True


def test_marketplace_patch_is_content_bound_by_receipt(tmp_path):
    config = _config(tmp_path)
    _materialize_installed(config)
    marketplace_bundle = plugin._marketplace_plugin_root(config) / "dist/index.mjs"

    marketplace_bundle.write_text("externally reset marketplace")

    assert plugin.installation_status(config)["ready"] is False


@pytest.mark.asyncio
async def test_post_start_repair_rejects_unreviewed_runtime_cache(tmp_path):
    config = _config(tmp_path)
    root = _materialize_installed(config)
    runtime_source = root / "src" / "trace.ts"
    runtime_source.write_text("unreviewed runtime hook")

    status = await plugin.repair_after_appserver_start(config)

    assert status["ready"] is False
    assert status["usage_normalization"] is None
    assert "does not match the reviewed marketplace snapshot" in status["last_error"]


@pytest.mark.asyncio
async def test_usage_patch_mismatch_disables_tracing_without_network(
    tmp_path, monkeypatch,
):
    config = _config(tmp_path)
    _materialize_marketplace(config)
    root = _materialize_runtime(config)
    entrypoint = root / "dist" / "index.mjs"
    entrypoint.write_text(entrypoint.read_text().replace(
        'details.input = usage.input_tokens',
        'details.input = unexpected_provider_counter',
    ))
    _write_legacy_receipt(config, root)

    async def unexpected_run(*args, **kwargs):
        raise AssertionError("a reviewed-artifact mismatch must fail open")

    monkeypatch.setattr(plugin, "_run", unexpected_run)
    status = await plugin.ensure_installed(config)

    assert status["ready"] is False
    assert status["usage_normalization"] is None
    assert "does not match the reviewed plugin" in status["last_error"]


def test_local_marketplace_source_tree_is_not_runtime_ready(tmp_path):
    config = _config(tmp_path)
    root = (
        Path(config.codex.home_dir)
        / "plugins" / "cache" / "codex-observability-plugin"
        / "tracing" / "local" / "plugins" / "tracing"
    )
    manifest = root / ".codex-plugin" / "plugin.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"name": "tracing", "version": "0.1.0"}))
    (root / ".git").mkdir()
    (root / ".git" / "HEAD").write_text(REVISION)
    plugin._write_install_receipt(config, root, "0.1.0")

    status = plugin.installation_status(config)

    assert status["installed"] is False
    assert status["ready"] is False
    assert status["path"] is None


def test_child_configuration_contains_no_secret_in_overrides(tmp_path):
    config = _config(tmp_path)

    env = plugin.child_env(config)
    overrides = plugin.config_overrides()

    assert env["TRACE_TO_LANGFUSE"] == "true"
    assert env["LANGFUSE_BASE_URL"] == "https://us.cloud.langfuse.com"
    assert env["LANGFUSE_CODEX_MAX_CHARS"] == "20000"
    assert overrides == [
        "features.hooks=true",
        "features.plugin_hooks=true",
        'plugins."tracing@codex-observability-plugin".enabled=true',
    ]
    serialized = " ".join(overrides)
    assert config.langfuse.public_key not in serialized
    assert config.langfuse.secret_key not in serialized


@pytest.mark.asyncio
async def test_missing_revision_fails_open_without_install(tmp_path, monkeypatch):
    config = _config(tmp_path)
    config.langfuse.codex.revision = ""
    called = False

    async def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        return ""

    monkeypatch.setattr(plugin, "_run", fake_run)
    status = await plugin.ensure_installed(config)

    assert called is False
    assert status["ready"] is False
    assert status["last_error"] == "verified plugin revision is not configured"


@pytest.mark.asyncio
async def test_install_is_idempotent_under_concurrent_startup(tmp_path, monkeypatch):
    config = _config(tmp_path)
    calls: list[tuple[str, ...]] = []

    async def fake_run(*args, env, timeout=90.0):
        calls.append(tuple(args))
        assert "LANGFUSE_SECRET_KEY" not in env
        if args[1:4] == ("plugin", "marketplace", "add"):
            _materialize_marketplace(config)
        if args[1:3] == ("plugin", "add"):
            _materialize_runtime(config)
        return "{}"

    monkeypatch.setattr(plugin, "_run", fake_run)
    first, second = await asyncio.gather(
        plugin.ensure_installed(config),
        plugin.ensure_installed(config),
    )

    assert first["ready"] is True
    assert second["ready"] is True
    assert len(calls) == 4
    assert calls[0][1:4] == ("plugin", "remove", "tracing@codex-observability-plugin")
    assert calls[1][1:5] == (
        "plugin", "marketplace", "remove", "codex-observability-plugin",
    )
    assert calls[2][1:] == (
        "plugin", "marketplace", "add",
        "https://github.com/langfuse/codex-observability-plugin.git",
        "--ref", REVISION, "--json",
    )
    assert calls[3][1:3] == ("plugin", "add")


@pytest.mark.asyncio
async def test_install_error_is_redacted_and_fail_open(tmp_path, monkeypatch):
    config = _config(tmp_path)

    async def fake_run(*args, **kwargs):
        raise RuntimeError(f"authorization={config.langfuse.secret_key}")

    monkeypatch.setattr(plugin, "_run", fake_run)
    status = await plugin.ensure_installed(config)

    assert status["ready"] is False
    assert config.langfuse.secret_key not in status["last_error"]
    assert "[redacted]" in status["last_error"]


@pytest.mark.asyncio
async def test_remove_missing_marketplace_is_idempotent(monkeypatch):
    async def fake_run(*args, **kwargs):
        raise RuntimeError("marketplace is not configured or installed")

    monkeypatch.setattr(plugin, "_run", fake_run)

    await plugin._remove_if_present("codex", "plugin", "marketplace", "remove", env={})
