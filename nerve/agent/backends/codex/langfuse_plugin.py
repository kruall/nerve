"""Managed Langfuse plugin lifecycle for Nerve's isolated Codex home.

The official plugin reads Codex rollout transcripts, so it is deliberately
opt-in and pinned to an operator-reviewed git revision. Installation failures
are recorded as safe diagnostics and must never make Codex unavailable.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import signal
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MARKETPLACE = "codex-observability-plugin"
_PLUGIN = "tracing"
_INSTALL_LOCK = asyncio.Lock()
_last_error: str | None = None


def managed_dir(home: str | Path) -> Path:
    return Path(home).expanduser() / "nerve-managed" / "langfuse"


def _marketplace_root(config: Any) -> Path:
    return (
        Path(config.codex.home_dir).expanduser()
        / ".tmp" / "marketplaces" / _MARKETPLACE
    )


def _runtime_root(config: Any) -> Path:
    plugin = config.langfuse.codex
    return (
        Path(config.codex.home_dir).expanduser()
        / "plugins" / "cache" / _MARKETPLACE / _PLUGIN / plugin.version
    )


def _hook_entrypoint(root: Path) -> Path:
    return root / "dist" / "index.mjs"


def _atomic_write(path: Path, content: str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(content, encoding="utf-8")
    os.chmod(tmp, mode)
    tmp.replace(path)
    os.chmod(path, mode)


def _try_acquire_file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    os.chmod(path, 0o600)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return handle
    except BlockingIOError:
        handle.close()
        return None


async def _acquire_file_lock(path: Path, timeout: float = 180.0):
    deadline = time.monotonic() + timeout
    while True:
        handle = _try_acquire_file_lock(path)
        if handle is not None:
            return handle
        if time.monotonic() >= deadline:
            raise RuntimeError("timed out waiting for Langfuse plugin install lock")
        await asyncio.sleep(0.1)


def _release_file_lock(handle) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _safe_error(error: BaseException | str, config: Any) -> str:
    text = str(error).replace("\n", " ").strip()
    lf = config.langfuse
    for value in (lf.public_key, lf.secret_key):
        if value:
            text = text.replace(str(value), "[redacted]")
    text = re.sub(
        r"(?i)(authorization|password|secret|token|api[_-]?key)\s*[:=]\s*\S+",
        r"\1=[redacted]",
        text,
    )
    return text[:1000] or "unknown plugin error"


def record_error(error: BaseException | str, config: Any) -> None:
    global _last_error
    _last_error = _safe_error(error, config)
    logger.warning("Langfuse Codex plugin unavailable: %s", _last_error)


def _git_revision(plugin_root: Path) -> str:
    git_dir = plugin_root / ".git"
    try:
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref: "):
            head = (git_dir / head[5:]).read_text(encoding="utf-8").strip()
        return head.lower() if re.fullmatch(r"[0-9a-fA-F]{40}", head) else ""
    except OSError:
        return ""


def _tree_digest(root: Path) -> str:
    """Digest installed plugin bytes so a git-less Codex cache stays verifiable."""
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if ".git" in path.parts or not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def _receipt_revision(
    config: Any, plugin_root: Path, version: str,
) -> str:
    path = managed_dir(config.codex.home_dir) / "install-receipt.json"
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if (
            receipt.get("path") == str(plugin_root)
            and receipt.get("version") == version
            and receipt.get("digest") == _tree_digest(plugin_root)
            and re.fullmatch(r"[0-9a-f]{40}", str(receipt.get("revision") or ""))
        ):
            return str(receipt["revision"])
    except (OSError, ValueError, TypeError):
        pass
    return ""


def _write_install_receipt(
    config: Any, plugin_root: Path, version: str,
) -> None:
    _atomic_write(
        managed_dir(config.codex.home_dir) / "install-receipt.json",
        json.dumps({
            "path": str(plugin_root),
            "version": version,
            "revision": config.langfuse.codex.revision,
            "digest": _tree_digest(plugin_root),
        }, indent=2) + "\n",
    )


def _credentials_configured(config: Any) -> bool:
    return bool(config.langfuse.public_key and config.langfuse.secret_key)


def installation_status(config: Any) -> dict[str, Any]:
    """Inspect plugin state without invoking Codex or the network."""
    plugin = config.langfuse.codex
    root = _runtime_root(config)
    marketplace_revision = _git_revision(_marketplace_root(config))
    try:
        manifest = json.loads(
            (root / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        manifest = {}
    version = str(manifest.get("version") or "")
    receipt_revision = _receipt_revision(config, root, version)
    revision_configured = bool(re.fullmatch(r"[0-9a-f]{40}", plugin.revision))
    installed = bool(
        revision_configured
        and manifest.get("name") == _PLUGIN
        and version == plugin.version
        and marketplace_revision == plugin.revision
        and receipt_revision == plugin.revision
        and _hook_entrypoint(root).is_file()
    )
    auth = _credentials_configured(config)
    requested = bool(plugin.enabled)
    error = _last_error
    if requested and not revision_configured:
        error = "verified plugin revision is not configured"
    elif requested and not auth:
        error = "Langfuse credentials are not configured"
    return {
        "requested": requested,
        "enabled": requested,
        "auto_install": bool(plugin.auto_install),
        "installed": installed,
        "ready": bool(requested and installed and auth),
        "auth_configured": auth,
        "auth_ok": None if auth else False,
        "version": version or None,
        "expected_version": plugin.version,
        "revision": marketplace_revision or receipt_revision or None,
        "expected_revision": plugin.revision or None,
        "path": str(root) if root.exists() else None,
        "auto_update": False,
        "max_chars": plugin.max_chars,
        "last_error": error,
    }


async def _run(*args: str, env: dict[str, str], timeout: float = 90.0) -> str:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
        start_new_session=True,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except BaseException as error:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
        await proc.wait()
        if isinstance(error, asyncio.TimeoutError):
            raise RuntimeError(
                f"{' '.join(args[:4])} timed out after {timeout:.0f}s"
            ) from None
        raise
    output = stdout.decode("utf-8", errors="replace")
    error_output = stderr.decode("utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(
            (error_output or output or f"exit {proc.returncode}").strip()[:2000]
        )
    return output


async def _remove_if_present(*args: str, env: dict[str, str]) -> None:
    try:
        await _run(*args, env=env)
    except RuntimeError as error:
        message = str(error).lower()
        if any(
            marker in message
            for marker in (
                "not installed", "not configured", "not found",
                "unknown marketplace",
            )
        ):
            return
        raise


async def ensure_installed(config: Any) -> dict[str, Any]:
    """Install or repair the exact reviewed snapshot, returning fail-open status."""
    global _last_error
    plugin = config.langfuse.codex
    status = installation_status(config)
    if not plugin.enabled or status["ready"]:
        return status
    if not re.fullmatch(r"[0-9a-f]{40}", plugin.revision):
        record_error("verified plugin revision is not configured", config)
        return installation_status(config)
    if not _credentials_configured(config):
        record_error("Langfuse credentials are not configured", config)
        return installation_status(config)
    if not plugin.auto_install:
        return status

    async with _INSTALL_LOCK:
        lock = await _acquire_file_lock(
            managed_dir(config.codex.home_dir) / "install.lock",
        )
        try:
            status = installation_status(config)
            if status["ready"]:
                return status
            # Credentials intentionally are not present during installation.
            env = {
                key: value for key, value in os.environ.items()
                if not key.startswith("LANGFUSE_")
                and key != "TRACE_TO_LANGFUSE"
            }
            env["CODEX_HOME"] = str(Path(config.codex.home_dir).expanduser())
            try:
                # Repair an old or partial installation without touching any
                # unrelated marketplace or plugin.
                await _remove_if_present(
                    config.codex.bin_path,
                    "plugin", "remove", f"{_PLUGIN}@{_MARKETPLACE}", "--json",
                    env=env,
                )
                await _remove_if_present(
                    config.codex.bin_path,
                    "plugin", "marketplace", "remove", _MARKETPLACE, "--json",
                    env=env,
                )
                await _run(
                    config.codex.bin_path,
                    "plugin", "marketplace", "add", plugin.repository,
                    "--ref", plugin.revision, "--json",
                    env=env,
                )
                if _git_revision(_marketplace_root(config)) != plugin.revision:
                    raise RuntimeError(
                        "Codex marketplace snapshot does not match the pinned revision"
                    )
                await _run(
                    config.codex.bin_path,
                    "plugin", "add", f"{_PLUGIN}@{_MARKETPLACE}", "--json",
                    env=env,
                )
                root = _runtime_root(config)
                try:
                    manifest = json.loads(
                        (root / ".codex-plugin" / "plugin.json").read_text(
                            encoding="utf-8"
                        )
                    )
                except (OSError, ValueError) as error:
                    raise RuntimeError("Codex plugin manifest is unavailable") from error
                if (
                    manifest.get("name") != _PLUGIN
                    or manifest.get("version") != plugin.version
                    or not _hook_entrypoint(root).is_file()
                ):
                    raise RuntimeError(
                        "Codex plugin runtime cache does not match the pinned version"
                    )
                _write_install_receipt(config, root, plugin.version)
                status = installation_status(config)
                if not status["installed"]:
                    raise RuntimeError(
                        "Codex installed a plugin that does not match the "
                        f"pinned version {plugin.version} and revision"
                    )
                _last_error = None
                logger.info(
                    "Installed managed Langfuse Codex plugin %s at %s",
                    status["version"], status["path"],
                )
            except Exception as error:
                record_error(error, config)
        finally:
            _release_file_lock(lock)
        return installation_status(config)


def child_env(config: Any) -> dict[str, str]:
    """Return tracing-only child variables; no values are written to disk."""
    lf = config.langfuse
    return {
        "TRACE_TO_LANGFUSE": "true",
        "LANGFUSE_PUBLIC_KEY": lf.public_key,
        "LANGFUSE_SECRET_KEY": lf.secret_key,
        "LANGFUSE_BASE_URL": lf.effective_base_url,
        "LANGFUSE_CODEX_MAX_CHARS": str(lf.codex.max_chars),
        "LANGFUSE_CODEX_FAIL_ON_ERROR": "false",
    }


def config_overrides() -> list[str]:
    return [
        # Current Codex releases expose the stable generic hook gate. Keep the
        # legacy plugin-specific gate for the plugin's minimum supported CLI.
        "features.hooks=true",
        "features.plugin_hooks=true",
        f'plugins."{_PLUGIN}@{_MARKETPLACE}".enabled=true',
    ]
