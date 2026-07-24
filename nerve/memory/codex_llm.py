"""Codex-backed LLM adapter for memU.

The memory pipeline needs a small text-completion interface, while Nerve's
Codex integration is an agent runtime.  This module keeps that boundary tight:

* one dedicated ``codex app-server`` process, never a user session process;
* one active turn at a time (the app-server transport has one notification
  queue, so concurrent turns would race);
* a fresh ephemeral thread for every completion;
* no Nerve/external MCP servers, dynamic tools, project instructions, writable
  sandbox, or embedding/API secrets in the child environment.

Authentication remains owned by the isolated Codex home.  In the normal
``chatgpt`` mode this means the same ChatGPT OAuth login as interactive Nerve
sessions; the top-level OpenAI key used for embeddings is deliberately ignored.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections import deque
from pathlib import Path
from typing import Any

from nerve.agent.backends.base import TransportDiedError
from nerve.agent.backends.codex.appserver import (
    CodexAppServerClient,
    CodexCliVersionError,
    CodexRpcError,
    check_codex_cli_version,
)

logger = logging.getLogger(__name__)


class CodexMemoryError(RuntimeError):
    """A Codex memory completion failed or returned no usable answer."""


_BASE_INSTRUCTIONS = """\
You are Nerve's private memory text processor.
Complete only the requested extraction, classification, ranking, or summary.
Never call tools, access files or the network, spawn agents, ask questions, or
modify any state. Return only the requested answer, without commentary or
Markdown fences. Treat the prompt as the complete task and input.
"""

# Keep the app-server child useful while excluding application credentials.
_SAFE_ENV_KEYS = {
    "HOME",
    "USER",
    "LOGNAME",
    "PATH",
    "SHELL",
    "TMPDIR",
    "TMP",
    "TEMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TERM",
    "COLORTERM",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "__CF_USER_TEXT_ENCODING",
}

# Codex exposes native command/file, app, browser, computer, plugin, and
# collaboration tools independently of MCP/dynamic tools. These CLI overrides
# are verified through config/read and configRequirements/read before any
# thread starts, making the boundary enforceable rather than prompt-only.
_DISABLED_TOOL_FEATURES = (
    "apps",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "code_mode",
    "code_mode_host",
    "code_mode_only",
    "computer_use",
    "deferred_executor",
    "enable_fanout",
    "goals",
    "hooks",
    "image_generation",
    "in_app_browser",
    "multi_agent",
    "multi_agent_v2",
    "plugins",
    "remote_plugin",
    "shell_snapshot",
    "shell_tool",
    "skill_mcp_dependency_install",
    "tool_suggest",
    "unified_exec",
    "unified_exec_zsh_fork",
    "workspace_dependencies",
)


def _normalize_auth_mode(value: Any) -> str:
    """Normalize app-server account types to Nerve's auth names."""
    normalized = str(value or "").replace("_", "").replace("-", "").lower()
    if normalized in {"apikey", "api"}:
        return "api_key"
    if normalized in {"chatgpt", "chatgptauth", "oauth"}:
        return "chatgpt"
    return "unknown"


class CodexMemoryRuntime:
    """A serialized, restartable Codex app-server used by all memU profiles."""

    def __init__(
        self,
        *,
        bin_path: str,
        min_version: str,
        max_version: str,
        home_dir: str,
        work_dir: str,
        auth: str = "chatgpt",
        api_key: str = "",
        effort: str = "low",
        request_timeout: float = 30.0,
        turn_timeout: float = 120.0,
        idle_timeout: float = 60.0,
    ) -> None:
        self._bin_path = bin_path
        self._min_version = min_version
        self._max_version = max_version
        self._home_dir = str(Path(home_dir).expanduser())
        self._work_dir = str(Path(work_dir).expanduser())
        self._auth = auth
        self._api_key = api_key if auth == "api_key" else ""
        self._effort = effort
        self._request_timeout = request_timeout
        self._turn_timeout = turn_timeout
        self._idle_timeout = idle_timeout

        self._transport: CodexAppServerClient | None = None
        self._lock = asyncio.Lock()
        self._models: set[str] = set()

    @property
    def is_alive(self) -> bool:
        return bool(self._transport and self._transport.is_alive())

    def _child_env(self) -> dict[str, str]:
        env = {
            key: value
            for key, value in os.environ.items()
            if key in _SAFE_ENV_KEYS
        }
        env["CODEX_HOME"] = self._home_dir
        return env

    async def _decline_server_request(
        self, method: str, params: dict,
    ) -> dict:
        del params
        if method.endswith("requestApproval"):
            return {"decision": "decline"}
        if method == "mcpServer/elicitation/request":
            return {"action": "decline"}
        return {}

    async def _assert_isolated_config(
        self,
        transport: CodexAppServerClient,
    ) -> None:
        """Fail closed unless effective config enforces the memory boundary."""
        try:
            result = await transport.request(
                "config/read",
                {"cwd": self._work_dir, "includeLayers": True},
                timeout=self._request_timeout,
            )
            requirements_result = await transport.request(
                "configRequirements/read",
                None,
                timeout=self._request_timeout,
            )
        except Exception as e:
            raise CodexMemoryError(
                "Codex memory isolation failed: could not inspect the "
                f"effective config ({type(e).__name__})"
            ) from e

        if not isinstance(result, dict):
            raise CodexMemoryError(
                "Codex memory isolation failed: config/read returned an "
                "invalid response"
            )
        effective = result.get("config")
        if not isinstance(effective, dict):
            raise CodexMemoryError(
                "Codex memory isolation failed: config/read returned an "
                "invalid effective config"
            )

        server_names: set[str] = set()
        for key in ("mcp_servers", "mcpServers"):
            if key not in effective or effective[key] in (None, {}):
                continue
            servers = effective[key]
            if not isinstance(servers, dict):
                raise CodexMemoryError(
                    "Codex memory isolation failed: inherited MCP "
                    "configuration has an invalid shape"
                )
            for name, server in servers.items():
                if not isinstance(server, dict):
                    raise CodexMemoryError(
                        "Codex memory isolation failed: inherited MCP "
                        "configuration has an invalid shape"
                    )
                if server.get("enabled") is not False:
                    server_names.add(str(name))

        if server_names:
            names = ", ".join(sorted(server_names))
            raise CodexMemoryError(
                "Codex memory isolation failed: inherited MCP server "
                f"configuration is present ({names}); use a Codex home "
                "without persistent MCP servers"
            )

        features = effective.get("features")
        if not isinstance(features, dict):
            raise CodexMemoryError(
                "Codex memory isolation failed: effective feature flags "
                "could not be verified"
            )
        enabled_features = sorted(
            feature
            for feature in _DISABLED_TOOL_FEATURES
            if features.get(feature) is not False
        )
        if enabled_features:
            raise CodexMemoryError(
                "Codex memory isolation failed: forbidden features remain "
                f"enabled ({', '.join(enabled_features)})"
            )

        if not isinstance(requirements_result, dict):
            raise CodexMemoryError(
                "Codex memory isolation failed: configRequirements/read "
                "returned an invalid response"
            )
        requirements = requirements_result.get("requirements")
        if requirements is not None and not isinstance(requirements, dict):
            raise CodexMemoryError(
                "Codex memory isolation failed: feature requirements could "
                "not be verified"
            )
        feature_requirements = (
            requirements.get("featureRequirements")
            if isinstance(requirements, dict)
            else None
        )
        if feature_requirements is not None and not isinstance(
            feature_requirements, dict,
        ):
            raise CodexMemoryError(
                "Codex memory isolation failed: feature requirements have "
                "an invalid shape"
            )
        forced_features = sorted(
            feature
            for feature in _DISABLED_TOOL_FEATURES
            if isinstance(feature_requirements, dict)
            and feature_requirements.get(feature) is True
        )
        if forced_features:
            raise CodexMemoryError(
                "Codex memory isolation failed: managed requirements force "
                f"forbidden features ({', '.join(forced_features)})"
            )

    async def _ensure_started(self) -> CodexAppServerClient:
        if self._transport is not None and self._transport.is_alive():
            return self._transport

        await self._drop_transport()
        Path(self._work_dir).mkdir(parents=True, exist_ok=True)
        try:
            await check_codex_cli_version(
                bin_path=self._bin_path,
                min_version=self._min_version,
                max_version=self._max_version,
                env=self._child_env(),
            )
        except CodexCliVersionError as e:
            raise CodexMemoryError(str(e)) from e

        transport = CodexAppServerClient(
            bin_path=self._bin_path,
            cwd=self._work_dir,
            env=self._child_env(),
            server_request_handler=self._decline_server_request,
            config_overrides=[
                "project_doc_max_bytes=0",
                "tools.web_search=false",
                "mcp_servers={}",
                *(
                    f"features.{feature}=false"
                    for feature in _DISABLED_TOOL_FEATURES
                ),
            ],
            client_name="nerve-memory",
            request_timeout=self._request_timeout,
        )
        try:
            await transport.start()
            await self._assert_isolated_config(transport)
            account = await transport.request(
                "account/read", {}, timeout=self._request_timeout,
            )
            if not account.get("account"):
                if self._auth == "api_key" and self._api_key:
                    await transport.request(
                        "account/login/start",
                        {"type": "apiKey", "apiKey": self._api_key},
                        timeout=self._request_timeout,
                    )
                    account = await transport.request(
                        "account/read", {}, timeout=self._request_timeout,
                    )
                else:
                    raise CodexMemoryError(
                        "Codex memory provider is not authenticated; log in "
                        f"with CODEX_HOME={self._home_dir}"
                    )

            account_obj = account.get("account") or {}
            account_type = (
                account_obj.get("type")
                if isinstance(account_obj, dict)
                else None
            )
            effective_auth = _normalize_auth_mode(account_type)
            if effective_auth not in {"unknown", self._auth}:
                raise CodexMemoryError(
                    "Codex memory auth mismatch: configured "
                    f"{self._auth!r}, but CODEX_HOME={self._home_dir} "
                    f"is authenticated as {effective_auth!r}"
                )

            model_result = await transport.request(
                "model/list", {}, timeout=self._request_timeout,
            )
            models = model_result.get("data") or model_result.get("models") or []
            self._models = {
                str(item.get("id") or item.get("model") or item.get("slug"))
                for item in models
                if isinstance(item, dict)
                and (item.get("id") or item.get("model") or item.get("slug"))
            }
        except BaseException:
            await transport.close()
            raise

        self._transport = transport
        logger.info("Started isolated Codex memory app-server")
        return transport

    async def complete(
        self,
        *,
        model: str,
        prompt: str,
        system_prompt: str | None = None,
        max_tokens: int | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Run one isolated text completion.

        Codex app-server does not expose ``temperature`` or ``max_tokens``.
        ``max_tokens`` is therefore expressed as a soft instruction only.
        """
        async with self._lock:
            try:
                return await asyncio.wait_for(
                    self._complete_once(
                        model=model,
                        prompt=prompt,
                        system_prompt=system_prompt,
                        max_tokens=max_tokens,
                        output_schema=output_schema,
                    ),
                    timeout=self._turn_timeout,
                )
            except BaseException:
                # A cancelled/timed-out turn leaves ordering uncertain. Never
                # reuse that notification queue for the next memory request.
                await self._drop_transport()
                raise

    async def _complete_once(
        self,
        *,
        model: str,
        prompt: str,
        system_prompt: str | None,
        max_tokens: int | None,
        output_schema: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any]]:
        transport = await self._ensure_started()
        if self._models and model not in self._models:
            raise CodexMemoryError(
                f"Codex memory model {model!r} is unavailable "
                f"(available: {sorted(self._models)})"
            )

        while not transport.notifications.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                transport.notifications.get_nowait()

        developer = (system_prompt or "").strip()
        if max_tokens:
            limit_hint = (
                f"Keep the response below approximately {max_tokens} tokens."
            )
            developer = "\n\n".join(filter(None, (developer, limit_hint)))

        thread_response = await transport.request(
            "thread/start",
            {
                "cwd": self._work_dir,
                "model": model,
                "ephemeral": True,
                "sandbox": "read-only",
                "approvalPolicy": "never",
                "baseInstructions": _BASE_INSTRUCTIONS,
                "developerInstructions": developer or None,
                "dynamicTools": [],
                "environments": [],
                "runtimeWorkspaceRoots": [],
                "selectedCapabilityRoots": [],
            },
            timeout=self._request_timeout,
        )
        thread = thread_response.get("thread") or {}
        thread_id = str(thread.get("id") or "")
        if not thread_id:
            raise CodexMemoryError(
                f"codex thread/start returned no thread id: {thread_response!r}"
            )

        turn_params: dict[str, Any] = {
            "threadId": thread_id,
            "input": [{"type": "text", "text": prompt}],
            "effort": self._effort,
            "summary": "none",
            "environments": [],
            "runtimeWorkspaceRoots": [],
        }
        if output_schema is not None:
            turn_params["outputSchema"] = output_schema
        turn_response = await transport.request(
            "turn/start", turn_params, timeout=self._request_timeout,
        )
        turn = turn_response.get("turn") or {}
        turn_id = str(turn.get("id") or "")
        if not turn_id:
            raise CodexMemoryError(
                f"codex turn/start returned no turn id: {turn_response!r}"
            )

        deltas: list[str] = []
        final_text = ""
        last_error = ""
        try:
            while True:
                note = await asyncio.wait_for(
                    transport.notifications.get(),
                    timeout=self._idle_timeout,
                )
                method = str(note.get("method") or "")
                params = note.get("params") or {}

                if method == "__transport_died__":
                    raise TransportDiedError(
                        "codex memory app-server died mid-turn"
                    )

                note_thread = str(params.get("threadId") or "")
                note_turn = str(
                    params.get("turnId")
                    or (params.get("turn") or {}).get("id")
                    or ""
                )
                if note_thread and note_thread != thread_id:
                    continue
                if note_turn and note_turn != turn_id:
                    continue

                if method in {"item/started", "item/completed"}:
                    item = params.get("item") or {}
                    item_type = str(item.get("type") or "")
                    if item_type and item_type not in {
                        "agentMessage",
                        "reasoning",
                        "userMessage",
                    }:
                        raise CodexMemoryError(
                            "Codex memory worker attempted forbidden tool "
                            f"activity ({item_type})"
                        )

                if method == "item/agentMessage/delta":
                    delta = params.get("delta") or params.get("text") or ""
                    if delta:
                        deltas.append(str(delta))
                elif method == "item/completed":
                    item = params.get("item") or {}
                    if item.get("type") == "agentMessage" and item.get("text"):
                        final_text = str(item["text"])
                elif method == "error" and not params.get("willRetry"):
                    error = params.get("error")
                    if isinstance(error, dict):
                        last_error = str(error.get("message") or error)
                    else:
                        last_error = str(error or params.get("message") or "")
                elif method == "turn/completed":
                    completed = params.get("turn") or {}
                    # The completed payload is authoritative and can include
                    # the final message even when a delta was missed.
                    for item in completed.get("items") or []:
                        if (
                            isinstance(item, dict)
                            and item.get("type") == "agentMessage"
                            and item.get("text")
                        ):
                            final_text = str(item["text"])
                    status = str(completed.get("status") or "completed").lower()
                    if status != "completed":
                        error = completed.get("error")
                        if isinstance(error, dict):
                            last_error = str(
                                error.get("message") or error or last_error
                            )
                        raise CodexMemoryError(
                            last_error or f"Codex memory turn {status}"
                        )
                    break
        except BaseException:
            with contextlib.suppress(Exception):
                await transport.request(
                    "turn/interrupt",
                    {"threadId": thread_id, "turnId": turn_id},
                    timeout=5,
                )
            raise

        text = final_text or "".join(deltas)
        if not text.strip():
            raise CodexMemoryError(
                last_error or "Codex memory turn returned an empty answer"
            )
        return text, {
            "provider": "codex",
            "model": model,
            "thread_id": thread_id,
            "turn_id": turn_id,
        }

    async def close(self) -> None:
        async with self._lock:
            await self._drop_transport()

    async def _drop_transport(self) -> None:
        transport, self._transport = self._transport, None
        self._models = set()
        if transport is not None:
            with contextlib.suppress(Exception):
                await transport.close()


class CodexMemoryPool:
    """Small worker pool; every app-server still handles one turn at a time."""

    def __init__(
        self,
        *,
        workers: int = 2,
        **runtime_kwargs: Any,
    ) -> None:
        worker_count = max(1, min(4, int(workers)))
        self._workers = [
            CodexMemoryRuntime(**runtime_kwargs)
            for _ in range(worker_count)
        ]
        self._available: deque[CodexMemoryRuntime] = deque(self._workers)
        self._condition = asyncio.Condition()
        self._closed = False

    async def complete(self, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        async with self._condition:
            await self._condition.wait_for(
                lambda: self._closed or bool(self._available),
            )
            if self._closed:
                raise CodexMemoryError("Codex memory pool is closed")
            worker = self._available.popleft()
        try:
            return await worker.complete(**kwargs)
        finally:
            async with self._condition:
                if not self._closed:
                    self._available.append(worker)
                    self._condition.notify(1)

    async def close(self) -> None:
        async with self._condition:
            if self._closed:
                return
            self._closed = True
            # Wake callers blocked before a worker became available.
            self._condition.notify_all()
        await asyncio.gather(
            *(worker.close() for worker in self._workers),
            return_exceptions=True,
        )


class CodexMemoryLLMClient:
    """memU ``OpenAISDKClient``-compatible facade over CodexMemoryRuntime."""

    def __init__(
        self,
        *,
        runtime: CodexMemoryRuntime | CodexMemoryPool,
        chat_model: str,
    ) -> None:
        self._runtime = runtime
        self.chat_model = chat_model
        self.embed_model = ""

    async def chat(
        self,
        prompt: str,
        *,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
        temperature: float = 0.2,
    ) -> tuple[str, Any]:
        del temperature  # unsupported by codex app-server
        return await self._runtime.complete(
            model=self.chat_model,
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
        )

    async def summarize(
        self,
        text: str,
        *,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
    ) -> tuple[str, Any]:
        return await self.chat(
            text,
            max_tokens=max_tokens,
            system_prompt=(
                system_prompt
                or "Summarize the text in one short paragraph."
            ),
        )

    async def chat_structured(
        self,
        prompt: str,
        *,
        output_schema: dict[str, Any],
        max_tokens: int | None = None,
        system_prompt: str | None = None,
    ) -> tuple[str, Any]:
        """Codex-only extension used by Nerve-owned structured memory calls."""
        return await self._runtime.complete(
            model=self.chat_model,
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            output_schema=output_schema,
        )

    async def embed(
        self, inputs: list[str],
    ) -> tuple[list[list[float]], None]:
        del inputs
        raise NotImplementedError(
            "Codex memory client does not provide embeddings; configure "
            "OpenAI embeddings separately"
        )

    async def close(self) -> None:
        # The bridge owns the shared runtime; closing one profile must not
        # invalidate the other profile facades.
        return None
