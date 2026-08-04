"""OpenCode backend using one private headless server per Nerve session."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
from pathlib import Path
from typing import Any, AsyncIterator

import httpx

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import (
    AgentClient, BackendCapabilities, BackendError, SessionSpec,
    TransportDiedError, TurnInput,
)

logger = logging.getLogger(__name__)


class OpenCodeBackend:
    name = "opencode"
    capabilities = BackendCapabilities(
        cost_is_cumulative=False, supports_idle_stream=False,
        supports_cache_ttl=False, interactive_builtins=False,
        reports_context_window=False,
    )

    def __init__(self, deps: Any):
        self._deps = deps
        self.config = deps.config
        self.opencode = deps.config.opencode

    def default_model(self, source: str) -> str:
        if source in ("cron", "hook") and self.opencode.cron_model:
            return self.opencode.cron_model
        return self.opencode.model

    def excluded_tools(self) -> set[str]:
        return set()

    async def validate_model(self, model: str) -> None:
        # Availability depends on OpenCode's authenticated providers.
        return None

    def validate_resume_target(self, native_id: str, cwd: str) -> bool:
        return True

    async def create_client(self, spec: SessionSpec) -> "OpenCodeClient":
        client = OpenCodeClient(self, spec)
        try:
            await client.connect()
            return client
        except BaseException:
            await client.disconnect()
            raise

    def runtime_env(self, spec: SessionSpec) -> dict[str, str]:
        root = Path(os.path.expanduser(self.opencode.home_dir)) / spec.session_id
        root.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.update({
            "OPENCODE_CONFIG_DIR": str(root / "config"),
            "XDG_CACHE_HOME": str(root / "cache"),
            "XDG_DATA_HOME": str(root / "data"),
        })
        port = self._deps.gateway_port()
        token = self._deps.mint_session_token(spec.session_id) if (
            port is not None and self._deps.mint_session_token
        ) else ""
        if token:
            env["NERVE_MCP_TOKEN"] = token
        override: dict[str, Any] = {"permission": "allow"}
        if port is not None:
            override["mcp"] = {"servers": {"nerve": {
                "type": "remote", "url": f"http://127.0.0.1:{port}/mcp/v1/",
                "headers": {"Authorization": "Bearer {env:NERVE_MCP_TOKEN}"},
                "oauth": False, "codemode": False,
            }}}
        env["OPENCODE_CONFIG_CONTENT"] = json.dumps(override)
        return env


class OpenCodeClient(AgentClient):
    def __init__(self, backend: OpenCodeBackend, spec: SessionSpec):
        self._backend = backend
        self._spec = spec
        self._proc: asyncio.subprocess.Process | None = None
        self._http: httpx.AsyncClient | None = None
        self._native_session_id: str | None = None
        self._turn_task: asyncio.Task[list[ev.AgentEvent]] | None = None
        self.resume_dropped = False

    @property
    def native_session_id(self) -> str | None:
        return self._native_session_id

    @staticmethod
    def _reserve_port() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    async def connect(self) -> None:
        port = self._reserve_port()
        base_url = f"http://127.0.0.1:{port}"
        self._proc = await asyncio.create_subprocess_exec(
            self._backend.opencode.bin_path, "serve", "--hostname", "127.0.0.1",
            "--port", str(port), cwd=self._spec.cwd,
            env=self._backend.runtime_env(self._spec),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        self._http = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(self._backend.opencode.request_timeout_seconds),
        )
        deadline = asyncio.get_running_loop().time() + self._backend.opencode.startup_timeout_seconds
        while True:
            if self._proc.returncode is not None:
                raise BackendError(f"OpenCode server exited with code {self._proc.returncode}")
            try:
                if (await self._http.get("/global/health", timeout=1)).is_success:
                    break
            except httpx.HTTPError:
                pass
            if asyncio.get_running_loop().time() >= deadline:
                raise BackendError("Timed out waiting for OpenCode server health")
            await asyncio.sleep(0.1)
        if self._spec.resume_native_id:
            existing = await self._request("GET", f"/session/{self._spec.resume_native_id}", allow_missing=True)
            if existing is not None:
                self._native_session_id = self._spec.resume_native_id
                return
            self.resume_dropped = True
        created = await self._request("POST", "/session", json={})
        session_id = created.get("id") if isinstance(created, dict) else None
        if not isinstance(session_id, str) or not session_id:
            raise BackendError("OpenCode returned a session without an id")
        self._native_session_id = session_id

    async def _request(self, method: str, path: str, *, json: dict | None = None, allow_missing: bool = False) -> dict | list | None:
        if self._http is None:
            raise TransportDiedError("OpenCode HTTP client is not connected")
        try:
            response = await self._http.request(method, path, json=json)
        except httpx.HTTPError as error:
            raise TransportDiedError(f"OpenCode HTTP request failed: {error}") from error
        if allow_missing and response.status_code == 404:
            return None
        if not response.is_success:
            raise BackendError(f"OpenCode {method} {path} failed ({response.status_code}): {response.text[:1000]}")
        if response.status_code == 204 or not response.content:
            return None
        data = response.json()
        if not isinstance(data, (dict, list)):
            raise BackendError(f"OpenCode {method} {path} returned non-object JSON")
        return data

    async def start_turn(self, turn: TurnInput) -> None:
        if self._turn_task and not self._turn_task.done():
            raise BackendError("OpenCode turn is already running")
        if not self._native_session_id:
            raise TransportDiedError("OpenCode session was not created")
        self._turn_task = asyncio.create_task(self._run_turn(turn))

    async def _run_turn(self, turn: TurnInput) -> list[ev.AgentEvent]:
        assert self._native_session_id
        body: dict[str, Any] = {"parts": [{"type": "text", "text": turn.text}]}
        if self._spec.model:
            body["model"] = self._spec.model
        if self._spec.system_prompt:
            body["system"] = self._spec.system_prompt
        response = await self._request("POST", f"/session/{self._native_session_id}/message", json=body)
        parts = response.get("parts", []) if isinstance(response, dict) else []
        events: list[ev.AgentEvent] = []
        for part in parts if isinstance(parts, list) else []:
            if isinstance(part, dict):
                text = part.get("text") or part.get("content")
                if isinstance(text, str) and text:
                    events.append(ev.TextDelta(text=text))
        info = response.get("info", {}) if isinstance(response, dict) else {}
        model = (info.get("modelID") or info.get("model")) if isinstance(info, dict) else None
        if isinstance(model, str) and model:
            events.insert(0, ev.ModelObserved(model=model))
        events.append(ev.TurnCompleted(status="completed", native_session_id=self._native_session_id))
        return events

    async def steer(self, turn: TurnInput) -> bool:
        return False

    async def receive_turn(self) -> AsyncIterator[ev.AgentEvent]:
        if self._turn_task is None:
            raise BackendError("OpenCode turn was not started")
        try:
            events = await self._turn_task
        except asyncio.CancelledError:
            yield ev.TurnCompleted(status="interrupted", native_session_id=self._native_session_id)
            return
        except Exception as error:
            yield ev.TurnCompleted(status="failed", error=str(error), native_session_id=self._native_session_id)
            return
        for event in events:
            yield event

    async def interrupt(self) -> None:
        if self._native_session_id:
            try:
                await self._request("POST", f"/session/{self._native_session_id}/abort")
            except BackendError:
                logger.warning("OpenCode abort failed", exc_info=True)
        if self._turn_task and not self._turn_task.done():
            self._turn_task.cancel()

    async def disconnect(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        if self._proc is not None and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                self._proc.kill()
                await self._proc.wait()
        self._proc = None

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    def buffer_used(self) -> int:
        return 0
