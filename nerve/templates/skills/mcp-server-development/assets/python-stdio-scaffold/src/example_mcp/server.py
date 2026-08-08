"""Dependency-free, bounded JSON-RPC stdio MCP scaffold."""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Any

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
MAX_MESSAGE_BYTES, MAX_TEXT_BYTES = 16_384, 1_024
SCHEMA = {"type": "object", "properties": {"text": {"type": "string", "maxLength": MAX_TEXT_BYTES}}, "required": ["text"], "additionalProperties": False}


def emit(value: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n"); sys.stdout.flush()


def error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


async def echo(arguments: Any) -> dict[str, Any]:
    if not isinstance(arguments, dict) or set(arguments) != {"text"} or not isinstance(arguments.get("text"), str):
        return {"content": [{"type": "text", "text": "invalid input"}], "isError": True}
    text = arguments["text"]
    if len(text.encode()) > MAX_TEXT_BYTES:
        return {"content": [{"type": "text", "text": "input exceeds limit"}], "isError": True}
    return {"content": [{"type": "text", "text": text}], "structuredContent": {"text": text}}


async def dispatch(request: dict[str, Any]) -> dict[str, Any] | None:
    method, request_id = request.get("method"), request.get("id")
    if method == "notifications/initialized": return None
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}, "serverInfo": {"name": "example-mcp", "version": "0.1.0"}}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": [{"name": "echo", "description": "Return bounded text.", "inputSchema": SCHEMA}]}}
    if method == "tools/call":
        params = request.get("params")
        if not isinstance(params, dict) or params.get("name") != "echo": return error(request_id, -32602, "unknown or invalid tool")
        try: result = await asyncio.wait_for(echo(params.get("arguments")), timeout=2)
        except TimeoutError: result = {"content": [{"type": "text", "text": "tool timed out"}], "isError": True}
        return {"jsonrpc": "2.0", "id": request_id, "result": result}
    return error(request_id, -32601, "method not found")


async def run() -> None:
    tasks: dict[Any, asyncio.Task] = {}

    async def respond(request_id: Any, task: asyncio.Task) -> None:
        try: response = await task
        except asyncio.CancelledError: response = error(request_id, -32800, "request cancelled")
        finally: tasks.pop(request_id, None)
        if response is not None: emit(response)

    while line := await asyncio.to_thread(sys.stdin.buffer.readline):
        if len(line) > MAX_MESSAGE_BYTES: emit(error(None, -32700, "message exceeds limit")); continue
        try: request = json.loads(line)
        except json.JSONDecodeError: emit(error(None, -32700, "invalid JSON")); continue
        if not isinstance(request, dict) or request.get("jsonrpc") != "2.0": emit(error(None, -32600, "invalid request")); continue
        if request.get("method") == "notifications/cancelled":
            params = request.get("params", {}); task = tasks.get(params.get("requestId") if isinstance(params, dict) else None)
            if task: task.cancel()
            continue
        request_id, task = request.get("id"), asyncio.create_task(dispatch(request))
        if request_id is not None: tasks[request_id] = task
        asyncio.create_task(respond(request_id, task))
    if tasks: await asyncio.gather(*tasks.values(), return_exceptions=True)


def main() -> None: asyncio.run(run())
if __name__ == "__main__": main()
