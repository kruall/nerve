"""Typed, bounded inputs for the reviewed remote SPIN operation."""
from __future__ import annotations
import hashlib
import re
from typing import Any

MAX_SOURCE_BYTES = 128 * 1024
_FORBIDDEN = (
    (re.compile(r"^\s*#\s*include", re.MULTILINE), "includes are not allowed"),
    (re.compile(r"\bc_(?:code|expr|decl|state|track)\b"), "embedded C is not allowed"),
)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def validate_request(
    *, model: Any, profile: Any, timeout_seconds: Any, memory_mb: Any,
    max_depth: Any, hash_bits: Any, property_name: Any = None,
) -> dict[str, Any]:
    if not isinstance(model, str) or not model or len(model.encode()) > MAX_SOURCE_BYTES:
        raise ValueError("Promela source must be non-empty UTF-8 text no larger than 128 KiB")
    for pattern, reason in _FORBIDDEN:
        if pattern.search(model):
            raise ValueError(reason)
    if profile not in {"exhaustive", "bitstate"}:
        raise ValueError("profile must be exhaustive or bitstate")
    for value, name, lower, upper in (
        (timeout_seconds, "timeout_seconds", 1, 300),
        (memory_mb, "memory_mb", 64, 8192),
        (max_depth, "max_depth", 100, 10_000_000),
        (hash_bits, "hash_bits", 10, 36),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
            raise ValueError(f"{name} must be between {lower} and {upper}")
    if property_name is not None and (
        not isinstance(property_name, str) or not _IDENTIFIER.fullmatch(property_name)
    ):
        raise ValueError("property_name must be a Promela identifier")
    return {
        "model": model,
        "profile": profile,
        "timeout_seconds": timeout_seconds,
        "memory_mb": memory_mb,
        "max_depth": max_depth,
        "hash_bits": hash_bits,
        "property_name": property_name,
        "model_sha256": hashlib.sha256(model.encode()).hexdigest(),
    }
