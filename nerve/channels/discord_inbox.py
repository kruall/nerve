"""Shared Discord audit-forum inbox helpers."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

INBOX_TAG_NAME = "user-inbox"
_MAX_THREAD_TAGS = 5


def _tag_id(tag: Any) -> int:
    value = tag.get("id") if isinstance(tag, dict) else getattr(tag, "id")
    return int(value)


def _tag_name(tag: Any) -> str:
    value = tag.get("name") if isinstance(tag, dict) else getattr(tag, "name")
    return str(value or "")


def resolve_inbox_tag(forum: Any) -> Any | None:
    """Return the unique ``user-inbox`` forum tag, if configured."""
    matches = [
        tag
        for tag in list(getattr(forum, "available_tags", []) or [])
        if _tag_name(tag).casefold() == INBOX_TAG_NAME.casefold()
    ]
    if len(matches) == 1:
        return matches[0]
    if matches:
        logger.warning(
            "Discord audit forum has duplicate %s tags; inbox threads "
            "will remain untagged",
            INBOX_TAG_NAME,
        )
    else:
        logger.warning(
            "Discord audit forum is missing the %s tag; inbox threads "
            "will remain untagged",
            INBOX_TAG_NAME,
        )
    return None


def tags_with_inbox_tag(thread: Any, inbox_tag: Any | None) -> list[Any] | None:
    """Return current thread tags plus ``user-inbox`` when it can be added."""
    if inbox_tag is None:
        return None
    current = list(getattr(thread, "applied_tags", []) or [])
    inbox_tag_id = _tag_id(inbox_tag)
    if any(_tag_id(tag) == inbox_tag_id for tag in current):
        return None
    if len(current) >= _MAX_THREAD_TAGS:
        logger.warning(
            "Discord inbox thread %s already has five tags; cannot add %s",
            getattr(thread, "id", "unknown"),
            INBOX_TAG_NAME,
        )
        return None
    return [*current, inbox_tag]
