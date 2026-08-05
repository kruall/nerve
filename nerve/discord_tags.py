"""Approval-gated Discord forum-tag management.

Read operations call Discord immediately. Mutations are represented as a
small JSON action stored in an approval notification's metadata; the
dispatcher re-fetches Discord state and re-validates the configured project
forum before applying the action. No mutation helper is exposed directly as
an MCP tool.
"""

from __future__ import annotations

import json
import logging
import stat
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import quote

import httpx

from nerve.config import DiscordConfig, NerveConfig
from nerve.notifications import handlers as notification_handlers

logger = logging.getLogger(__name__)

DISCORD_FORUM_TAG_TARGET_KIND = "discord-forum-tag"
DISCORD_FORUM_TAG_METADATA_KEY = "discord_forum_tag_action"
DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND = "discord-project-task-completion"
DISCORD_PROJECT_TASK_RECOVERY_TARGET_KIND = "discord-project-task-recovery"
DISCORD_PROJECT_TASK_VALIDATION_TARGET_KIND = "discord-project-task-validation"
DISCORD_AUDIT_FORUM_PROJECT = "AUDIT"

# Project forums use one of these tags as the durable task state.  An
# untagged project thread is a newly-created task that has not yet passed
# triage; it is intentionally represented as ``new-task`` rather than by a
# synthetic Discord tag.
PROJECT_TASK_NEW = "new-task"
PROJECT_TASK_STATUSES = frozenset({
    "backlog",
    "ready-for-agent",
    "in-progress",
    "ready-for-user",
    "completed",
    "blocked",
    "cancelled",
})
_PROJECT_TASK_TRANSITIONS = {
    PROJECT_TASK_NEW: frozenset({"ready-for-agent", "blocked", "cancelled"}),
    "backlog": frozenset({"ready-for-agent", "blocked", "cancelled"}),
    "ready-for-agent": frozenset({"in-progress"}),
    "in-progress": frozenset({
        "ready-for-user", "backlog", "blocked", "cancelled",
    }),
    "ready-for-user": frozenset({"completed", "blocked", "cancelled"}),
    "blocked": frozenset({"ready-for-agent"}),
    "completed": frozenset(),
    "cancelled": frozenset(),
}

_API_BASE = "https://discord.com/api/v10"
_FORUM_CHANNEL_TYPES = frozenset({15, 16})
_THREAD_CHANNEL_TYPES = frozenset({10, 11, 12})
_DISCORD_MIN_REQUEST_INTERVAL_SECONDS = 0.5
_DISCORD_MAX_RATE_LIMIT_RETRIES = 2
_DISCORD_MAX_THROTTLE_DELAY_SECONDS = 30.0
_MUTATION_OPERATIONS = frozenset(
    {
        "create_tag",
        "update_tag",
        "delete_tag",
        "add_thread_tag",
        "remove_thread_tag",
        "replace_thread_tags",
    }
)
_TAG_FIELDS = ("id", "name", "moderated", "emoji_id", "emoji_name")


class DiscordForumTagError(ValueError):
    """A safe, user-facing failure from Discord forum-tag management."""


class DiscordProjectTaskStatusError(DiscordForumTagError):
    """A safe failure while moving a project task through its lifecycle."""


class _DiscordRequestThrottle:
    """Serialize REST calls and honor Discord rate-limit reset hints."""

    def __init__(
        self,
        *,
        min_interval: float = _DISCORD_MIN_REQUEST_INTERVAL_SECONDS,
        max_rate_limit_retries: int = _DISCORD_MAX_RATE_LIMIT_RETRIES,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._min_interval = max(0.0, min_interval)
        self._max_rate_limit_retries = max(0, max_rate_limit_retries)
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._last_request_at: float | None = None
        self._not_before = 0.0

    def request(self, fn: Callable[[], httpx.Response]) -> httpx.Response:
        """Run one request at a time, retrying bounded HTTP 429 responses."""
        with self._lock:
            retries = 0
            while True:
                self._wait_for_slot()
                try:
                    response = fn()
                finally:
                    self._last_request_at = self._clock()

                retry_after = self._rate_limit_delay(response)
                if retry_after is not None:
                    self._not_before = max(
                        self._not_before,
                        self._last_request_at + retry_after,
                    )

                if (
                    response.status_code != 429
                    or retries >= self._max_rate_limit_retries
                ):
                    return response

                retries += 1
                logger.warning(
                    "Discord API rate limited; retrying after %.3fs "
                    "(attempt %d/%d)",
                    retry_after or self._min_interval,
                    retries,
                    self._max_rate_limit_retries,
                )

    def _wait_for_slot(self) -> None:
        now = self._clock()
        earliest = self._not_before
        if self._last_request_at is not None:
            earliest = max(
                earliest,
                self._last_request_at + self._min_interval,
            )
        delay = earliest - now
        if delay > 0:
            self._sleep(delay)

    def _rate_limit_delay(self, response: httpx.Response) -> float | None:
        delay: float | None = None
        if response.status_code == 429:
            try:
                error = response.json()
            except ValueError:
                error = {}
            if isinstance(error, dict):
                delay = _positive_float(error.get("retry_after"))
            if delay is None:
                delay = _positive_float(response.headers.get("Retry-After"))
            if delay is None:
                delay = self._min_interval
        elif response.headers.get("X-RateLimit-Remaining") == "0":
            delay = _positive_float(
                response.headers.get("X-RateLimit-Reset-After")
            )

        if delay is None:
            return None
        return min(delay, _DISCORD_MAX_THROTTLE_DELAY_SECONDS)


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


_DISCORD_REQUEST_THROTTLE = _DiscordRequestThrottle()
_DISCORD_MUTATION_LOCK = threading.Lock()


def _load_token(config: DiscordConfig) -> str:
    if config.bot_token:
        token = config.bot_token.strip()
    else:
        path = config.bot_token_file
        if path is None:
            raise DiscordForumTagError("Discord bot token is not configured")
        try:
            token = path.read_text(encoding="utf-8").strip()
            mode = stat.S_IMODE(path.stat().st_mode)
        except OSError as exc:
            raise DiscordForumTagError(
                f"Cannot read Discord bot token file: {exc}"
            ) from exc
        if mode & 0o077:
            logger.warning(
                "Discord bot token file permissions are %03o; use 600",
                mode,
            )
    if not token or "\n" in token:
        raise DiscordForumTagError("Discord bot token file must contain one token")
    return token


def _discord_request(
    config: DiscordConfig,
    method: str,
    channel_id: int,
    payload: dict[str, Any] | None = None,
    *,
    path_suffix: str = "",
    audit_reason: str = "",
) -> dict[str, Any]:
    """Perform one bounded Discord channel request without leaking the token."""
    headers = {
        "Authorization": f"Bot {_load_token(config)}",
        "User-Agent": "Nerve (https://github.com/ClickHouse/nerve, 0.1)",
    }
    if audit_reason:
        headers["X-Audit-Log-Reason"] = quote(audit_reason[:512], safe="")
    try:
        response = _DISCORD_REQUEST_THROTTLE.request(
            lambda: httpx.request(
                method,
                f"{_API_BASE}/channels/{channel_id}{path_suffix}",
                headers=headers,
                json=payload,
                timeout=15.0,
            )
        )
    except httpx.HTTPError as exc:
        raise DiscordForumTagError(
            f"Discord API request failed: {type(exc).__name__}"
        ) from exc

    if response.is_error:
        code = ""
        message = response.reason_phrase or "request failed"
        try:
            error = response.json()
        except ValueError:
            error = {}
        if isinstance(error, dict):
            if error.get("code") is not None:
                code = f" code={error['code']}"
            if isinstance(error.get("message"), str):
                message = error["message"]
        raise DiscordForumTagError(
            f"Discord API returned HTTP {response.status_code}{code}: {message}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise DiscordForumTagError(
            "Discord API returned a non-JSON response"
        ) from exc
    if not isinstance(data, dict):
        raise DiscordForumTagError("Discord API returned an invalid channel response")
    return data


def _metadata_dict(notification: dict[str, Any]) -> dict[str, Any]:
    raw = notification.get("metadata")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _snowflake(value: Any, field: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise DiscordForumTagError(f"{field} must be a Discord snowflake") from exc
    if result <= 0:
        raise DiscordForumTagError(f"{field} must be a positive Discord snowflake")
    return result


def _canonical_project(
    config: DiscordConfig,
    project: str,
) -> tuple[str, int]:
    requested = project.strip().casefold()
    if not requested:
        raise DiscordForumTagError(
            "project is required outside a configured forum thread"
        )
    if (
        requested == DISCORD_AUDIT_FORUM_PROJECT.casefold()
        and config.audit_forum_id
    ):
        return DISCORD_AUDIT_FORUM_PROJECT, int(config.audit_forum_id)
    for name, forum_id in config.task_forums.items():
        if name.casefold() == requested:
            return name, int(forum_id)
    configured_names = list(config.task_forums)
    if config.audit_forum_id:
        configured_names.append(DISCORD_AUDIT_FORUM_PROJECT)
    configured = ", ".join(sorted(configured_names)) or "(none)"
    raise DiscordForumTagError(
        f"Unknown Discord project {project!r}; configured projects: {configured}"
    )


def _project_for_forum(
    config: DiscordConfig,
    forum_id: int,
) -> str:
    for project, configured_id in config.task_forums.items():
        if int(configured_id) == forum_id:
            return project
    if config.audit_forum_id and int(config.audit_forum_id) == forum_id:
        return DISCORD_AUDIT_FORUM_PROJECT
    raise DiscordForumTagError(
        "Discord forum is not configured as a project or audit forum"
    )


def _validate_guild(config: DiscordConfig, channel: dict[str, Any]) -> None:
    guild_id = _snowflake(channel.get("guild_id"), "guild_id")
    if guild_id != int(config.guild_id):
        raise DiscordForumTagError("Discord channel belongs to a different guild")


def _normalize_tag(tag: dict[str, Any]) -> dict[str, Any]:
    normalized = {field: tag.get(field) for field in _TAG_FIELDS if field in tag}
    if "id" in normalized and normalized["id"] is not None:
        normalized["id"] = str(_snowflake(normalized["id"], "tag id"))
    normalized["name"] = str(normalized.get("name") or "")
    normalized["moderated"] = bool(normalized.get("moderated", False))
    normalized.setdefault("emoji_id", None)
    normalized.setdefault("emoji_name", None)
    return normalized


def _forum_tags(forum: dict[str, Any]) -> list[dict[str, Any]]:
    raw = forum.get("available_tags") or []
    if not isinstance(raw, list):
        raise DiscordForumTagError("Discord forum returned invalid available_tags")
    return [_normalize_tag(tag) for tag in raw if isinstance(tag, dict)]


def _tag_by_id(
    tags: list[dict[str, Any]],
    tag_id: Any,
) -> dict[str, Any] | None:
    requested = str(_snowflake(tag_id, "tag_id"))
    return next((tag for tag in tags if tag.get("id") == requested), None)


def _validate_name(name: Any) -> str:
    value = str(name or "").strip()
    if not 1 <= len(value) <= 20:
        raise DiscordForumTagError(
            "Discord forum tag name must contain 1-20 characters"
        )
    return value


def _validate_emoji(
    emoji_id: Any,
    emoji_name: Any,
) -> tuple[str | None, str | None]:
    normalized_id = None
    if emoji_id not in (None, ""):
        normalized_id = str(_snowflake(emoji_id, "emoji_id"))
    normalized_name = None
    if emoji_name not in (None, ""):
        normalized_name = str(emoji_name)
    if normalized_id and normalized_name:
        raise DiscordForumTagError("At most one of emoji_id and emoji_name may be set")
    return normalized_id, normalized_name


@dataclass(frozen=True)
class PreparedDiscordTagAction:
    action: dict[str, Any]
    title: str
    body: str


class DiscordForumTagManager:
    """Read forum state, prepare actions, and execute approved actions."""

    def __init__(self, config: NerveConfig):
        self.config = config.discord
        if not self.config.enabled:
            raise DiscordForumTagError("Discord integration is disabled")
        if not self.config.task_forums and not self.config.audit_forum_id:
            raise DiscordForumTagError(
                "Discord has no configured project or audit forums"
            )

    def fetch_forum(
        self,
        project: str,
    ) -> tuple[str, int, dict[str, Any]]:
        canonical, forum_id = _canonical_project(self.config, project)
        forum = _discord_request(self.config, "GET", forum_id)
        _validate_guild(self.config, forum)
        if int(forum.get("type", -1)) not in _FORUM_CHANNEL_TYPES:
            raise DiscordForumTagError(
                f"Configured project {canonical} is not a Discord forum channel"
            )
        if _snowflake(forum.get("id"), "forum id") != forum_id:
            raise DiscordForumTagError("Discord returned the wrong forum channel")
        return canonical, forum_id, forum

    def fetch_thread(
        self,
        thread_id: Any,
    ) -> tuple[str, int, dict[str, Any]]:
        resolved_id = _snowflake(thread_id, "thread_id")
        thread = _discord_request(self.config, "GET", resolved_id)
        _validate_guild(self.config, thread)
        if int(thread.get("type", -1)) not in _THREAD_CHANNEL_TYPES:
            raise DiscordForumTagError("Discord target is not a thread")
        if _snowflake(thread.get("id"), "thread id") != resolved_id:
            raise DiscordForumTagError("Discord returned the wrong thread")
        forum_id = _snowflake(thread.get("parent_id"), "thread parent_id")
        project = _project_for_forum(self.config, forum_id)
        return project, forum_id, thread

    def inspect(
        self,
        *,
        project: str = "",
        thread_id: Any = None,
    ) -> dict[str, Any]:
        thread: dict[str, Any] | None = None
        if thread_id not in (None, ""):
            resolved_project, forum_id, thread = self.fetch_thread(thread_id)
            if project and project.casefold() != resolved_project.casefold():
                raise DiscordForumTagError(
                    "thread_id does not belong to the requested project"
                )
            project = resolved_project
            _, _, forum = self.fetch_forum(project)
        else:
            project, forum_id, forum = self.fetch_forum(project)

        tags = _forum_tags(forum)
        applied_ids = {
            str(_snowflake(value, "applied tag id"))
            for value in (thread or {}).get("applied_tags", []) or []
        }
        result = {
            "project": project,
            "forum_id": str(forum_id),
            "available_tags": [
                {**tag, "applied": tag.get("id") in applied_ids} for tag in tags
            ],
        }
        if thread is not None:
            result.update(
                {
                    "thread_id": str(_snowflake(thread.get("id"), "thread id")),
                    "thread_name": str(thread.get("name") or ""),
                    "applied_tag_ids": sorted(applied_ids),
                }
            )
        return result

    def prepare(
        self,
        operation: str,
        args: dict[str, Any],
        *,
        implicit_thread_id: Any = None,
    ) -> PreparedDiscordTagAction:
        if operation not in _MUTATION_OPERATIONS:
            raise DiscordForumTagError(f"Unsupported operation {operation!r}")

        thread_id = args.get("thread_id") or implicit_thread_id
        thread: dict[str, Any] | None = None
        if operation.endswith("_thread_tag") or operation == "replace_thread_tags":
            if not thread_id:
                raise DiscordForumTagError(f"thread_id is required for {operation}")
            project, forum_id, thread = self.fetch_thread(thread_id)
            requested_project = str(args.get("project") or "").strip()
            if requested_project and requested_project.casefold() != project.casefold():
                raise DiscordForumTagError(
                    "thread_id does not belong to the requested project"
                )
            _, _, forum = self.fetch_forum(project)
        else:
            project = str(args.get("project") or "").strip()
            if not project and thread_id:
                project, forum_id, _ = self.fetch_thread(thread_id)
                _, _, forum = self.fetch_forum(project)
            else:
                project, forum_id, forum = self.fetch_forum(project)

        tags = _forum_tags(forum)
        action: dict[str, Any] = {
            "version": 1,
            "operation": operation,
            "project": project,
            "forum_id": str(forum_id),
        }

        if operation == "create_tag":
            name = _validate_name(args.get("name"))
            emoji_id, emoji_name = _validate_emoji(
                args.get("emoji_id"),
                args.get("emoji_name"),
            )
            action["tag"] = {
                "name": name,
                "moderated": bool(args.get("moderated", False)),
                "emoji_id": emoji_id,
                "emoji_name": emoji_name,
            }
            title = f"Create Discord tag {name!r} in {project}"
            body = (
                f"Create forum tag **{name}** in project **{project}** "
                f"(forum `{forum_id}`)."
            )
        elif operation in {"update_tag", "delete_tag"}:
            tag = self._resolve_requested_tag(tags, args)
            action["tag_id"] = tag["id"]
            if operation == "delete_tag":
                title = f"Delete Discord tag {tag['name']!r} from {project}"
                body = (
                    f"Delete forum tag **{tag['name']}** (`{tag['id']}`) "
                    f"from project **{project}**. This removes the tag from "
                    "the forum's available tag set."
                )
            else:
                changes: dict[str, Any] = {}
                if args.get("name") not in (None, ""):
                    changes["name"] = _validate_name(args.get("name"))
                if "moderated" in args:
                    changes["moderated"] = bool(args.get("moderated"))
                if "emoji_id" in args or "emoji_name" in args:
                    emoji_id, emoji_name = _validate_emoji(
                        args.get("emoji_id"),
                        args.get("emoji_name"),
                    )
                    changes["emoji_id"] = emoji_id
                    changes["emoji_name"] = emoji_name
                if not changes:
                    raise DiscordForumTagError(
                        "update_tag requires name, moderated, emoji_id, or emoji_name"
                    )
                action["changes"] = changes
                title = f"Update Discord tag {tag['name']!r} in {project}"
                body = (
                    f"Update forum tag **{tag['name']}** (`{tag['id']}`) "
                    f"in project **{project}** with:\n"
                    f"```json\n{json.dumps(changes, ensure_ascii=False, indent=2)}\n```"
                )
        else:
            assert thread is not None
            resolved_thread_id = str(_snowflake(thread.get("id"), "thread id"))
            action["thread_id"] = resolved_thread_id
            thread_name = str(thread.get("name") or resolved_thread_id)
            if operation == "replace_thread_tags":
                requested = args.get("tag_ids") or []
                if isinstance(requested, str):
                    requested = [
                        value.strip() for value in requested.split(",") if value.strip()
                    ]
                if not isinstance(requested, list):
                    raise DiscordForumTagError(
                        "tag_ids must be a list or comma-separated string"
                    )
                tag_ids = list(
                    dict.fromkeys(
                        str(_snowflake(value, "tag id")) for value in requested
                    )
                )
                if len(tag_ids) > 5:
                    raise DiscordForumTagError(
                        "Discord threads support at most 5 applied tags"
                    )
                missing = [
                    value for value in tag_ids if _tag_by_id(tags, value) is None
                ]
                if missing:
                    raise DiscordForumTagError(
                        "Unknown tag IDs for this forum: " + ", ".join(missing)
                    )
                action["tag_ids"] = tag_ids
                names = [_tag_by_id(tags, value)["name"] for value in tag_ids]
                title = f"Replace tags on Discord thread {thread_name!r}"
                body = (
                    f"Replace all tags on **{thread_name}** (`{resolved_thread_id}`) "
                    f"with: {', '.join(names) if names else '(none)'}."
                )
            else:
                tag = self._resolve_requested_tag(tags, args)
                action["tag_id"] = tag["id"]
                verb = "Add" if operation == "add_thread_tag" else "Remove"
                preposition = "to" if operation == "add_thread_tag" else "from"
                title = f"{verb} Discord tag {tag['name']!r} {preposition} thread"
                body = (
                    f"{verb} tag **{tag['name']}** (`{tag['id']}`) "
                    f"{preposition} **{thread_name}** (`{resolved_thread_id}`) "
                    f"in project **{project}**."
                )

        return PreparedDiscordTagAction(action=action, title=title, body=body)

    @staticmethod
    def _resolve_requested_tag(
        tags: list[dict[str, Any]],
        args: dict[str, Any],
    ) -> dict[str, Any]:
        tag_id = args.get("tag_id")
        if tag_id not in (None, ""):
            tag = _tag_by_id(tags, tag_id)
            if tag is None:
                raise DiscordForumTagError(f"Unknown tag_id {tag_id!r} for this forum")
            return tag
        tag_name = str(args.get("tag_name") or "").strip().casefold()
        if not tag_name:
            raise DiscordForumTagError("tag_id or tag_name is required")
        matches = [
            tag for tag in tags if str(tag.get("name") or "").casefold() == tag_name
        ]
        if len(matches) != 1:
            raise DiscordForumTagError(
                f"Expected one tag named {args.get('tag_name')!r}; "
                f"found {len(matches)}"
            )
        return matches[0]

    def execute(
        self,
        action: dict[str, Any],
        *,
        audit_reason: str,
    ) -> dict[str, Any]:
        operation = str(action.get("operation") or "")
        if action.get("version") != 1 or operation not in _MUTATION_OPERATIONS:
            raise DiscordForumTagError("Invalid Discord forum-tag action payload")

        project, forum_id = _canonical_project(
            self.config,
            str(action.get("project") or ""),
        )
        if str(forum_id) != str(action.get("forum_id") or ""):
            raise DiscordForumTagError(
                "Configured forum changed after approval was requested"
            )
        _, _, forum = self.fetch_forum(project)
        tags = _forum_tags(forum)

        if operation == "create_tag":
            requested = action.get("tag")
            if not isinstance(requested, dict):
                raise DiscordForumTagError("create_tag payload is missing tag")
            name = _validate_name(requested.get("name"))
            existing = next(
                (
                    tag
                    for tag in tags
                    if str(tag.get("name") or "").casefold() == name.casefold()
                ),
                None,
            )
            if existing is not None:
                return {
                    "operation": operation,
                    "status": "already_exists",
                    "tag_id": existing["id"],
                }
            if len(tags) >= 20:
                raise DiscordForumTagError(
                    "Discord forums support at most 20 available tags"
                )
            emoji_id, emoji_name = _validate_emoji(
                requested.get("emoji_id"),
                requested.get("emoji_name"),
            )
            tags.append(
                {
                    "name": name,
                    "moderated": bool(requested.get("moderated", False)),
                    "emoji_id": emoji_id,
                    "emoji_name": emoji_name,
                }
            )
            updated = self._patch_forum_tags(
                forum_id,
                tags,
                audit_reason=audit_reason,
            )
            created = next(
                (
                    tag
                    for tag in _forum_tags(updated)
                    if str(tag.get("name") or "").casefold() == name.casefold()
                ),
                None,
            )
            return {
                "operation": operation,
                "status": "executed",
                "tag_id": (created or {}).get("id"),
            }

        if operation in {"update_tag", "delete_tag"}:
            tag_id = str(_snowflake(action.get("tag_id"), "tag_id"))
            index = next(
                (i for i, tag in enumerate(tags) if tag.get("id") == tag_id),
                None,
            )
            if index is None:
                status = "already_deleted" if operation == "delete_tag" else None
                if status:
                    return {"operation": operation, "status": status}
                raise DiscordForumTagError(
                    "Tag no longer exists; request a new approval"
                )
            if operation == "delete_tag":
                tags.pop(index)
            else:
                changes = action.get("changes")
                if not isinstance(changes, dict) or not changes:
                    raise DiscordForumTagError("update_tag payload is missing changes")
                updated_tag = dict(tags[index])
                if "name" in changes:
                    updated_tag["name"] = _validate_name(changes["name"])
                if "moderated" in changes:
                    updated_tag["moderated"] = bool(changes["moderated"])
                if "emoji_id" in changes or "emoji_name" in changes:
                    emoji_id, emoji_name = _validate_emoji(
                        changes.get("emoji_id"),
                        changes.get("emoji_name"),
                    )
                    updated_tag["emoji_id"] = emoji_id
                    updated_tag["emoji_name"] = emoji_name
                tags[index] = updated_tag
            self._patch_forum_tags(
                forum_id,
                tags,
                audit_reason=audit_reason,
            )
            return {"operation": operation, "status": "executed", "tag_id": tag_id}

        thread_id = _snowflake(action.get("thread_id"), "thread_id")
        resolved_project, resolved_forum_id, thread = self.fetch_thread(thread_id)
        if resolved_project != project or resolved_forum_id != forum_id:
            raise DiscordForumTagError(
                "Thread moved outside the approved project forum"
            )
        current_ids = list(
            dict.fromkeys(
                str(_snowflake(value, "applied tag id"))
                for value in thread.get("applied_tags", []) or []
            )
        )
        available_ids = {str(tag["id"]) for tag in tags}

        if operation == "replace_thread_tags":
            desired_ids = list(
                dict.fromkeys(
                    str(_snowflake(value, "tag id"))
                    for value in action.get("tag_ids", [])
                )
            )
        else:
            tag_id = str(_snowflake(action.get("tag_id"), "tag_id"))
            desired_ids = list(current_ids)
            if operation == "add_thread_tag" and tag_id not in desired_ids:
                desired_ids.append(tag_id)
            elif operation == "remove_thread_tag":
                desired_ids = [value for value in desired_ids if value != tag_id]

        if len(desired_ids) > 5:
            raise DiscordForumTagError("Discord threads support at most 5 applied tags")
        missing = [value for value in desired_ids if value not in available_ids]
        if missing:
            raise DiscordForumTagError(
                "Approved tags no longer exist: " + ", ".join(missing)
            )
        if desired_ids == current_ids:
            return {
                "operation": operation,
                "status": "already_applied",
                "thread_id": str(thread_id),
            }
        _discord_request(
            self.config,
            "PATCH",
            thread_id,
            {"applied_tags": desired_ids},
            audit_reason=audit_reason,
        )
        return {
            "operation": operation,
            "status": "executed",
            "thread_id": str(thread_id),
            "applied_tag_ids": desired_ids,
        }

    def _patch_forum_tags(
        self,
        forum_id: int,
        tags: list[dict[str, Any]],
        *,
        audit_reason: str,
    ) -> dict[str, Any]:
        payload_tags: list[dict[str, Any]] = []
        for tag in tags:
            payload = {
                key: tag.get(key)
                for key in _TAG_FIELDS
                if key != "id" or tag.get(key) is not None
            }
            payload_tags.append(payload)
        return _discord_request(
            self.config,
            "PATCH",
            forum_id,
            {"available_tags": payload_tags},
            audit_reason=audit_reason,
        )


def _project_task_state(
    manager: DiscordForumTagManager,
    thread_id: Any,
) -> tuple[str, dict[str, Any], dict[str, str], list[str], str]:
    """Read and validate one project task while the mutation lock is held."""
    project, forum_id, thread = manager.fetch_thread(thread_id)
    if project == DISCORD_AUDIT_FORUM_PROJECT:
        raise DiscordProjectTaskStatusError(
            "Task lifecycle statuses apply only to configured project forums"
        )
    _, resolved_forum_id, forum = manager.fetch_forum(project)
    if resolved_forum_id != forum_id:
        raise DiscordProjectTaskStatusError(
            "Thread moved outside its configured project forum"
        )

    available = _forum_tags(forum)
    by_name: dict[str, list[dict[str, Any]]] = {}
    for tag in available:
        by_name.setdefault(str(tag.get("name") or "").casefold(), []).append(tag)
    missing_or_ambiguous = [
        status
        for status in sorted(PROJECT_TASK_STATUSES)
        if len(by_name.get(status, [])) != 1
    ]
    if missing_or_ambiguous:
        raise DiscordProjectTaskStatusError(
            "Project forum must contain exactly one tag for every task status; "
            "invalid: " + ", ".join(missing_or_ambiguous)
        )

    status_tag_ids = {
        str(by_name[status][0]["id"]): status
        for status in PROJECT_TASK_STATUSES
    }
    current_ids = list(dict.fromkeys(
        str(_snowflake(value, "applied tag id"))
        for value in thread.get("applied_tags", []) or []
    ))
    current_statuses = [
        status_tag_ids[tag_id]
        for tag_id in current_ids
        if tag_id in status_tag_ids
    ]
    if len(current_statuses) > 1:
        raise DiscordProjectTaskStatusError(
            "Project thread has more than one task-status tag: "
            + ", ".join(sorted(current_statuses))
        )
    current = current_statuses[0] if current_statuses else PROJECT_TASK_NEW
    return project, thread, status_tag_ids, current_ids, current


def _apply_project_task_status(
    manager: DiscordForumTagManager,
    *,
    project: str,
    thread: dict[str, Any],
    status_tag_ids: dict[str, str],
    current_ids: list[str],
    current: str,
    target: str,
    audit_reason: str,
    allow_reopen: bool = False,
    allow_verified_blocked_handoff: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    thread_id = str(_snowflake(thread.get("id"), "thread id"))
    if current == target:
        return {
            "status": "already_applied",
            "project": project,
            "thread_id": thread_id,
            "previous_status": current,
            "current_status": target,
        }
    if force and not any(tag_id in status_tag_ids for tag_id in current_ids):
        raise DiscordProjectTaskStatusError(
            "Project thread has no task-status tag; force requires one "
            "unambiguous current lifecycle tag"
        )
    if not force and target not in _PROJECT_TASK_TRANSITIONS[current] and not (
        allow_reopen
        and current == "ready-for-user"
        and target == "in-progress"
    ) and not (
        allow_verified_blocked_handoff
        and current == "blocked"
        and target == "ready-for-user"
    ):
        allowed = ", ".join(sorted(_PROJECT_TASK_TRANSITIONS[current])) or "(none)"
        raise DiscordProjectTaskStatusError(
            f"Invalid project task transition {current} -> {target}; "
            f"allowed next statuses: {allowed}"
        )

    desired_ids = [
        tag_id for tag_id in current_ids if tag_id not in status_tag_ids
    ]
    target_tag_id = next(
        (tag_id for tag_id, status in status_tag_ids.items() if status == target),
        None,
    )
    if target_tag_id is None:
        raise DiscordProjectTaskStatusError(
            f"Project forum has no unique tag for status {target}"
        )
    desired_ids.append(target_tag_id)
    if len(desired_ids) > 5:
        raise DiscordProjectTaskStatusError(
            "Discord threads support at most 5 applied tags"
        )
    _discord_request(
        manager.config,
        "PATCH",
        int(thread_id),
        {"applied_tags": desired_ids},
        audit_reason=audit_reason,
    )
    return {
        "status": "executed",
        "project": project,
        "thread_id": thread_id,
        "previous_status": current,
        "current_status": target,
        "applied_tag_ids": desired_ids,
    }


def transition_project_task_status(
    config: NerveConfig,
    *,
    thread_id: Any,
    target_status: str,
    audit_reason: str,
    allow_verified_blocked_handoff: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    """Apply one validated lifecycle transition to a project-thread tag.

    ``allow_verified_blocked_handoff`` is reserved for the recovery approval
    dispatcher after it has authenticated the Discord actor and decision.
    Agents using the ordinary lifecycle tool cannot bypass a blocked task.
    ``force`` is reserved for authenticated guild commands; it bypasses only
    the transition graph after the project thread and its lifecycle tags have
    been fully validated.
    """
    target = str(target_status or "").strip().casefold()
    if target not in PROJECT_TASK_STATUSES:
        valid = ", ".join(sorted(PROJECT_TASK_STATUSES))
        raise DiscordProjectTaskStatusError(
            f"Unknown project task status {target_status!r}; expected one of: {valid}"
        )

    manager = DiscordForumTagManager(config)
    with _DISCORD_MUTATION_LOCK:
        project, thread, status_tag_ids, current_ids, current = (
            _project_task_state(manager, thread_id)
        )
        return _apply_project_task_status(
            manager,
            project=project,
            thread=thread,
            status_tag_ids=status_tag_ids,
            current_ids=current_ids,
            current=current,
            target=target,
            audit_reason=audit_reason,
            allow_verified_blocked_handoff=allow_verified_blocked_handoff,
            force=force,
        )


def resume_project_task(
    config: NerveConfig,
    *,
    thread_id: Any,
    audit_reason: str,
) -> dict[str, Any]:
    """Resume only a task handed back by the user for more implementation."""
    manager = DiscordForumTagManager(config)
    with _DISCORD_MUTATION_LOCK:
        project, thread, status_tag_ids, current_ids, current = (
            _project_task_state(manager, thread_id)
        )
        if current != "ready-for-user":
            return {
                "status": "no_op",
                "project": project,
                "thread_id": str(_snowflake(thread.get("id"), "thread id")),
                "previous_status": current,
                "current_status": current,
            }
        return _apply_project_task_status(
            manager,
            project=project,
            thread=thread,
            status_tag_ids=status_tag_ids,
            current_ids=current_ids,
            current=current,
            target="in-progress",
            audit_reason=audit_reason,
            allow_reopen=True,
        )


def complete_project_task(
    config: NerveConfig,
    *,
    thread_id: Any,
    audit_reason: str,
    force: bool = False,
) -> dict[str, Any]:
    """Mark a project task complete, then archive its Discord thread.

    Discord has no transaction spanning its tag and archive endpoints. The
    status update intentionally happens first: an archive failure leaves an
    accurately completed, visible task rather than an archived task that still
    appears ready for user review. Repeating the operation is safe because a
    completed tag is accepted idempotently and archiving is idempotent.
    """
    manager = DiscordForumTagManager(config)
    with _DISCORD_MUTATION_LOCK:
        project, thread, status_tag_ids, current_ids, current = (
            _project_task_state(manager, thread_id)
        )
        if not force and current not in {"ready-for-user", "completed"}:
            raise DiscordProjectTaskStatusError(
                "Project task can be closed only from ready-for-user; "
                f"current status is {current}"
            )
        status_result = _apply_project_task_status(
            manager,
            project=project,
            thread=thread,
            status_tag_ids=status_tag_ids,
            current_ids=current_ids,
            current=current,
            target="completed",
            audit_reason=audit_reason,
            force=force,
        )
        if not bool(thread.get("archived")):
            _discord_request(
                config.discord,
                "PATCH",
                _snowflake(status_result["thread_id"], "thread id"),
                {"archived": True},
                audit_reason=audit_reason,
            )
        return {**status_result, "archived": True}


def dispatch_discord_project_task_completion(
    notification: dict[str, Any],
    target_id: str,
    decision: str,
    config: NerveConfig | None,
) -> notification_handlers.DispatchResult:
    """Complete a ready-for-user task after an approval button is clicked.

    The dispatcher has no continuation: this is a mechanical Discord state
    transition, so confirmation must not spend another model turn.
    """
    base_event: dict[str, Any] = {
        "event": "approval-acted",
        "notification_id": notification.get("id", ""),
        "target_kind": DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND,
        "target_id": target_id,
        "decision": decision,
    }
    if decision == "decline":
        return notification_handlers.DispatchResult(
            ok=True,
            audit_event={**base_event, "ok": True, "executed": False},
        )
    if decision != "approve":
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": f"unsupported decision: {decision}",
            },
        )
    if config is None:
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": "Nerve config unavailable",
            },
        )

    try:
        result = complete_project_task(
            config,
            thread_id=target_id,
            audit_reason=(
                "Nerve task completion approval "
                f"{notification.get('id', '')}"
            ),
        )
    except DiscordForumTagError as exc:
        logger.warning("Approved project task completion failed: %s", exc)
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": str(exc),
            },
        )
    return notification_handlers.DispatchResult(
        ok=True,
        audit_event={
            **base_event,
            "ok": True,
            "executed": True,
            "project": result["project"],
            "archived": True,
        },
    )


def dispatch_discord_project_task_recovery(
    notification: dict[str, Any],
    target_id: str,
    decision: str,
    config: NerveConfig | None,
    *,
    answered_by: str = "",
) -> notification_handlers.DispatchResult:
    """Release a blocked task and mention the user who chose the action."""
    base_event: dict[str, Any] = {
        "event": "approval-acted",
        "notification_id": notification.get("id", ""),
        "target_kind": DISCORD_PROJECT_TASK_RECOVERY_TARGET_KIND,
        "target_id": target_id,
        "decision": decision,
    }
    if decision not in {"cancelled", "backlog", "ready-for-user"}:
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": f"unsupported decision: {decision}",
            },
        )
    if config is None:
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": "Nerve config unavailable",
            },
        )
    actor_prefix, separator, actor_value = str(answered_by).partition(":")
    if actor_prefix != "discord" or not separator:
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": "recovery action has no Discord user actor",
            },
        )
    try:
        actor_id = _snowflake(actor_value, "Discord actor id")
    except DiscordForumTagError as exc:
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": str(exc),
            },
        )

    try:
        result = transition_project_task_status(
            config,
            thread_id=target_id,
            target_status=decision,
            audit_reason=(
                "Nerve blocked Discord task recovery approval "
                f"{notification.get('id', '')}"
            ),
            allow_verified_blocked_handoff=True,
        )
        _discord_request(
            config.discord,
            "POST",
            _snowflake(target_id, "thread id"),
            {
                "content": (
                    f"<@{actor_id}> Task runner claim released: "
                    f"`{decision}`."
                ),
                "allowed_mentions": {"users": [str(actor_id)]},
            },
            path_suffix="/messages",
            audit_reason="Nerve task recovery user mention",
        )
    except DiscordForumTagError as exc:
        logger.warning("Blocked project task recovery failed: %s", exc)
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": str(exc),
            },
        )
    return notification_handlers.DispatchResult(
        ok=True,
        audit_event={
            **base_event,
            "ok": True,
            "executed": True,
            "current_status": result["current_status"],
            "user_mentioned": True,
        },
    )


def dispatch_discord_forum_tag_action(
    notification: dict[str, Any],
    target_id: str,
    decision: str,
    config: NerveConfig | None,
) -> notification_handlers.DispatchResult:
    """Execute a queued Discord tag mutation only after explicit approval."""
    base_event: dict[str, Any] = {
        "event": "approval-acted",
        "notification_id": notification.get("id", ""),
        "target_kind": DISCORD_FORUM_TAG_TARGET_KIND,
        "target_id": target_id,
        "decision": decision,
    }
    feedback = notification_handlers._decision_feedback(notification)
    if feedback:
        base_event["feedback"] = feedback
    if decision == "decline":
        return notification_handlers.DispatchResult(
            ok=True,
            audit_event={**base_event, "ok": True, "executed": False},
        )
    if decision != "approve":
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": f"unsupported decision: {decision}",
            },
        )
    if config is None:
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": "Nerve config unavailable",
            },
        )

    metadata = _metadata_dict(notification)
    action = metadata.get(DISCORD_FORUM_TAG_METADATA_KEY)
    if not isinstance(action, dict):
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": "approval is missing Discord action metadata",
            },
        )
    if str(action.get("action_id") or "") != target_id:
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "error": "Discord action target_id mismatch",
            },
        )

    try:
        with _DISCORD_MUTATION_LOCK:
            result = DiscordForumTagManager(config).execute(
                action,
                audit_reason=f"Nerve approval {notification.get('id', '')}",
            )
    except DiscordForumTagError as exc:
        logger.warning("Approved Discord forum-tag action failed: %s", exc)
        return notification_handlers.DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "executed": False,
                "operation": action.get("operation"),
                "project": action.get("project"),
                "error": str(exc),
            },
        )
    return notification_handlers.DispatchResult(
        ok=True,
        audit_event={
            **base_event,
            "ok": True,
            "executed": True,
            "operation": action.get("operation"),
            "project": action.get("project"),
            "result": result,
        },
    )


notification_handlers.register(
    DISCORD_FORUM_TAG_TARGET_KIND,
    dispatch_discord_forum_tag_action,
)
notification_handlers.register(
    DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND,
    dispatch_discord_project_task_completion,
)
