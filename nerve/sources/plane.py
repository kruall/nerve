"""Plane source — surfaces allowlisted work-item snapshots in the inbox.

The source intentionally emits *current work-item snapshots*, not imperative
instructions.  A stable work-item UUID is used as the record id, so Nerve's
source inbox re-surfaces the record only when its normalized content or
metadata changes.

Cursor semantics are versioned JSON with an independent watermark per project.
That prevents a healthy project from advancing the cursor of a project whose
request failed during the same run.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from typing import Any

from nerve.integrations.plane import PlaneAPIError, PlaneClient
from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)

_CURSOR_VERSION = 1
_MAX_DESCRIPTION_CHARS = 6_000


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def _html_to_text(value: str) -> str:
    parser = _HTMLTextExtractor()
    try:
        parser.feed(value or "")
        parser.close()
    except Exception:
        return unescape(value or "")
    return unescape(" ".join(part.strip() for part in parser.parts if part.strip()))


def _expanded_name(value: Any, *, fallback: str = "") -> str:
    if isinstance(value, dict):
        return str(
            value.get("display_name")
            or value.get("name")
            or value.get("id")
            or fallback
        )
    return str(value or fallback)


class PlaneSource(Source):
    """Monitor work-item changes in explicitly allowlisted Plane projects."""

    def __init__(
        self,
        config: dict[str, Any],
        *,
        client: PlaneClient | None = None,
    ):
        self._config = dict(config or {})
        self._workspace_slug = str(
            self._config.get("workspace_slug") or ""
        ).strip()
        self._projects = tuple(dict.fromkeys(
            str(project).strip()
            for project in (self._config.get("projects") or [])
            if str(project).strip()
        ))
        self._initial_backfill = bool(
            self._config.get("initial_backfill", False)
        )
        self._max_pages = max(
            1, int(self._config.get("max_pages_per_project", 20))
        )
        self._project_cache: dict[str, dict[str, Any]] = {}

        self.source_name = f"plane:{self._workspace_slug}"
        self._client = client or PlaneClient(
            base_url=str(self._config.get("base_url") or ""),
            workspace_slug=self._workspace_slug,
            api_key=str(self._config.get("api_key") or ""),
            project_ids=list(self._projects),
            timeout_seconds=float(self._config.get("timeout_seconds", 30.0)),
        )

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        limit = max(1, int(limit))
        cursor_state = self._decode_cursor(cursor)
        project_cursors = cursor_state["projects"]

        snapshots: dict[str, list[dict[str, Any]]] = {}
        errors: list[str] = []
        for project_id in self._projects:
            try:
                if project_id not in self._project_cache:
                    self._project_cache[project_id] = (
                        await self._client.get_project(project_id)
                    )
                snapshots[project_id] = await self._client.list_all_work_items(
                    project_id,
                    per_page=100,
                    max_pages=self._max_pages,
                )
            except Exception as exc:
                logger.warning(
                    "plane source: fetch failed for project %s: %s",
                    project_id,
                    exc,
                )
                errors.append(project_id)

        if errors and len(errors) == len(self._projects):
            raise PlaneAPIError(
                f"Plane fetch failed for all {len(errors)} configured project(s)"
            )

        # First run establishes a baseline by default, avoiding a full backlog
        # flood.  A caller may explicitly opt into initial_backfill.
        if cursor is None and not self._initial_backfill:
            for project_id, items in snapshots.items():
                project_cursors[project_id] = self._snapshot_watermark(items)
            return FetchResult(
                records=[],
                next_cursor=self._encode_cursor(project_cursors),
            )

        # A project may have been unavailable during the first baseline run.
        # When it recovers later, baseline that project independently instead
        # of flooding its entire backlog into an already-running source.
        newly_baselined: set[str] = set()
        if not self._initial_backfill:
            for project_id, items in snapshots.items():
                if project_id not in project_cursors:
                    project_cursors[project_id] = self._snapshot_watermark(items)
                    newly_baselined.add(project_id)

        candidates: list[tuple[str, str, dict[str, Any]]] = []
        for project_id, items in snapshots.items():
            if project_id in newly_baselined:
                continue
            watermark = project_cursors.get(project_id) or {
                "updated_at": "",
                "ids": [],
            }
            watermark_at = str(watermark.get("updated_at") or "")
            watermark_ids = {
                str(item_id) for item_id in (watermark.get("ids") or [])
            }
            for item in items:
                updated_at = str(
                    item.get("updated_at") or item.get("created_at") or ""
                )
                item_id = str(item.get("id") or "")
                if not item_id:
                    continue
                if updated_at > watermark_at or (
                    updated_at == watermark_at and item_id not in watermark_ids
                ):
                    candidates.append((updated_at, project_id, item))

        candidates.sort(key=lambda row: (row[0], row[1], str(row[2].get("id"))))
        selected = candidates[:limit]
        records = [
            self._to_record(project_id, item)
            for _, project_id, item in selected
        ]

        # Advance only projects represented in the selected batch.  If the
        # global limit leaves candidates queued for another project, its
        # watermark remains untouched so a later run cannot skip them.
        selected_by_project: dict[str, list[dict[str, Any]]] = {}
        for _, project_id, item in selected:
            selected_by_project.setdefault(project_id, []).append(item)
        for project_id, items in selected_by_project.items():
            previous = project_cursors.get(project_id) or {
                "updated_at": "",
                "ids": [],
            }
            project_cursors[project_id] = self._advance_watermark(previous, items)

        return FetchResult(
            records=records,
            next_cursor=self._encode_cursor(project_cursors),
            has_more=len(candidates) > len(selected),
        )

    def _to_record(
        self,
        project_id: str,
        item: dict[str, Any],
    ) -> SourceRecord:
        project = self._project_cache.get(project_id) or {}
        identifier = str(project.get("identifier") or project_id)
        project_name = str(project.get("name") or identifier)
        sequence_id = item.get("sequence_id")
        key = f"{identifier}-{sequence_id}" if sequence_id is not None else str(item.get("id"))
        title = str(item.get("name") or "Untitled work item")
        state = _expanded_name(item.get("state"))
        priority = str(item.get("priority") or "none")
        assignees = [
            _expanded_name(value)
            for value in (item.get("assignees") or [])
            if _expanded_name(value)
        ]
        labels = [
            _expanded_name(value)
            for value in (item.get("labels") or [])
            if _expanded_name(value)
        ]
        updated_at = str(
            item.get("updated_at")
            or item.get("created_at")
            or datetime.now(timezone.utc).isoformat()
        )
        raw_description = str(item.get("description_html") or "")
        description = str(item.get("description_stripped") or "").strip()
        if not description and raw_description:
            description = _html_to_text(raw_description).strip()
        if len(description) > _MAX_DESCRIPTION_CHARS:
            description = (
                description[:_MAX_DESCRIPTION_CHARS] + "\n[... truncated]"
            )

        content = [
            f"Project: {project_name} ({identifier})",
            f"Work item: {key}",
            f"Title: {title}",
            f"State: {state}" if state else None,
            f"Priority: {priority}",
            f"Assignees: {', '.join(assignees)}" if assignees else "Assignees: none",
            f"Labels: {', '.join(labels)}" if labels else "Labels: none",
            f"Updated: {updated_at}",
        ]
        if description:
            content.append(f"\n--- Description ---\n{description}")

        state_group = (
            item.get("state", {}).get("group")
            if isinstance(item.get("state"), dict)
            else ""
        )
        return SourceRecord(
            id=str(item.get("id")),
            source=self.source_name,
            record_type="plane_work_item",
            summary=f"[{identifier}] {key} updated: {title}",
            content="\n".join(part for part in content if part),
            raw_content=raw_description or None,
            timestamp=updated_at,
            metadata={
                "workspace_slug": self._workspace_slug,
                "project_id": project_id,
                "project_name": project_name,
                "project_identifier": identifier,
                "work_item_id": str(item.get("id")),
                "sequence_id": sequence_id,
                "state": state,
                "state_group": state_group,
                "priority": priority,
                "assignees": assignees,
                "labels": labels,
                "updated_at": updated_at,
            },
        )

    @staticmethod
    def _snapshot_watermark(items: list[dict[str, Any]]) -> dict[str, Any]:
        if not items:
            return {"updated_at": "", "ids": []}
        newest = max(
            str(item.get("updated_at") or item.get("created_at") or "")
            for item in items
        )
        ids = sorted(
            str(item.get("id"))
            for item in items
            if str(item.get("updated_at") or item.get("created_at") or "") == newest
            and item.get("id")
        )
        return {"updated_at": newest, "ids": ids}

    @staticmethod
    def _advance_watermark(
        previous: dict[str, Any],
        items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        newest = max(
            str(item.get("updated_at") or item.get("created_at") or "")
            for item in items
        )
        ids = {
            str(item.get("id"))
            for item in items
            if str(item.get("updated_at") or item.get("created_at") or "") == newest
            and item.get("id")
        }
        if newest == str(previous.get("updated_at") or ""):
            ids.update(str(value) for value in (previous.get("ids") or []))
        return {"updated_at": newest, "ids": sorted(ids)}

    @staticmethod
    def _decode_cursor(value: str | None) -> dict[str, Any]:
        if value is None:
            return {"v": _CURSOR_VERSION, "projects": {}}
        try:
            data = json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise PlaneAPIError("Invalid Plane source cursor JSON") from exc
        if not isinstance(data, dict) or data.get("v") != _CURSOR_VERSION:
            raise PlaneAPIError("Unsupported Plane source cursor version")
        projects = data.get("projects")
        if not isinstance(projects, dict):
            raise PlaneAPIError("Invalid Plane source cursor projects")
        return {"v": _CURSOR_VERSION, "projects": dict(projects)}

    @staticmethod
    def _encode_cursor(projects: dict[str, Any]) -> str:
        return json.dumps(
            {"v": _CURSOR_VERSION, "projects": projects},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    async def close(self) -> None:
        await self._client.close()
