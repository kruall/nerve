"""Minimal, allowlisted async client for the Plane REST API.

The client is intentionally narrower than Plane's full API. It provides the
read surface shared by the Plane source and Nerve's first-party MCP tools while
enforcing the configured workspace and project allowlist in one place.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

import httpx


class PlaneAPIError(RuntimeError):
    """A redacted Plane API failure safe to surface in logs/tool results."""


@dataclass(frozen=True)
class PlanePage:
    """One cursor-paginated Plane response."""

    results: list[dict[str, Any]]
    next_cursor: str | None = None
    has_more: bool = False


def _normalize_base_url(value: str) -> str:
    """Return an origin-like base URL without Plane API suffixes."""
    base_url = str(value or "").strip().rstrip("/")
    for suffix in ("/api/v1", "/api"):
        if base_url.endswith(suffix):
            base_url = base_url[: -len(suffix)]
            break

    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("plane base_url must be an absolute http(s) URL")
    if parsed.username or parsed.password:
        raise ValueError("plane base_url must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("plane base_url must not contain query or fragment")
    return base_url


def _path_segment(value: str, *, resource: str) -> str:
    """Validate and quote an opaque Plane API path segment."""
    segment = str(value or "").strip()
    if (
        not segment
        or segment in {".", ".."}
        or any(char in segment for char in ("/", "\\", "\x00"))
    ):
        raise ValueError(f"invalid Plane {resource}")
    return quote(segment, safe="-._~")


class PlaneClient:
    """Plane API client restricted to one workspace and project allowlist."""

    def __init__(
        self,
        *,
        base_url: str,
        workspace_slug: str,
        api_key: str,
        project_ids: list[str] | tuple[str, ...],
        timeout_seconds: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.base_url = _normalize_base_url(base_url)
        self.workspace_slug = _path_segment(
            workspace_slug,
            resource="workspace_slug",
        )
        self.api_key = str(api_key or "")
        self.project_ids = tuple(dict.fromkeys(
            str(project_id).strip()
            for project_id in project_ids
            if str(project_id).strip()
        ))
        self.timeout_seconds = max(float(timeout_seconds), 1.0)
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

        if not self.workspace_slug:
            raise ValueError("plane workspace_slug is required")
        if not self.api_key:
            raise ValueError("plane API key is required")
        if not self.project_ids:
            raise ValueError("plane projects allowlist must not be empty")

    @property
    def workspace_path(self) -> str:
        return f"/api/v1/workspaces/{self.workspace_slug}"

    def _project_path(self, project_id: str) -> str:
        project_id = str(project_id or "").strip()
        if project_id not in self.project_ids:
            raise ValueError(f"Plane project is not allowlisted: {project_id!r}")
        return (
            f"{self.workspace_path}/projects/"
            f"{_path_segment(project_id, resource='project_id')}"
        )

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "X-API-Key": self.api_key,
                    "Accept": "application/json",
                },
                timeout=self.timeout_seconds,
                follow_redirects=False,
                transport=self._transport,
            )
        return self._client

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        client = await self._http()
        try:
            response = await client.get(path, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            raise PlaneAPIError(f"Plane GET failed with HTTP {status}") from exc
        except (httpx.HTTPError, ValueError) as exc:
            raise PlaneAPIError(f"Plane GET failed: {type(exc).__name__}") from exc

    async def get_project(self, project_id: str) -> dict[str, Any]:
        data = await self._get(f"{self._project_path(project_id)}/")
        if not isinstance(data, dict):
            raise PlaneAPIError("Plane project response is not an object")
        return data

    async def list_project_states(self, project_id: str) -> list[dict[str, Any]]:
        data = await self._get(f"{self._project_path(project_id)}/states/")
        return self._results(data, resource="states")

    async def list_project_members(self, project_id: str) -> list[dict[str, Any]]:
        data = await self._get(f"{self._project_path(project_id)}/members/")
        return self._results(data, resource="members")

    async def get_work_item(
        self,
        project_id: str,
        work_item_id: str,
        *,
        expand: str = "state,assignees,labels",
    ) -> dict[str, Any]:
        item_id = _path_segment(work_item_id, resource="work_item_id")
        data = await self._get(
            f"{self._project_path(project_id)}/work-items/{item_id}/",
            params={"expand": expand} if expand else None,
        )
        if not isinstance(data, dict):
            raise PlaneAPIError("Plane work-item response is not an object")
        return data

    async def list_work_items_page(
        self,
        project_id: str,
        *,
        cursor: str | None = None,
        per_page: int = 100,
        order_by: str = "-updated_at",
        expand: str = "state,assignees,labels",
    ) -> PlanePage:
        params: dict[str, Any] = {
            "per_page": max(1, min(int(per_page), 100)),
            "order_by": order_by,
        }
        if expand:
            params["expand"] = expand
        if cursor:
            params["cursor"] = cursor

        data = await self._get(
            f"{self._project_path(project_id)}/work-items/",
            params=params,
        )
        if isinstance(data, list):
            return PlanePage(results=[
                item for item in data if isinstance(item, dict)
            ])
        if not isinstance(data, dict):
            raise PlaneAPIError("Plane work-items response is not a list/object")

        results = data.get("results") or []
        if not isinstance(results, list):
            raise PlaneAPIError("Plane work-items results is not a list")
        return PlanePage(
            results=[item for item in results if isinstance(item, dict)],
            next_cursor=str(data.get("next_cursor") or "") or None,
            has_more=data.get("next_page_results") is True,
        )

    async def list_all_work_items(
        self,
        project_id: str,
        *,
        per_page: int = 100,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        """Read a bounded full snapshot of one allowlisted project."""
        results: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()

        for _ in range(max(1, int(max_pages))):
            page = await self.list_work_items_page(
                project_id,
                cursor=cursor,
                per_page=per_page,
            )
            results.extend(page.results)
            if not page.has_more:
                return results
            if not page.next_cursor or page.next_cursor in seen_cursors:
                raise PlaneAPIError("Plane pagination cursor did not advance")
            seen_cursors.add(page.next_cursor)
            cursor = page.next_cursor

        raise PlaneAPIError(
            f"Plane project exceeded max_pages={max(1, int(max_pages))}"
        )

    @staticmethod
    def _results(data: Any, *, resource: str) -> list[dict[str, Any]]:
        if isinstance(data, dict):
            data = data.get("results") or []
        if not isinstance(data, list):
            raise PlaneAPIError(f"Plane {resource} response is not a list")
        return [item for item in data if isinstance(item, dict)]

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
