"""Tests for the first-party Plane client, source, config, and MCP reads."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from nerve.agent.tools.handlers import plane as plane_tools
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig
from nerve.integrations.plane import PlaneAPIError, PlaneClient
from nerve.sources.plane import PlaneSource
from nerve.sources.registry import build_source_runners


PROJECT_A = "11111111-1111-4111-8111-111111111111"
PROJECT_B = "22222222-2222-4222-8222-222222222222"


def _item(
    item_id: str,
    sequence_id: int,
    updated_at: str,
    *,
    name: str = "Synthetic task",
    state: str = "Todo",
) -> dict:
    return {
        "id": item_id,
        "sequence_id": sequence_id,
        "name": name,
        "description_html": "<p>Safe synthetic description</p>",
        "description_stripped": "Safe synthetic description",
        "priority": "medium",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": updated_at,
        "state": {"id": "state-1", "name": state, "group": "unstarted"},
        "assignees": [{"id": "user-1", "display_name": "Alice"}],
        "labels": [{"id": "label-1", "name": "backend"}],
    }


class _FakePlaneClient:
    def __init__(
        self,
        snapshots: dict[str, list[dict] | Exception],
    ):
        self.snapshots = snapshots
        self.closed = False

    async def get_project(self, project_id: str) -> dict:
        return {
            "id": project_id,
            "name": f"Project {project_id[-1]}",
            "identifier": f"P{project_id[-1]}",
        }

    async def list_all_work_items(
        self,
        project_id: str,
        *,
        per_page: int,
        max_pages: int,
    ) -> list[dict]:
        value = self.snapshots[project_id]
        if isinstance(value, Exception):
            raise value
        return list(value)

    async def close(self) -> None:
        self.closed = True


def _source(
    client: _FakePlaneClient,
    *,
    projects: list[str] | None = None,
    initial_backfill: bool = False,
) -> PlaneSource:
    return PlaneSource(
        {
            "base_url": "https://plane.invalid",
            "workspace_slug": "synthetic-workspace",
            "projects": projects or [PROJECT_A],
            "api_key": "test-token",
            "initial_backfill": initial_backfill,
        },
        client=client,
    )


def _cursor(result) -> dict:
    return json.loads(result.next_cursor)


def test_plane_config_resolves_env_key(monkeypatch):
    monkeypatch.setenv("SYNTHETIC_PLANE_KEY", "env-secret")
    config = NerveConfig.from_dict({
        "sync": {
            "plane": {
                "enabled": True,
                "base_url": "https://plane.invalid/api",
                "workspace_slug": "synthetic",
                "projects": [PROJECT_A],
                "api_key_env": "SYNTHETIC_PLANE_KEY",
            },
        },
    })

    assert config.sync.plane.base_url == "https://plane.invalid/api"
    assert config.sync.plane.projects == [PROJECT_A]
    assert config.sync.plane.effective_api_key == "env-secret"


@pytest.mark.asyncio
async def test_registry_builds_plane_runner_from_config(db, monkeypatch):
    monkeypatch.setenv("SYNTHETIC_PLANE_KEY", "env-secret")
    config = NerveConfig.from_dict({
        "sync": {
            "telegram": {"enabled": False},
            "gmail": {"enabled": False},
            "github": {"enabled": False},
            "github_events": {"enabled": False},
            "github_repos": {"enabled": False},
            "plane": {
                "enabled": True,
                "base_url": "https://plane.invalid",
                "workspace_slug": "synthetic",
                "projects": [PROJECT_A],
                "api_key_env": "SYNTHETIC_PLANE_KEY",
            },
        },
    })

    runners = build_source_runners(config, db)

    assert [runner.source.source_name for runner in runners] == [
        "plane:synthetic"
    ]


@pytest.mark.asyncio
async def test_client_enforces_allowlist_and_paginates():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["X-API-Key"] == "test-token"
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(200, json={
                "results": [_item("item-1", 1, "2026-01-01T01:00:00Z")],
                "next_cursor": "100:1:0",
                "next_page_results": True,
            })
        return httpx.Response(200, json={
            "results": [_item("item-2", 2, "2026-01-01T02:00:00Z")],
            "next_cursor": "100:2:0",
            "next_page_results": False,
        })

    client = PlaneClient(
        base_url="https://plane.invalid/api/v1/",
        workspace_slug="synthetic",
        api_key="test-token",
        project_ids=[PROJECT_A],
        transport=httpx.MockTransport(handler),
    )
    try:
        items = await client.list_all_work_items(PROJECT_A)
        assert [item["id"] for item in items] == ["item-1", "item-2"]
        assert len(requests) == 2
        assert requests[0].url.path.endswith(
            f"/projects/{PROJECT_A}/work-items/"
        )
        with pytest.raises(ValueError, match="not allowlisted"):
            await client.get_project(PROJECT_B)
        with pytest.raises(ValueError, match="work_item_id"):
            await client.get_work_item(PROJECT_A, "../members")
    finally:
        await client.close()


@pytest.mark.parametrize(
    "base_url",
    [
        "https://user:password@plane.invalid",
        "https://plane.invalid?token=secret",
        "https://plane.invalid#fragment",
    ],
)
def test_client_rejects_credential_bearing_or_ambiguous_base_url(base_url):
    with pytest.raises(ValueError):
        PlaneClient(
            base_url=base_url,
            workspace_slug="synthetic",
            api_key="test-token",
            project_ids=[PROJECT_A],
        )


@pytest.mark.asyncio
async def test_client_redacts_http_failures():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="secret-bearing response")

    client = PlaneClient(
        base_url="https://plane.invalid",
        workspace_slug="synthetic",
        api_key="super-secret-token",
        project_ids=[PROJECT_A],
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(PlaneAPIError) as exc_info:
            await client.get_project(PROJECT_A)
        text = str(exc_info.value)
        assert "HTTP 403" in text
        assert "super-secret-token" not in text
        assert "secret-bearing" not in text
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_source_first_run_baselines_without_backfill():
    client = _FakePlaneClient({
        PROJECT_A: [
            _item("item-1", 1, "2026-01-01T01:00:00Z"),
            _item("item-2", 2, "2026-01-01T02:00:00Z"),
        ],
    })
    source = _source(client)

    result = await source.fetch(None)

    assert result.records == []
    assert _cursor(result)["projects"][PROJECT_A] == {
        "updated_at": "2026-01-01T02:00:00Z",
        "ids": ["item-2"],
    }


@pytest.mark.asyncio
async def test_source_emits_changed_snapshot_after_baseline():
    client = _FakePlaneClient({
        PROJECT_A: [_item("item-1", 1, "2026-01-01T01:00:00Z")],
    })
    source = _source(client)
    baseline = await source.fetch(None)
    client.snapshots[PROJECT_A] = [
        _item(
            "item-1",
            1,
            "2026-01-01T03:00:00Z",
            name="Changed synthetic task",
            state="In Progress",
        ),
    ]

    result = await source.fetch(baseline.next_cursor)

    assert len(result.records) == 1
    record = result.records[0]
    assert record.id == "item-1"
    assert record.source == "plane:synthetic-workspace"
    assert record.record_type == "plane_work_item"
    assert "Changed synthetic task" in record.content
    assert record.metadata["state"] == "In Progress"
    assert record.metadata["assignees"] == ["Alice"]


@pytest.mark.asyncio
async def test_source_limit_does_not_advance_unemitted_projects():
    client = _FakePlaneClient({
        PROJECT_A: [
            _item("a-1", 1, "2026-01-01T01:00:00Z"),
            _item("a-3", 3, "2026-01-01T03:00:00Z"),
        ],
        PROJECT_B: [
            _item("b-2", 2, "2026-01-01T02:00:00Z"),
            _item("b-4", 4, "2026-01-01T04:00:00Z"),
        ],
    })
    source = _source(
        client,
        projects=[PROJECT_A, PROJECT_B],
        initial_backfill=True,
    )

    first = await source.fetch(None, limit=2)
    assert [record.id for record in first.records] == ["a-1", "b-2"]
    first_cursor = _cursor(first)["projects"]
    assert first_cursor[PROJECT_A]["updated_at"] == "2026-01-01T01:00:00Z"
    assert first_cursor[PROJECT_B]["updated_at"] == "2026-01-01T02:00:00Z"
    assert first.has_more is True

    second = await source.fetch(first.next_cursor, limit=2)
    assert [record.id for record in second.records] == ["a-3", "b-4"]
    assert second.has_more is False


@pytest.mark.asyncio
async def test_source_partial_failure_keeps_failed_project_uninitialized():
    client = _FakePlaneClient({
        PROJECT_A: [_item("a-1", 1, "2026-01-01T01:00:00Z")],
        PROJECT_B: PlaneAPIError("temporary"),
    })
    source = _source(client, projects=[PROJECT_A, PROJECT_B])

    baseline = await source.fetch(None)
    state = _cursor(baseline)["projects"]
    assert PROJECT_A in state
    assert PROJECT_B not in state

    client.snapshots[PROJECT_B] = [
        _item("b-1", 1, "2026-01-01T02:00:00Z"),
    ]
    recovered = await source.fetch(baseline.next_cursor)
    assert recovered.records == []
    assert PROJECT_B in _cursor(recovered)["projects"]


@pytest.mark.asyncio
async def test_source_rejects_invalid_cursor():
    source = _source(_FakePlaneClient({PROJECT_A: []}))
    with pytest.raises(PlaneAPIError, match="cursor"):
        await source.fetch("not-json")


@pytest.mark.asyncio
async def test_read_only_plane_mcp_tools(monkeypatch):
    class FakeToolClient:
        def __init__(self, **kwargs):
            self.workspace_slug = kwargs["workspace_slug"]
            self.project_ids = tuple(kwargs["project_ids"])

        async def get_project(self, project_id):
            return {
                "id": project_id,
                "name": "Synthetic",
                "identifier": "SYN",
                "is_member": True,
                "member_role": 15,
                "updated_at": "2026-01-01T00:00:00Z",
            }

        async def get_work_item(self, project_id, work_item_id):
            return _item(
                work_item_id,
                1,
                "2026-01-01T01:00:00Z",
            )

        async def close(self):
            return None

    monkeypatch.setattr(plane_tools, "PlaneClient", FakeToolClient)
    config = NerveConfig.from_dict({
        "sync": {
            "plane": {
                "enabled": True,
                "base_url": "https://plane.invalid",
                "workspace_slug": "synthetic",
                "projects": [PROJECT_A],
                "api_key": "test-token",
            },
        },
    })
    ctx = ToolContext(session_id="session-1", config=config)

    projects_result = await plane_tools.plane_list_projects_handler(ctx, {})
    projects_payload = json.loads(projects_result.content[0]["text"])
    assert projects_payload["projects"][0]["identifier"] == "SYN"
    assert "test-token" not in projects_result.content[0]["text"]

    item_result = await plane_tools.plane_get_work_item_handler(ctx, {
        "project_id": PROJECT_A,
        "work_item_id": "item-1",
    })
    item_payload = json.loads(item_result.content[0]["text"])
    assert item_payload["id"] == "item-1"
    assert item_payload["assignees"] == [
        {"id": "user-1", "display_name": "Alice"},
    ]


def test_bundled_plane_skill_documents_safety_contract():
    skill = (
        Path(__file__).parents[1]
        / "nerve/templates/skills/plane-collaboration/SKILL.md"
    ).read_text(encoding="utf-8")
    assert "name: Plane Collaboration" in skill
    assert "updated_at" in skill
    assert "Do not retry an ambiguous POST/PATCH" in skill
    assert "plane_list_work_items" in skill
