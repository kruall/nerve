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


def test_plane_config_resolves_strict_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("SYNTHETIC_PLANE_KEY", raising=False)
    credentials = tmp_path / "plane.env"
    credentials.write_text(
        "# dedicated agent credential\n"
        "PLANE_AGENT_TOKEN='file-secret'\n",
        encoding="utf-8",
    )
    credentials.chmod(0o600)
    config = NerveConfig.from_dict({
        "sync": {
            "plane": {
                "enabled": True,
                "base_url": "https://plane.invalid",
                "workspace_slug": "synthetic",
                "projects": [PROJECT_A],
                "api_key_env": "SYNTHETIC_PLANE_KEY",
                "api_key_file": str(credentials),
                "api_key_file_env": "PLANE_AGENT_TOKEN",
            },
        },
    })

    assert config.sync.plane.effective_api_key == "file-secret"


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o644])
def test_plane_config_rejects_broad_credential_file_permissions(
    tmp_path,
    monkeypatch,
    mode,
):
    monkeypatch.delenv("SYNTHETIC_PLANE_KEY", raising=False)
    credentials = tmp_path / "plane.env"
    credentials.write_text(
        "PLANE_AGENT_TOKEN=file-secret\n",
        encoding="utf-8",
    )
    credentials.chmod(mode)
    config = NerveConfig.from_dict({
        "sync": {
            "plane": {
                "api_key_env": "SYNTHETIC_PLANE_KEY",
                "api_key_file": str(credentials),
                "api_key_file_env": "PLANE_AGENT_TOKEN",
            },
        },
    })

    with pytest.raises(ValueError, match="permissions"):
        _ = config.sync.plane.effective_api_key


def test_plane_config_rejects_credential_file_symlink(tmp_path, monkeypatch):
    monkeypatch.delenv("SYNTHETIC_PLANE_KEY", raising=False)
    target = tmp_path / "target.env"
    target.write_text("PLANE_AGENT_TOKEN=file-secret\n", encoding="utf-8")
    target.chmod(0o600)
    credentials = tmp_path / "plane.env"
    credentials.symlink_to(target)
    config = NerveConfig.from_dict({
        "sync": {
            "plane": {
                "api_key_env": "SYNTHETIC_PLANE_KEY",
                "api_key_file": str(credentials),
                "api_key_file_env": "PLANE_AGENT_TOKEN",
            },
        },
    })

    with pytest.raises(ValueError, match="non-symlink"):
        _ = config.sync.plane.effective_api_key


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
        "https://plane.invalid/unexpected-prefix",
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
async def test_client_write_methods_issue_one_request_and_use_exact_paths():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        if request.method == "POST" and path.endswith("/work-items/"):
            return httpx.Response(201, json={"id": "item-1"})
        if request.method == "PATCH" and path.endswith(
            "/work-items/item-1/"
        ):
            return httpx.Response(200, json={"id": "item-1"})
        if request.method == "POST" and path.endswith(
            "/work-items/item-1/comments/"
        ):
            return httpx.Response(201, json={"id": "comment-1"})
        if request.method == "POST" and path.endswith(
            "/work-items/item-1/links/"
        ):
            return httpx.Response(201, json={"id": "link-1"})
        return httpx.Response(404)

    client = PlaneClient(
        base_url="https://plane.invalid",
        workspace_slug="synthetic",
        api_key="test-token",
        project_ids=[PROJECT_A],
        transport=httpx.MockTransport(handler),
    )
    try:
        created = await client.create_work_item(
            PROJECT_A,
            {"name": "Created"},
        )
        updated = await client.update_work_item(
            PROJECT_A,
            "item-1",
            {"priority": "high"},
        )
        comment = await client.add_work_item_comment(
            PROJECT_A,
            "item-1",
            {"comment_html": "<p>Evidence</p>"},
        )
        link = await client.add_work_item_link(
            PROJECT_A,
            "item-1",
            {"title": "Review", "url": "https://example.invalid"},
        )
    finally:
        await client.close()

    assert [created["id"], updated["id"], comment["id"], link["id"]] == [
        "item-1",
        "item-1",
        "comment-1",
        "link-1",
    ]
    assert [request.method for request in requests] == [
        "POST",
        "PATCH",
        "POST",
        "POST",
    ]
    assert all(
        request.url.path.startswith(
            f"/api/v1/workspaces/synthetic/projects/{PROJECT_A}/"
        )
        for request in requests
    )


@pytest.mark.asyncio
async def test_client_does_not_retry_ambiguous_write_failure():
    request_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(503, text="ambiguous upstream failure")

    client = PlaneClient(
        base_url="https://plane.invalid",
        workspace_slug="synthetic",
        api_key="test-token",
        project_ids=[PROJECT_A],
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(PlaneAPIError, match="POST failed with HTTP 503"):
            await client.create_work_item(PROJECT_A, {"name": "Created"})
    finally:
        await client.close()

    assert request_count == 1


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


def _mutation_context() -> ToolContext:
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
    return ToolContext(session_id="session-1", config=config)


def _install_mutation_client(monkeypatch, backend):
    class FakeMutationClient:
        def __init__(self, **kwargs):
            self.workspace_slug = kwargs["workspace_slug"]
            self.project_ids = tuple(kwargs["project_ids"])

        @staticmethod
        def _copy(value):
            return json.loads(json.dumps(value))

        async def list_all_work_items(self, project_id, **kwargs):
            return self._copy(backend.get("work_items", []))

        async def get_work_item(self, project_id, work_item_id):
            if work_item_id == "blocker":
                return self._copy(backend["blocker"])
            return self._copy(backend["item"])

        async def list_project_states(self, project_id):
            return self._copy([
                {"id": "state-todo", "name": "Todo", "group": "unstarted"},
                {
                    "id": "state-progress",
                    "name": "In Progress",
                    "group": "started",
                },
                {"id": "state-done", "name": "Done", "group": "completed"},
            ])

        async def list_project_members(self, project_id):
            return self._copy([
                {"member": {"id": "user-1", "display_name": "Alice"}},
            ])

        async def list_project_labels(self, project_id):
            return self._copy([
                {"id": "label-1", "name": "backend", "color": "#fff"},
            ])

        @staticmethod
        def _apply_payload(item, payload):
            item.update(payload)
            if "state" in payload:
                state_id = payload["state"]
                group = {
                    "state-todo": "unstarted",
                    "state-progress": "started",
                    "state-done": "completed",
                }[state_id]
                item["state"] = {
                    "id": state_id,
                    "name": state_id,
                    "group": group,
                }
            if "assignees" in payload:
                item["assignees"] = [
                    {"id": value, "display_name": "Alice"}
                    for value in payload["assignees"]
                ]
            if "labels" in payload:
                item["labels"] = [
                    {"id": value, "name": "backend"}
                    for value in payload["labels"]
                ]

        async def create_work_item(self, project_id, payload):
            backend["calls"].append("create")
            item = _item(
                "created-item",
                2,
                "2026-01-01T02:00:00Z",
                name=payload["name"],
            )
            self._apply_payload(item, payload)
            backend["item"] = item
            backend.setdefault("work_items", []).append(item)
            return {"id": item["id"]}

        async def update_work_item(self, project_id, work_item_id, payload):
            backend["calls"].append("update")
            self._apply_payload(backend["item"], payload)
            backend["item"]["updated_at"] = "2026-01-01T03:00:00Z"
            return self._copy(backend["item"])

        async def list_work_item_relations(self, project_id, work_item_id):
            return self._copy(backend.get("relations", {}))

        async def list_work_item_comments(self, project_id, work_item_id):
            return self._copy(backend.setdefault("comments", []))

        async def add_work_item_comment(
            self,
            project_id,
            work_item_id,
            payload,
        ):
            backend["calls"].append("comment")
            row = {
                "id": "comment-1",
                "created_at": "2026-01-01T03:30:00Z",
                **payload,
            }
            backend["comments"].append(row)
            backend["item"]["updated_at"] = "2026-01-01T03:30:00Z"
            return self._copy(row)

        async def list_work_item_links(self, project_id, work_item_id):
            return self._copy(backend.setdefault("links", []))

        async def add_work_item_link(
            self,
            project_id,
            work_item_id,
            payload,
        ):
            backend["calls"].append("link")
            row = {"id": "link-1", **payload}
            backend["links"].append(row)
            return self._copy(row)

        async def close(self):
            return None

    monkeypatch.setattr(plane_tools, "PlaneClient", FakeMutationClient)


@pytest.mark.asyncio
async def test_conflict_safe_plane_mcp_mutations(monkeypatch):
    backend = {
        "calls": [],
        "item": _item(
            "placeholder",
            1,
            "2026-01-01T01:00:00Z",
        ),
        "work_items": [],
        "relations": {},
        "comments": [],
        "links": [],
    }
    _install_mutation_client(monkeypatch, backend)
    ctx = _mutation_context()

    created = await plane_tools.plane_create_work_item_handler(ctx, {
        "project_id": PROJECT_A,
        "name": "Created task",
        "description_html": "<p>Created safely</p>",
        "priority": "high",
        "state": "state-todo",
        "assignees": ["user-1"],
        "labels": ["label-1"],
    })
    assert created.is_error is False
    assert json.loads(created.content[0]["text"])["verified"] is True

    updated = await plane_tools.plane_update_work_item_handler(ctx, {
        "project_id": PROJECT_A,
        "work_item_id": "created-item",
        "expected_updated_at": "2026-01-01T02:00:00Z",
        "state": "state-progress",
        "priority": "urgent",
        "assignees": ["user-1"],
    })
    assert updated.is_error is False
    assert json.loads(updated.content[0]["text"])["verified"] is True

    commented = await plane_tools.plane_add_comment_handler(ctx, {
        "project_id": PROJECT_A,
        "work_item_id": "created-item",
        "expected_updated_at": "2026-01-01T03:00:00Z",
        "comment": "Evidence <verified>",
    })
    assert commented.is_error is False
    assert backend["comments"][0]["comment_html"] == (
        "<p>Evidence &lt;verified&gt;</p>"
    )

    linked = await plane_tools.plane_add_link_handler(ctx, {
        "project_id": PROJECT_A,
        "work_item_id": "created-item",
        "expected_updated_at": "2026-01-01T03:30:00Z",
        "title": "Review",
        "url": "https://example.invalid/review/1",
    })
    assert linked.is_error is False
    assert backend["calls"] == ["create", "update", "comment", "link"]


@pytest.mark.asyncio
async def test_plane_mutations_fail_closed_before_write(monkeypatch):
    backend = {
        "calls": [],
        "item": _item(
            "item-1",
            1,
            "2026-01-01T02:00:00Z",
        ),
        "work_items": [],
        "relations": {},
        "comments": [{
            "id": "existing-comment",
            "comment_html": "<p>Already recorded</p>",
        }],
        "links": [],
    }
    _install_mutation_client(monkeypatch, backend)
    ctx = _mutation_context()

    stale = await plane_tools.plane_update_work_item_handler(ctx, {
        "project_id": PROJECT_A,
        "work_item_id": "item-1",
        "expected_updated_at": "2026-01-01T01:00:00Z",
        "priority": "high",
    })
    assert stale.is_error is True
    assert "changed since" in stale.content[0]["text"]

    duplicate = await plane_tools.plane_add_comment_handler(ctx, {
        "project_id": PROJECT_A,
        "work_item_id": "item-1",
        "expected_updated_at": "2026-01-01T02:00:00Z",
        "comment": "Already recorded",
    })
    assert duplicate.is_error is True
    assert "collision" in duplicate.content[0]["text"]
    assert backend["calls"] == []


@pytest.mark.asyncio
async def test_plane_state_update_honors_blocked_by_gate(monkeypatch):
    backend = {
        "calls": [],
        "item": _item(
            "item-1",
            1,
            "2026-01-01T02:00:00Z",
        ),
        "work_items": [],
        "relations": {
            "blocked_by": [{
                "id": "blocker",
                "state": {
                    "id": "state-todo",
                    "name": "Todo",
                    "group": "unstarted",
                },
            }],
        },
        "comments": [],
        "links": [],
    }
    _install_mutation_client(monkeypatch, backend)
    ctx = _mutation_context()

    result = await plane_tools.plane_update_work_item_handler(ctx, {
        "project_id": PROJECT_A,
        "work_item_id": "item-1",
        "expected_updated_at": "2026-01-01T02:00:00Z",
        "state": "state-progress",
    })
    assert result.is_error is True
    assert "dependency gate" in result.content[0]["text"]
    assert backend["calls"] == []


def test_bundled_plane_skill_documents_safety_contract():
    skill = (
        Path(__file__).parents[1]
        / "nerve/templates/skills/plane-collaboration/SKILL.md"
    ).read_text(encoding="utf-8")
    assert "name: Plane Collaboration" in skill
    assert "updated_at" in skill
    assert "Do not retry an ambiguous POST/PATCH" in skill
    assert "plane_list_work_items" in skill
