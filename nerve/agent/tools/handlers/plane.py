"""First-party read-only Plane MCP tools.

The initial surface is deliberately small and allowlisted.  Mutation tools are
added separately once they can enforce compare-before-write and read-back
inside the handler rather than relying on prompt instructions.
"""

from __future__ import annotations

import json
from typing import Any

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    PLANE_GET_WORK_ITEM_SCHEMA,
    PLANE_LIST_PROJECTS_SCHEMA,
    PLANE_LIST_WORK_ITEMS_SCHEMA,
    PLANE_PROJECT_RESOURCE_SCHEMA,
)
from nerve.integrations.plane import PlaneAPIError, PlaneClient


def _client_from_context(ctx: ToolContext) -> PlaneClient:
    if ctx.config is None:
        raise ValueError("Nerve config is unavailable")
    config = ctx.config.sync.plane
    if not config.enabled:
        raise ValueError("Plane integration is disabled")
    return PlaneClient(
        base_url=config.base_url,
        workspace_slug=config.workspace_slug,
        api_key=config.effective_api_key,
        project_ids=config.projects,
        timeout_seconds=config.timeout_seconds,
    )


def _json_result(payload: Any) -> ToolResult:
    return ToolResult.text(json.dumps(payload, ensure_ascii=False, indent=2))


def _state_summary(value: Any) -> dict[str, Any] | str | None:
    if isinstance(value, dict):
        return {
            "id": value.get("id"),
            "name": value.get("name"),
            "group": value.get("group"),
        }
    return value


def _person_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"id": value, "display_name": None}
    return {
        "id": value.get("id"),
        "display_name": (
            value.get("display_name")
            or " ".join(
                str(part)
                for part in (
                    value.get("first_name"),
                    value.get("last_name"),
                )
                if part
            )
            or None
        ),
    }


def _work_item_summary(item: dict[str, Any], *, full: bool = False) -> dict[str, Any]:
    result = {
        "id": item.get("id"),
        "sequence_id": item.get("sequence_id"),
        "name": item.get("name"),
        "state": _state_summary(item.get("state")),
        "priority": item.get("priority"),
        "assignees": [
            _person_summary(value) for value in (item.get("assignees") or [])
        ],
        "labels": [
            {
                "id": value.get("id"),
                "name": value.get("name"),
            } if isinstance(value, dict) else {"id": value, "name": None}
            for value in (item.get("labels") or [])
        ],
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
    }
    if full:
        result.update({
            "description_html": item.get("description_html"),
            "description_stripped": item.get("description_stripped"),
            "start_date": item.get("start_date"),
            "target_date": item.get("target_date"),
            "parent": item.get("parent"),
            "archived_at": item.get("archived_at"),
        })
    return result


async def plane_list_projects_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    del args
    try:
        client = _client_from_context(ctx)
        try:
            projects = []
            for project_id in client.project_ids:
                project = await client.get_project(project_id)
                projects.append({
                    "id": project.get("id"),
                    "name": project.get("name"),
                    "identifier": project.get("identifier"),
                    "is_member": project.get("is_member"),
                    "member_role": project.get("member_role"),
                    "updated_at": project.get("updated_at"),
                })
            return _json_result({
                "workspace_slug": client.workspace_slug,
                "projects": projects,
            })
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


async def plane_list_states_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            states = await client.list_project_states(args["project_id"])
            return _json_result({
                "project_id": args["project_id"],
                "states": [
                    {
                        "id": state.get("id"),
                        "name": state.get("name"),
                        "group": state.get("group"),
                        "sequence": state.get("sequence"),
                    }
                    for state in states
                ],
            })
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


async def plane_list_members_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            rows = await client.list_project_members(args["project_id"])
            members = []
            for row in rows:
                member = row.get("member") if isinstance(row.get("member"), dict) else row
                members.append({
                    **_person_summary(member),
                    "role": (
                        row.get("role")
                        or row.get("member_role")
                        or member.get("role")
                    ),
                })
            return _json_result({
                "project_id": args["project_id"],
                "members": members,
            })
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


async def plane_list_work_items_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            page = await client.list_work_items_page(
                args["project_id"],
                cursor=str(args.get("cursor") or "") or None,
                per_page=int(args.get("limit", 50)),
            )
            return _json_result({
                "project_id": args["project_id"],
                "work_items": [
                    _work_item_summary(item) for item in page.results
                ],
                "next_cursor": page.next_cursor,
                "has_more": page.has_more,
            })
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


async def plane_get_work_item_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            item = await client.get_work_item(
                args["project_id"],
                args["work_item_id"],
            )
            return _json_result(_work_item_summary(item, full=True))
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


PLANE_LIST_PROJECTS_SPEC = ToolSpec(
    name="plane_list_projects",
    description=(
        "List the Plane projects explicitly allowlisted in Nerve configuration."
    ),
    input_schema=PLANE_LIST_PROJECTS_SCHEMA,
    handler=plane_list_projects_handler,
)

PLANE_LIST_STATES_SPEC = ToolSpec(
    name="plane_list_states",
    description="List workflow states for an allowlisted Plane project.",
    input_schema=PLANE_PROJECT_RESOURCE_SCHEMA,
    handler=plane_list_states_handler,
)

PLANE_LIST_MEMBERS_SPEC = ToolSpec(
    name="plane_list_members",
    description=(
        "List redacted member identities and roles for an allowlisted Plane project."
    ),
    input_schema=PLANE_PROJECT_RESOURCE_SCHEMA,
    handler=plane_list_members_handler,
)

PLANE_LIST_WORK_ITEMS_SPEC = ToolSpec(
    name="plane_list_work_items",
    description=(
        "List work items from an allowlisted Plane project using Plane pagination."
    ),
    input_schema=PLANE_LIST_WORK_ITEMS_SCHEMA,
    handler=plane_list_work_items_handler,
)

PLANE_GET_WORK_ITEM_SPEC = ToolSpec(
    name="plane_get_work_item",
    description="Retrieve one work item from an allowlisted Plane project.",
    input_schema=PLANE_GET_WORK_ITEM_SCHEMA,
    handler=plane_get_work_item_handler,
)

PLANE_SPECS = [
    PLANE_LIST_PROJECTS_SPEC,
    PLANE_LIST_STATES_SPEC,
    PLANE_LIST_MEMBERS_SPEC,
    PLANE_LIST_WORK_ITEMS_SPEC,
    PLANE_GET_WORK_ITEM_SPEC,
]
