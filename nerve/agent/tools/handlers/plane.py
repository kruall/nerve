"""First-party Plane MCP tools with an allowlisted, conflict-safe write surface.

The surface is deliberately small. Mutations enforce compare-before-write,
collision checks, one request only, and read-back inside the handler rather
than relying on prompt instructions.
"""

from __future__ import annotations

import json
from html import escape
from typing import Any
from urllib.parse import urlsplit

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    PLANE_ADD_COMMENT_SCHEMA,
    PLANE_ADD_LINK_SCHEMA,
    PLANE_CREATE_WORK_ITEM_SCHEMA,
    PLANE_GET_WORK_ITEM_SCHEMA,
    PLANE_LIST_PROJECTS_SCHEMA,
    PLANE_LIST_WORK_ITEMS_SCHEMA,
    PLANE_PROJECT_RESOURCE_SCHEMA,
    PLANE_UPDATE_WORK_ITEM_SCHEMA,
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


def _reference_id(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("id") or "")
    return str(value or "")


def _reference_ids(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [
        reference_id
        for reference_id in (_reference_id(value) for value in values)
        if reference_id
    ]


def _assert_expected_updated_at(
    item: dict[str, Any],
    expected_updated_at: str,
) -> None:
    actual = str(item.get("updated_at") or "")
    expected = str(expected_updated_at or "")
    if not expected or actual != expected:
        raise ValueError(
            "Plane conflict: work item changed since the preceding read"
        )


async def _validate_references(
    client: PlaneClient,
    project_id: str,
    payload: dict[str, Any],
) -> None:
    if "state" in payload:
        states = await client.list_project_states(project_id)
        allowed = {_reference_id(state) for state in states}
        if str(payload["state"]) not in allowed:
            raise ValueError("Plane state is not present in the project")

    if "assignees" in payload:
        rows = await client.list_project_members(project_id)
        allowed = set()
        for row in rows:
            member = row.get("member")
            if member is None:
                member = row
            allowed.add(_reference_id(member))
        requested = [str(value) for value in payload["assignees"]]
        if len(requested) != len(set(requested)):
            raise ValueError("Plane assignee set contains duplicates")
        if not set(requested).issubset(allowed):
            raise ValueError("Plane assignee is not a project member")

    if "labels" in payload:
        labels = await client.list_project_labels(project_id)
        allowed = {_reference_id(label) for label in labels}
        requested = [str(value) for value in payload["labels"]]
        if len(requested) != len(set(requested)):
            raise ValueError("Plane label set contains duplicates")
        if not set(requested).issubset(allowed):
            raise ValueError("Plane label is not present in the project")


def _verify_work_item_payload(
    item: dict[str, Any],
    payload: dict[str, Any],
) -> None:
    mismatches = []
    for field, expected in payload.items():
        if field == "state":
            actual = _reference_id(item.get("state"))
            matches = actual == str(expected)
        elif field in {"assignees", "labels"}:
            actual = set(_reference_ids(item.get(field)))
            matches = actual == {str(value) for value in expected}
        else:
            actual = item.get(field)
            matches = actual == expected
        if not matches:
            mismatches.append(field)
    if mismatches:
        raise PlaneAPIError(
            "Plane read-back mismatch for: " + ", ".join(sorted(mismatches))
        )


def _work_item_field(item: dict[str, Any], field: str) -> Any:
    if field == "state":
        return _reference_id(item.get(field))
    if field in {"assignees", "labels"}:
        return set(_reference_ids(item.get(field)))
    return item.get(field)


def _verify_protected_fields(
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    changed_fields: set[str],
) -> None:
    protected = {
        "name",
        "description_html",
        "priority",
        "state",
        "assignees",
        "labels",
        "start_date",
        "target_date",
        "parent",
        "archived_at",
    } - changed_fields
    mismatches = [
        field
        for field in protected
        if _work_item_field(before, field) != _work_item_field(after, field)
    ]
    if mismatches:
        raise PlaneAPIError(
            "Plane protected-field read-back mismatch for: "
            + ", ".join(sorted(mismatches))
        )


async def _assert_dependency_gate(
    client: PlaneClient,
    project_id: str,
    work_item_id: str,
    target_state_id: str,
) -> None:
    states = await client.list_project_states(project_id)
    target = next(
        (
            state
            for state in states
            if _reference_id(state) == str(target_state_id)
        ),
        None,
    )
    if target is None:
        raise ValueError("Plane state is not present in the project")
    if str(target.get("group") or "") not in {"started", "completed"}:
        return

    relations = await client.list_work_item_relations(
        project_id,
        work_item_id,
    )
    blocked_by = relations.get("blocked_by") or []
    if not isinstance(blocked_by, list):
        raise PlaneAPIError("Plane blocked_by relations are not a list")
    for related in blocked_by:
        related_id = _reference_id(related)
        if not related_id:
            raise PlaneAPIError("Plane blocked_by relation has no work-item id")
        related_state = (
            related.get("state")
            if isinstance(related, dict)
            else None
        )
        if not isinstance(related_state, dict):
            related_item = await client.get_work_item(project_id, related_id)
            related_state = related_item.get("state")
        group = (
            str(related_state.get("group") or "")
            if isinstance(related_state, dict)
            else ""
        )
        if group != "completed":
            raise ValueError(
                "Plane dependency gate: a blocked_by item is not completed"
            )


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
                member = row.get("member")
                if member is None:
                    member = row
                member_role = (
                    member.get("role")
                    if isinstance(member, dict)
                    else None
                )
                members.append({
                    **_person_summary(member),
                    "role": (
                        row.get("role")
                        or row.get("member_role")
                        or member_role
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


async def plane_list_labels_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            labels = await client.list_project_labels(args["project_id"])
            return _json_result({
                "project_id": args["project_id"],
                "labels": [
                    {
                        "id": label.get("id"),
                        "name": label.get("name"),
                        "color": label.get("color"),
                    }
                    for label in labels
                ],
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


async def plane_create_work_item_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            project_id = args["project_id"]
            name = str(args.get("name") or "").strip()
            if not name:
                raise ValueError("Plane work-item name must not be empty")

            max_pages = (
                ctx.config.sync.plane.max_pages_per_project
                if ctx.config is not None
                else 20
            )
            existing = await client.list_all_work_items(
                project_id,
                max_pages=max_pages,
            )
            normalized_name = " ".join(name.casefold().split())
            collisions = [
                item
                for item in existing
                if " ".join(
                    str(item.get("name") or "").casefold().split()
                ) == normalized_name
            ]
            if collisions:
                collision_id = str(collisions[0].get("id") or "unknown")
                raise ValueError(
                    f"Plane create collision: exact title already exists "
                    f"({collision_id})"
                )

            payload: dict[str, Any] = {
                "name": name,
                "priority": str(args.get("priority") or "none"),
            }
            for field in ("description_html", "state"):
                value = args.get(field)
                if value:
                    payload[field] = str(value)
            for field in ("assignees", "labels"):
                if field in args:
                    payload[field] = [
                        str(value) for value in (args.get(field) or [])
                    ]

            await _validate_references(client, project_id, payload)
            existing = await client.list_all_work_items(
                project_id,
                max_pages=max_pages,
            )
            if any(
                " ".join(
                    str(item.get("name") or "").casefold().split()
                ) == normalized_name
                for item in existing
            ):
                raise ValueError(
                    "Plane create collision: exact title appeared "
                    "during preflight"
                )
            created = await client.create_work_item(project_id, payload)
            work_item_id = _reference_id(created)
            if not work_item_id:
                raise PlaneAPIError(
                    "Plane create work-item response has no id"
                )
            readback = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _verify_work_item_payload(readback, payload)
            return _json_result({
                "verified": True,
                "work_item": _work_item_summary(readback, full=True),
            })
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


async def plane_update_work_item_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            project_id = args["project_id"]
            work_item_id = args["work_item_id"]
            before = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _assert_expected_updated_at(
                before,
                args["expected_updated_at"],
            )

            mutable_fields = {
                "name",
                "description_html",
                "priority",
                "state",
                "assignees",
                "labels",
                "start_date",
                "target_date",
            }
            payload = {
                field: args[field]
                for field in mutable_fields
                if field in args
            }
            if not payload:
                raise ValueError("Plane update has no mutable fields")
            if "name" in payload:
                payload["name"] = str(payload["name"] or "").strip()
                if not payload["name"]:
                    raise ValueError("Plane work-item name must not be empty")
                max_pages = (
                    ctx.config.sync.plane.max_pages_per_project
                    if ctx.config is not None
                    else 20
                )
                normalized_name = " ".join(
                    payload["name"].casefold().split()
                )
                existing = await client.list_all_work_items(
                    project_id,
                    max_pages=max_pages,
                )
                if any(
                    _reference_id(item) != str(work_item_id)
                    and " ".join(
                        str(item.get("name") or "").casefold().split()
                    ) == normalized_name
                    for item in existing
                ):
                    raise ValueError(
                        "Plane update collision: exact title already exists"
                    )
            for field in ("assignees", "labels"):
                if field in payload:
                    payload[field] = [
                        str(value) for value in (payload[field] or [])
                    ]

            await _validate_references(client, project_id, payload)
            if "state" in payload:
                await _assert_dependency_gate(
                    client,
                    project_id,
                    work_item_id,
                    str(payload["state"]),
                )

            fresh = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _assert_expected_updated_at(
                fresh,
                args["expected_updated_at"],
            )
            await client.update_work_item(
                project_id,
                work_item_id,
                payload,
            )
            readback = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _verify_work_item_payload(readback, payload)
            _verify_protected_fields(
                before,
                readback,
                changed_fields=set(payload),
            )
            return _json_result({
                "verified": True,
                "work_item": _work_item_summary(readback, full=True),
            })
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


async def plane_add_comment_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            project_id = args["project_id"]
            work_item_id = args["work_item_id"]
            before = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _assert_expected_updated_at(
                before,
                args["expected_updated_at"],
            )
            comment = str(args.get("comment") or "").strip()
            if not comment:
                raise ValueError("Plane comment must not be empty")
            comment_html = (
                "<p>" + escape(comment).replace("\n", "<br>") + "</p>"
            )

            existing = await client.list_work_item_comments(
                project_id,
                work_item_id,
            )
            if any(
                str(row.get("comment_html") or "") == comment_html
                for row in existing
            ):
                raise ValueError(
                    "Plane comment collision: identical comment exists"
                )

            fresh = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _assert_expected_updated_at(
                fresh,
                args["expected_updated_at"],
            )
            created = await client.add_work_item_comment(
                project_id,
                work_item_id,
                {
                    "comment_html": comment_html,
                    "comment_stripped": comment,
                    "access": "INTERNAL",
                },
            )
            comment_id = _reference_id(created)
            if not comment_id:
                raise PlaneAPIError("Plane create comment response has no id")
            readback = await client.list_work_item_comments(
                project_id,
                work_item_id,
            )
            matches = [
                row for row in readback
                if _reference_id(row) == comment_id
                and str(row.get("comment_html") or "") == comment_html
            ]
            if len(matches) != 1:
                raise PlaneAPIError("Plane comment read-back mismatch")
            item_readback = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _verify_protected_fields(
                before,
                item_readback,
                changed_fields=set(),
            )
            return _json_result({
                "verified": True,
                "comment": {
                    "id": comment_id,
                    "created_at": matches[0].get("created_at"),
                },
            })
        finally:
            await client.close()
    except (ValueError, PlaneAPIError) as exc:
        return ToolResult.text(str(exc), is_error=True)


async def plane_add_link_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    try:
        client = _client_from_context(ctx)
        try:
            project_id = args["project_id"]
            work_item_id = args["work_item_id"]
            before = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _assert_expected_updated_at(
                before,
                args["expected_updated_at"],
            )
            title = str(args.get("title") or "").strip()
            url = str(args.get("url") or "").strip()
            parsed = urlsplit(url)
            if not title:
                raise ValueError("Plane link title must not be empty")
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username
                or parsed.password
            ):
                raise ValueError(
                    "Plane link must be an absolute credential-free http(s) URL"
                )

            existing = await client.list_work_item_links(
                project_id,
                work_item_id,
            )
            if any(str(row.get("url") or "") == url for row in existing):
                raise ValueError("Plane link collision: URL already exists")

            fresh = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _assert_expected_updated_at(
                fresh,
                args["expected_updated_at"],
            )
            created = await client.add_work_item_link(
                project_id,
                work_item_id,
                {"title": title, "url": url},
            )
            link_id = _reference_id(created)
            if not link_id:
                raise PlaneAPIError("Plane create link response has no id")
            readback = await client.list_work_item_links(
                project_id,
                work_item_id,
            )
            matches = [
                row for row in readback
                if _reference_id(row) == link_id
                and str(row.get("title") or "") == title
                and str(row.get("url") or "") == url
            ]
            if len(matches) != 1:
                raise PlaneAPIError("Plane link read-back mismatch")
            item_readback = await client.get_work_item(
                project_id,
                work_item_id,
            )
            _verify_protected_fields(
                before,
                item_readback,
                changed_fields=set(),
            )
            return _json_result({
                "verified": True,
                "link": {
                    "id": link_id,
                    "title": title,
                    "url": url,
                },
            })
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

PLANE_LIST_LABELS_SPEC = ToolSpec(
    name="plane_list_labels",
    description="List labels for an allowlisted Plane project.",
    input_schema=PLANE_PROJECT_RESOURCE_SCHEMA,
    handler=plane_list_labels_handler,
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

PLANE_CREATE_WORK_ITEM_SPEC = ToolSpec(
    name="plane_create_work_item",
    description=(
        "Create one work item after an exact-title collision check, then "
        "verify it by read-back."
    ),
    input_schema=PLANE_CREATE_WORK_ITEM_SCHEMA,
    handler=plane_create_work_item_handler,
)

PLANE_UPDATE_WORK_ITEM_SPEC = ToolSpec(
    name="plane_update_work_item",
    description=(
        "Update one work item only if expected_updated_at still matches; "
        "enforces dependency gates and read-back."
    ),
    input_schema=PLANE_UPDATE_WORK_ITEM_SCHEMA,
    handler=plane_update_work_item_handler,
)

PLANE_ADD_COMMENT_SPEC = ToolSpec(
    name="plane_add_comment",
    description=(
        "Add one durable plain-text comment after conflict and duplicate "
        "checks, then verify it by read-back."
    ),
    input_schema=PLANE_ADD_COMMENT_SCHEMA,
    handler=plane_add_comment_handler,
)

PLANE_ADD_LINK_SPEC = ToolSpec(
    name="plane_add_link",
    description=(
        "Add one credential-free http(s) link after conflict and duplicate "
        "checks, then verify it by read-back."
    ),
    input_schema=PLANE_ADD_LINK_SCHEMA,
    handler=plane_add_link_handler,
)

PLANE_SPECS = [
    PLANE_LIST_PROJECTS_SPEC,
    PLANE_LIST_STATES_SPEC,
    PLANE_LIST_MEMBERS_SPEC,
    PLANE_LIST_LABELS_SPEC,
    PLANE_LIST_WORK_ITEMS_SPEC,
    PLANE_GET_WORK_ITEM_SPEC,
    PLANE_CREATE_WORK_ITEM_SPEC,
    PLANE_UPDATE_WORK_ITEM_SPEC,
    PLANE_ADD_COMMENT_SPEC,
    PLANE_ADD_LINK_SPEC,
]
