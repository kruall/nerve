"""Handler package — exports :func:`build_default_registry`.

The default registry is constructed at engine startup and contains every
shipped Nerve tool. HoA tools are always registered; adapters filter
them at the build-session-server boundary based on the runtime config.
"""

from __future__ import annotations

from nerve.agent.tools.registry import ToolRegistry

from nerve.agent.tools.handlers.discord import DISCORD_SPECS
from nerve.agent.tools.handlers.hoa import HOA_SPECS
from nerve.agent.tools.handlers.mcp_admin import MCP_ADMIN_SPECS
from nerve.agent.tools.handlers.memory import MEMORY_SPECS
from nerve.agent.tools.handlers.long_commands import LONG_COMMAND_SPECS
from nerve.agent.tools.handlers.model_routing import MODEL_ROUTING_SPECS
from nerve.agent.tools.handlers.notifications import NOTIFICATION_SPECS
from nerve.agent.tools.handlers.plans import PLAN_SPECS
from nerve.agent.tools.handlers.restart import RESTART_SPECS
from nerve.agent.tools.handlers.skills import SKILL_SPECS
from nerve.agent.tools.handlers.sources import SOURCE_SPECS
from nerve.agent.tools.handlers.tasks import TASK_SPECS
from nerve.agent.tools.handlers.wakeups import WAKEUP_SPECS


def build_default_registry() -> ToolRegistry:
    """Construct the registry preloaded with every shipped tool.

    Order is irrelevant — :class:`ToolRegistry` indexes by name. We
    register HoA last only so it's clear they sit at the end of the
    list-returned-to-adapter (which decides whether to include them based
    on the ``include_hoa`` flag).
    """
    registry = ToolRegistry()
    for spec in (
        *TASK_SPECS,
        *MEMORY_SPECS,
        *LONG_COMMAND_SPECS,
        *SOURCE_SPECS,
        *PLAN_SPECS,
        *SKILL_SPECS,
        *NOTIFICATION_SPECS,
        *DISCORD_SPECS,
        *MCP_ADMIN_SPECS,
        *MODEL_ROUTING_SPECS,
        *WAKEUP_SPECS,
        *RESTART_SPECS,
        *HOA_SPECS,
    ):
        registry.register(spec)
    return registry


__all__ = ["build_default_registry"]
