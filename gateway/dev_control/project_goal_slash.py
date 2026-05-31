"""Gateway slash commands for durable Dev project goals."""

from __future__ import annotations

import shlex
from typing import Any, Callable, Dict, List, Optional, Tuple

from gateway.dev_control.project_goal_eval import reevaluate_project_goal
from gateway.dev_control.project_goals import (
    DevProjectGoalStore,
    abandon_project_goal,
    create_project_goal,
    get_project_goal_tree,
    list_project_goals,
    update_project_goal,
)
from gateway.dev_control.project_scope import DEFAULT_PROJECT_ID, resolve_project_id

_KIND_ALIASES = {
    "vision": "vision",
    "goal": "goal",
    "pgoal": "goal",
    "milestone": "milestone",
    "subgoal": "subgoal",
    "psubgoal": "subgoal",
}


def _parse_args(raw: str) -> Tuple[Dict[str, str], List[str]]:
    flags: Dict[str, str] = {}
    positionals: List[str] = []
    if not (raw or "").strip():
        return flags, positionals
    try:
        tokens = shlex.split(raw)
    except ValueError as exc:
        raise ValueError(f"Could not parse command arguments: {exc}") from exc
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--"):
            key = token[2:]
            if index + 1 >= len(tokens):
                raise ValueError(f"Missing value for --{key}")
            flags[key.replace("-", "_")] = tokens[index + 1]
            index += 2
            continue
        positionals.append(token)
        index += 1
    return flags, positionals


def _format_tree(nodes: List[Dict[str, Any]], indent: int = 0) -> List[str]:
    lines: List[str] = []
    for node in nodes:
        prefix = "  " * indent
        progress = float(node.get("progress") or 0.0)
        lines.append(
            f"{prefix}{node.get('kind', '?')}: {node.get('title', '')} "
            f"[{node.get('status', '?')}, {progress:.0%}] ({node.get('goal_id', '')})"
        )
        children = node.get("children") or []
        if children:
            lines.extend(_format_tree(children, indent + 1))
    return lines


def _project_id(flags: Dict[str, str]) -> str:
    return resolve_project_id(flags.get("project_id") or DEFAULT_PROJECT_ID)


def _create_kind(
    *,
    store: DevProjectGoalStore,
    kind: str,
    flags: Dict[str, str],
    positionals: List[str],
) -> Dict[str, Any]:
    title = " ".join(positionals).strip()
    if not title:
        raise ValueError(f"Provide a title for the {kind}.")
    return create_project_goal(
        store=store,
        kind=kind,
        title=title,
        project_id=_project_id(flags),
        parent_goal_id=flags.get("parent") or flags.get("parent_goal_id"),
        plan_artifact_id=flags.get("plan_artifact_id") or flags.get("plan_artifact"),
        status=flags.get("status") or "active",
        markdown=flags.get("markdown") or "",
    )


def dispatch_project_goal_slash(
    canonical: str,
    cmd_arg: str,
    *,
    goal_store: DevProjectGoalStore,
    verification_store: Any = None,
    execution_store: Any = None,
    plan_artifact_store: Any = None,
) -> Dict[str, Any]:
    """Return a structured slash result dict for project goal commands."""
    name = (canonical or "").strip().lower()
    try:
        flags, positionals = _parse_args(cmd_arg)
    except ValueError as exc:
        return {"type": "error", "message": str(exc)}

    if name in _KIND_ALIASES and name != "project":
        kind = _KIND_ALIASES[name]
        try:
            created = _create_kind(store=goal_store, kind=kind, flags=flags, positionals=positionals)
        except ValueError as exc:
            return {"type": "error", "message": str(exc)}
        return {
            "type": "text",
            "content": (
                f"✓ Created {kind} `{created.get('goal_id')}`: {created.get('title')} "
                f"({float(created.get('progress') or 0.0):.0%})"
            ),
        }

    if name != "project":
        return {"type": "error", "message": f"Unknown project goal command: /{name}"}

    subcommand = (positionals[0].lower() if positionals else "status")
    rest = positionals[1:]

    if subcommand in {"help", ""}:
        return {
            "type": "text",
            "content": (
                "**Dev project goals**\n\n"
                "- `/project tree` — goal hierarchy\n"
                "- `/project list [kind]` — flat list\n"
                "- `/project create <kind> <title> [--parent ID]`\n"
                "- `/project abandon <goal_id>`\n"
                "- `/project update <goal_id> [--title T] [--status S]`\n"
                "- `/project reevaluate <goal_id>`\n"
                "- `/vision`, `/milestone`, `/pgoal`, `/psubgoal <title>` — quick create\n\n"
                "Session `/goal` is separate (turn loop). Use `/pgoal` for project goals."
            ),
        }

    if subcommand == "status":
        tree = get_project_goal_tree(store=goal_store, project_id=_project_id(flags))
        roots = tree.get("roots") or []
        if not roots:
            return {"type": "text", "content": "No project goals yet. Try `/vision <title>`."}
        lines = ["**Project goal tree**", ""]
        lines.extend(_format_tree(roots))
        return {"type": "text", "content": "\n".join(lines)}

    if subcommand == "tree":
        tree = get_project_goal_tree(store=goal_store, project_id=_project_id(flags))
        roots = tree.get("roots") or []
        if not roots:
            return {"type": "text", "content": "No project goals yet."}
        return {"type": "text", "content": "\n".join(_format_tree(roots))}

    if subcommand == "list":
        kind = rest[0] if rest else flags.get("kind")
        result = list_project_goals(
            store=goal_store,
            project_id=_project_id(flags),
            kind=kind,
            include_abandoned=_truthy_flag(flags.get("include_abandoned")),
        )
        rows = result.get("data") or []
        if not rows:
            return {"type": "text", "content": "No matching project goals."}
        lines = ["**Project goals**", ""]
        for row in rows:
            progress = float(row.get("progress") or 0.0)
            lines.append(
                f"- {row.get('kind', '?')} [{row.get('status', '?')}, {progress:.0%}] "
                f"{row.get('title', '')} (`{row.get('goal_id', '')}`)"
            )
        return {"type": "text", "content": "\n".join(lines)}

    if subcommand == "create":
        if not rest or rest[0] not in _KIND_ALIASES.values():
            return {"type": "error", "message": "Usage: /project create <vision|goal|milestone|subgoal> <title>"}
        kind = rest[0]
        try:
            created = _create_kind(store=goal_store, kind=kind, flags=flags, positionals=rest[1:])
        except ValueError as exc:
            return {"type": "error", "message": str(exc)}
        return {
            "type": "text",
            "content": f"✓ Created {kind} `{created.get('goal_id')}`: {created.get('title')}",
        }

    if subcommand == "abandon":
        goal_id = (rest[0] if rest else flags.get("goal_id") or "").strip()
        if not goal_id:
            return {"type": "error", "message": "Usage: /project abandon <goal_id>"}
        try:
            abandoned = abandon_project_goal(store=goal_store, goal_id=goal_id)
        except KeyError:
            return {"type": "error", "message": f"Project goal not found: {goal_id}"}
        return {"type": "text", "content": f"✓ Abandoned `{abandoned.get('goal_id')}`: {abandoned.get('title')}"}

    if subcommand == "update":
        goal_id = (rest[0] if rest else flags.get("goal_id") or "").strip()
        if not goal_id:
            return {"type": "error", "message": "Usage: /project update <goal_id> [--title T] [--status S]"}
        try:
            updated = update_project_goal(
                store=goal_store,
                goal_id=goal_id,
                title=flags.get("title"),
                status=flags.get("status"),
                markdown=flags.get("markdown"),
                parent_goal_id=flags.get("parent") or flags.get("parent_goal_id"),
                plan_artifact_id=flags.get("plan_artifact_id") or flags.get("plan_artifact"),
                ordering=int(flags["ordering"]) if flags.get("ordering") else None,
            )
        except KeyError:
            return {"type": "error", "message": f"Project goal not found: {goal_id}"}
        except ValueError as exc:
            return {"type": "error", "message": str(exc)}
        return {
            "type": "text",
            "content": (
                f"✓ Updated `{updated.get('goal_id')}`: {updated.get('title')} "
                f"[{updated.get('status')}, {float(updated.get('progress') or 0.0):.0%}]"
            ),
        }

    if subcommand == "reevaluate":
        goal_id = (rest[0] if rest else flags.get("goal_id") or "").strip()
        if not goal_id:
            return {"type": "error", "message": "Usage: /project reevaluate <goal_id>"}
        try:
            result = reevaluate_project_goal(
                store=goal_store,
                goal_id=goal_id,
                verification_store=verification_store,
                execution_store=execution_store,
                plan_artifact_store=plan_artifact_store,
            )
        except KeyError:
            return {"type": "error", "message": f"Project goal not found: {goal_id}"}
        except ValueError as exc:
            return {"type": "error", "message": str(exc)}
        return {
            "type": "text",
            "content": (
                f"Re-evaluation for `{goal_id}`: {result.get('verdict')} — "
                f"{result.get('reason') or 'no reason'}"
            ),
        }

    return {"type": "error", "message": f"Unknown /project subcommand: {subcommand}"}


def _truthy_flag(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


PROJECT_GOAL_SLASH_COMMANDS = frozenset({"project", "vision", "milestone", "pgoal", "psubgoal"})
