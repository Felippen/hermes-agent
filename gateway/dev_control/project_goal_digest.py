"""Compact markdown digests for Dev project goal trees."""

from __future__ import annotations

from typing import Any, Dict, List


def format_goal_tree_digest(
    tree: Dict[str, Any],
    *,
    max_roots: int = 3,
    max_depth: int = 4,
) -> str:
    """Render a token-budget-friendly goal tree summary for chat overlays."""
    roots = tree.get("roots") or []
    if not roots:
        return "## Project goals\n\nNo project goals defined."

    lines: List[str] = ["## Project goals", ""]
    project_id = str(tree.get("project_id") or "").strip()
    if project_id:
        lines.append(f"Project: `{project_id}`")
        lines.append("")

    for root in roots[:max_roots]:
        _append_node(lines, root, depth=0, max_depth=max_depth)

    total = int(tree.get("total") or 0)
    if total > max_roots:
        lines.append(f"\n… {total - max_roots} more root nodes not shown")
    return "\n".join(lines).strip()


def _append_node(
    lines: List[str],
    node: Dict[str, Any],
    *,
    depth: int,
    max_depth: int,
) -> None:
    if depth > max_depth:
        return
    indent = "  " * depth
    kind = str(node.get("kind") or "?")
    title = str(node.get("title") or "").strip() or "(untitled)"
    status = str(node.get("status") or "?")
    progress = float(node.get("progress") or 0.0)
    goal_id = str(node.get("goal_id") or "")
    lines.append(f"{indent}- **{kind}** {title} [{status}, {progress:.0%}] `{goal_id}`")
    plan_artifact_id = str(node.get("plan_artifact_id") or "").strip()
    if plan_artifact_id:
        lines.append(f"{indent}  - plan_artifact: `{plan_artifact_id}`")
    payload = node.get("payload") if isinstance(node.get("payload"), dict) else {}
    plan_id = str(payload.get("plan_id") or "").strip()
    if plan_id:
        lines.append(f"{indent}  - plan_id: `{plan_id}`")

    for child in (node.get("children") or [])[:8]:
        _append_node(lines, child, depth=depth + 1, max_depth=max_depth)
