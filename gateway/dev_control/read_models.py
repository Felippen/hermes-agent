"""Oryn-facing Dev control-plane read-model helpers."""

from __future__ import annotations

import time
from typing import Any, Dict, Iterable, Optional


BOARD_LANES = ("queued", "running", "needs_input", "failed", "completed")


def build_agent_board_response(
    items: Iterable[Dict[str, Any]],
    *,
    updated_at: Optional[float] = None,
) -> Dict[str, Any]:
    data = list(items)
    lanes = {
        lane: sum(1 for item in data if item.get("lane") == lane)
        for lane in BOARD_LANES
    }
    groups_by_key: Dict[str, Dict[str, Any]] = {}
    for item in data:
        group_key = str(item.get("group_key") or "runtime:unknown")
        group = groups_by_key.setdefault(group_key, {
            "key": group_key,
            "label": item.get("group_label") or "Unknown",
            "kind": item.get("group_kind") or "runtime",
            "count": 0,
            "attention_count": 0,
        })
        group["count"] += 1
        if item.get("lane") in {"needs_input", "failed"}:
            group["attention_count"] += 1
    return {
        "object": "list",
        "data": data,
        "total": len(data),
        "lanes": lanes,
        "groups": list(groups_by_key.values()),
        "attention_count": sum(1 for item in data if item.get("lane") in {"needs_input", "failed"}),
        "updated_at": updated_at or time.time(),
    }


def build_dev_plans_response(plans: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    data = list(plans)
    return {"object": "list", "data": data, "total": len(data)}


def build_worker_detail_metadata(item: Dict[str, Any]) -> Dict[str, Any]:
    return dict(item)

