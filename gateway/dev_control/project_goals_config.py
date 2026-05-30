"""Configuration helpers for Dev project goals (v2)."""

from __future__ import annotations

import os
from typing import Any, Dict


def _dev_project_goals_config() -> Dict[str, Any]:
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        dev = cfg.get("dev") if isinstance(cfg.get("dev"), dict) else {}
        section = dev.get("project_goals") if isinstance(dev.get("project_goals"), dict) else {}
        return dict(section)
    except Exception:
        return {}


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def project_goals_tick_enabled() -> bool:
    section = _dev_project_goals_config()
    if "tick_enabled" in section:
        return _truthy(section.get("tick_enabled"))
    return _truthy(os.getenv("HERMES_DEV_PROJECT_GOALS_TICK"))


def project_goals_auto_subgoal_enabled() -> bool:
    section = _dev_project_goals_config()
    if "auto_subgoal_on_approve" in section:
        return _truthy(section.get("auto_subgoal_on_approve"))
    return _truthy(os.getenv("HERMES_DEV_PROJECT_GOALS_AUTO_SUBGOAL"))
