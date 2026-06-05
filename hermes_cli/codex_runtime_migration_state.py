"""In-process Codex plugin/MCP migration status for fast /codex-runtime toggles.

The API server and gateway defer ``migrate()`` to a background worker so
slash handlers return after persisting ``openai_runtime``. Clients poll
``GET /v1/providers/codex/migration-status`` until ``phase`` is not ``running``.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class MigrationPhase(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass(frozen=True)
class MigrationStatus:
    phase: MigrationPhase
    message: str = ""

    def as_dict(self) -> dict:
        return {"phase": self.phase.value, "message": self.message}


_lock = threading.Lock()
_status_by_profile: dict[str, MigrationStatus] = {}


def migration_profile_key() -> str:
    try:
        from hermes_constants import get_hermes_home

        return str(get_hermes_home())
    except Exception:
        return "default"


def get_status(profile_key: Optional[str] = None) -> MigrationStatus:
    key = profile_key or migration_profile_key()
    with _lock:
        return _status_by_profile.get(key, MigrationStatus(MigrationPhase.IDLE))


def _set_status(profile_key: str, status: MigrationStatus) -> None:
    with _lock:
        _status_by_profile[profile_key] = status


def mark_idle(profile_key: Optional[str] = None) -> None:
    key = profile_key or migration_profile_key()
    _set_status(key, MigrationStatus(MigrationPhase.IDLE))


def mark_running(
    profile_key: Optional[str] = None,
    message: str = "Finishing Codex setup…",
) -> None:
    key = profile_key or migration_profile_key()
    _set_status(key, MigrationStatus(MigrationPhase.RUNNING, message))


def mark_complete(
    profile_key: Optional[str] = None,
    message: str = "Codex setup complete.",
) -> None:
    key = profile_key or migration_profile_key()
    _set_status(key, MigrationStatus(MigrationPhase.COMPLETE, message))


def mark_failed(
    profile_key: Optional[str] = None,
    message: str = "Codex setup failed.",
) -> None:
    key = profile_key or migration_profile_key()
    _set_status(key, MigrationStatus(MigrationPhase.FAILED, message))


def format_migration_summary(report) -> str:
    """One-line summary for status polling after background migration."""
    parts: list[str] = []
    user_servers = [s for s in report.migrated if s != "hermes-tools"]
    if user_servers:
        parts.append(f"{len(user_servers)} MCP server(s)")
    if report.migrated_plugins:
        parts.append(f"{len(report.migrated_plugins)} Codex plugin(s)")
    if report.wrote_permissions_default:
        parts.append(f"sandbox {report.wrote_permissions_default}")
    if "hermes-tools" in report.migrated:
        parts.append("Hermes tools callback")
    if report.errors:
        return "Codex setup finished with warnings."
    if parts:
        return "Codex setup complete (" + ", ".join(parts) + ")."
    return "Codex setup complete."


def run_plugin_migration(hermes_config: dict):
    from hermes_cli.codex_runtime_plugin_migration import migrate

    return migrate(hermes_config)


def schedule_plugin_migration(
    hermes_config: dict,
    *,
    profile_key: Optional[str] = None,
) -> bool:
    """Start background migration unless one is already running for this profile."""
    key = profile_key or migration_profile_key()
    with _lock:
        current = _status_by_profile.get(key)
        if current and current.phase == MigrationPhase.RUNNING:
            return False

    mark_running(key)

    config_snapshot = dict(hermes_config)

    def _worker() -> None:
        try:
            report = run_plugin_migration(config_snapshot)
            mark_complete(key, format_migration_summary(report))
        except Exception as exc:
            mark_failed(key, f"Codex setup failed: {exc}")

    threading.Thread(
        target=_worker,
        daemon=True,
        name="codex-runtime-migration",
    ).start()
    return True
