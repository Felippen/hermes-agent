"""Helpers for recovering from a deleted process cwd in tool setup paths."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Iterable


def _existing_dir(candidate: str | os.PathLike[str] | None) -> str | None:
    if not candidate:
        return None
    try:
        expanded = os.path.expanduser(os.fspath(candidate))
    except (TypeError, ValueError):
        return None
    if os.path.isdir(expanded):
        return expanded
    return None


def _fallback_dirs() -> Iterable[str]:
    yield str(Path.home())
    yield tempfile.gettempdir()


def safe_getcwd() -> str | None:
    """Return the process cwd, or ``None`` when it has been deleted."""

    try:
        return os.getcwd()
    except FileNotFoundError:
        return None
    except OSError:
        return None


def resolve_tool_cwd(
    *preferred: str | os.PathLike[str] | None,
    include_process_cwd: bool = True,
) -> str:
    """Return a valid cwd for tool setup and subprocess launches.

    Preferred candidates are checked first, then the current process cwd, then
    stable local fallbacks. This function never calls raw ``os.getcwd()`` without
    handling the deleted-cwd failure mode.
    """

    for candidate in preferred:
        resolved = _existing_dir(candidate)
        if resolved:
            return resolved

    if include_process_cwd:
        resolved = safe_getcwd()
        if resolved:
            return resolved

    for candidate in _fallback_dirs():
        resolved = _existing_dir(candidate)
        if resolved:
            return resolved

    return tempfile.gettempdir()


def safe_abspath(path: str | os.PathLike[str], base: str | None = None) -> str:
    """Return an absolute path without depending on a valid process cwd."""

    expanded = os.path.expanduser(os.fspath(path))
    if os.path.isabs(expanded):
        return os.path.abspath(expanded)
    safe_base = resolve_tool_cwd(base)
    return os.path.abspath(os.path.join(safe_base, expanded))
