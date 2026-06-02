"""Persist chat attachments uploaded from Oryn Workspace onto the API-server host."""

from __future__ import annotations

import re
import uuid
from pathlib import Path

MAX_WORKSPACE_ATTACHMENT_BYTES = 25 * 1024 * 1024


def _sanitize_filename(name: str) -> str:
    cleaned = Path(name or "attachment").name.strip()
    if not cleaned or cleaned in {".", ".."}:
        return "attachment.bin"
    return cleaned


def save_workspace_attachment(
    *,
    session_id: str,
    original_filename: str,
    data: bytes,
    mime_type: str | None = None,
) -> dict[str, str]:
    """Write an uploaded attachment under HERMES_HOME and register it for tool access."""
    cleaned_session_id = session_id.strip()
    if (
        not cleaned_session_id
        or len(cleaned_session_id) > 128
        or re.search(r"[\r\n\x00/\\]", cleaned_session_id)
    ):
        raise ValueError("invalid session_id")
    if not data:
        raise ValueError("attachment body is empty")
    if len(data) > MAX_WORKSPACE_ATTACHMENT_BYTES:
        raise ValueError(
            f"attachment exceeds {MAX_WORKSPACE_ATTACHMENT_BYTES} bytes"
        )

    from hermes_constants import get_hermes_dir, get_hermes_home
    from tools.credential_files import register_credential_file

    safe_name = _sanitize_filename(original_filename)
    ext = Path(safe_name).suffix
    attachment_id = uuid.uuid4().hex
    dest_name = f"{attachment_id}{ext}" if ext else attachment_id

    base = (
        get_hermes_dir("cache/documents", "document_cache")
        / "workspace"
        / cleaned_session_id.lower()
    )
    base.mkdir(parents=True, exist_ok=True)
    dest = base / dest_name
    dest.write_bytes(data)

    hermes_home = get_hermes_home().resolve()
    rel = dest.resolve().relative_to(hermes_home)
    register_credential_file(str(rel))

    return {
        "id": attachment_id,
        "filename": safe_name,
        "mime_type": (mime_type or "application/octet-stream").strip(),
        "path": str(dest.resolve()),
    }
