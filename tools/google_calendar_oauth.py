"""Hermes-owned Google Calendar OAuth readiness helpers.

Calendar reuses the Google authorized-user token managed by the Gmail/Oryn
connection flow. This module reports Calendar scope health without exposing
token or client-secret contents.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable

from hermes_constants import display_hermes_home
from tools.gmail_oauth import (
    GMAIL_SCOPES,
    _client_secret_available,
    _pending_payload,
    _status_from_token_payload,
    _token_payload,
    start_gmail_oauth,
    token_path,
)


GOOGLE_CALENDAR_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
]

GOOGLE_CALENDAR_OAUTH_SCOPES = list(dict.fromkeys([*GMAIL_SCOPES, *GOOGLE_CALENDAR_SCOPES]))


def missing_calendar_scopes(granted: Iterable[str]) -> list[str]:
    granted_set = {scope for scope in granted if scope}
    return sorted(scope for scope in GOOGLE_CALENDAR_SCOPES if scope not in granted_set)


def _token_status(payload: Dict[str, Any]) -> tuple[bool, list[str]]:
    token_exists, _gmail_missing = _status_from_token_payload(payload)
    if not token_exists:
        return False, []
    raw_scopes = payload.get("scopes") or payload.get("scope") or []
    if isinstance(raw_scopes, str):
        scopes = raw_scopes.split()
    elif isinstance(raw_scopes, list):
        scopes = [str(scope) for scope in raw_scopes]
    else:
        scopes = []
    return True, missing_calendar_scopes(scopes)


def calendar_oauth_status() -> Dict[str, Any]:
    token_exists, missing_scopes = _token_status(_token_payload())
    pending = _pending_payload()
    client_configured = _client_secret_available()

    if token_exists and not missing_scopes:
        status = "connected"
        message = "Google Calendar is connected for this Hermes profile."
    elif token_exists:
        status = "missing_scopes"
        message = "Google OAuth is connected, but Calendar scope has not been granted."
    elif pending.get("state"):
        status = "pending"
        message = "Google authorization is waiting for callback."
    elif not client_configured:
        status = "configuration_needed"
        message = "Hermes has no Google OAuth client configuration for this profile."
    else:
        status = "not_connected"
        message = "Google Calendar is not connected for this Hermes profile."

    return {
        "object": "google_calendar.oauth_status",
        "provider": "google_calendar",
        "status": status,
        "connected": token_exists and not missing_scopes,
        "configured": client_configured,
        "pending": bool(pending.get("state")),
        "missing_scopes": missing_scopes,
        "message": message,
        "token_location": f"{display_hermes_home()}/{token_path().name}" if token_exists else None,
    }


def start_calendar_oauth(redirect_uri: str) -> Dict[str, Any]:
    """Start a Google OAuth flow that grants Calendar access for Oryn Workspace."""

    return start_gmail_oauth(
        redirect_uri,
        scopes=GOOGLE_CALENDAR_OAUTH_SCOPES,
        provider="google_calendar",
        object_name="google_calendar.oauth_start",
        message="Open the authorization_url to connect Google Calendar.",
    )
