"""Hermes-owned Gmail OAuth connection flow.

This module backs the Oryn Workspace "Connect Gmail" path. Workspace opens the
authorization URL and polls status; Hermes owns pending OAuth state, callback
exchange, token persistence, and all Gmail API access.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from hermes_constants import display_hermes_home, get_hermes_home


GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
]

DEFAULT_CLIENT_SECRET_NAME = "google_client_secret.json"
DEFAULT_TOKEN_NAME = "google_token.json"
DEFAULT_PENDING_NAME = "google_gmail_oauth_pending.json"


class GmailOAuthError(RuntimeError):
    """Expected Gmail OAuth setup failure surfaced by the API server."""

    def __init__(self, message: str, *, status: str = "failed"):
        super().__init__(message)
        self.status = status


def _configured_path(env_name: str, default: Path) -> Path:
    value = os.environ.get(env_name, "").strip()
    return Path(value).expanduser() if value else default


def client_secret_path() -> Path:
    return _configured_path(
        "HERMES_GMAIL_CLIENT_SECRETS_PATH",
        get_hermes_home() / DEFAULT_CLIENT_SECRET_NAME,
    )


def token_path() -> Path:
    return _configured_path("HERMES_GMAIL_TOKEN_PATH", get_hermes_home() / DEFAULT_TOKEN_NAME)


def pending_path() -> Path:
    return _configured_path(
        "HERMES_GMAIL_OAUTH_PENDING_PATH",
        get_hermes_home() / DEFAULT_PENDING_NAME,
    )


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _secure_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    with os.fdopen(os.open(tmp, flags, 0o600), "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    os.chmod(path, 0o600)


def _normalize_authorized_user_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    if not normalized.get("type"):
        normalized["type"] = "authorized_user"
    return normalized


def _client_secret_available() -> bool:
    path = client_secret_path()
    payload = _read_json(path)
    return path.exists() and ("installed" in payload or "web" in payload)


def _token_payload() -> Dict[str, Any]:
    return _read_json(token_path())


def _pending_payload() -> Dict[str, Any]:
    return _read_json(pending_path())


def _missing_scopes(granted: Iterable[str]) -> list[str]:
    granted_set = {scope for scope in granted if scope}
    return sorted(scope for scope in GMAIL_SCOPES if scope not in granted_set)


def _status_from_token_payload(payload: Dict[str, Any]) -> tuple[bool, list[str]]:
    if not payload:
        return False, []
    if payload.get("refresh_token") or payload.get("token"):
        raw_scopes = payload.get("scopes") or payload.get("scope") or GMAIL_SCOPES
        if isinstance(raw_scopes, str):
            scopes = raw_scopes.split()
        elif isinstance(raw_scopes, list):
            scopes = [str(scope) for scope in raw_scopes]
        else:
            scopes = list(GMAIL_SCOPES)
        return True, _missing_scopes(scopes)
    return False, []


def gmail_oauth_status() -> Dict[str, Any]:
    token_exists, missing_scopes = _status_from_token_payload(_token_payload())
    pending = _pending_payload()
    client_configured = _client_secret_available()

    if token_exists:
        status = "connected" if not missing_scopes else "partial"
        message = "Gmail is connected for this Hermes profile."
    elif pending.get("state"):
        status = "pending"
        message = "Gmail authorization is waiting for Google callback."
    elif not client_configured:
        status = "configuration_needed"
        message = "Hermes has no Gmail OAuth client configuration for this profile."
    else:
        status = "not_connected"
        message = "Gmail is not connected for this Hermes profile."

    return {
        "object": "gmail.oauth_status",
        "provider": "gmail",
        "status": status,
        "connected": token_exists and not missing_scopes,
        "configured": client_configured,
        "pending": bool(pending.get("state")),
        "missing_scopes": missing_scopes,
        "message": message,
        "token_location": f"{display_hermes_home()}/{token_path().name}" if token_exists else None,
    }


def _flow_from_client_secret(*, scopes: list[str], redirect_uri: str, **kwargs: Any):
    try:
        from google_auth_oauthlib.flow import Flow
    except ImportError as exc:
        try:
            from tools.lazy_deps import FeatureUnavailable, ensure

            ensure("skill.google_workspace", prompt=False)
            from google_auth_oauthlib.flow import Flow
        except FeatureUnavailable as lazy_exc:
            raise GmailOAuthError(
                f"Google OAuth dependencies are not available: {lazy_exc}",
                status="dependencies_missing",
            ) from lazy_exc
        except ImportError as retry_exc:
            raise GmailOAuthError(
                "Google OAuth dependencies are not installed. Install hermes-agent[google].",
                status="dependencies_missing",
            ) from retry_exc
        except Exception as install_exc:
            raise GmailOAuthError(
                f"Google OAuth dependencies could not be prepared: {install_exc}",
                status="dependencies_missing",
            ) from install_exc

    return Flow.from_client_secrets_file(
        str(client_secret_path()),
        scopes=scopes,
        redirect_uri=redirect_uri,
        **kwargs,
    )


def start_gmail_oauth(redirect_uri: str) -> Dict[str, Any]:
    if not _client_secret_available():
        raise GmailOAuthError(
            "Hermes has no Gmail OAuth client configuration for this profile.",
            status="configuration_needed",
        )

    flow = _flow_from_client_secret(
        scopes=list(GMAIL_SCOPES),
        redirect_uri=redirect_uri,
        autogenerate_code_verifier=True,
    )
    auth_url, state = flow.authorization_url(access_type="offline", prompt="consent")
    code_verifier = getattr(flow, "code_verifier", None)
    if not state or not code_verifier:
        raise GmailOAuthError("Could not create a Gmail OAuth PKCE session.")

    _secure_write_json(
        pending_path(),
        {
            "state": state,
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
            "created_at": time.time(),
        },
    )
    return {
        "object": "gmail.oauth_start",
        "provider": "gmail",
        "status": "pending",
        "authorization_url": auth_url,
        "redirect_uri": redirect_uri,
        "message": "Open the authorization_url to connect Gmail.",
    }


def complete_gmail_oauth_callback(
    *,
    code: Optional[str],
    state: Optional[str],
    granted_scope: Optional[str] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    if error:
        raise GmailOAuthError(f"Google OAuth failed: {error}", status="failed")
    if not code:
        raise GmailOAuthError("Missing Gmail OAuth authorization code.", status="failed")

    pending = _pending_payload()
    if not pending.get("state") or not pending.get("code_verifier"):
        raise GmailOAuthError("No pending Gmail OAuth session for this Hermes profile.", status="not_connected")
    if state != pending["state"]:
        raise GmailOAuthError("Gmail OAuth state mismatch.", status="failed")

    granted_scopes = granted_scope.split() if granted_scope else list(GMAIL_SCOPES)
    flow = _flow_from_client_secret(
        scopes=granted_scopes,
        redirect_uri=str(pending.get("redirect_uri") or ""),
        state=pending["state"],
        code_verifier=pending["code_verifier"],
    )

    previous_relax = os.environ.get("OAUTHLIB_RELAX_TOKEN_SCOPE")
    os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"
    try:
        flow.fetch_token(code=code)
    except Exception as exc:
        raise GmailOAuthError(f"Gmail OAuth token exchange failed: {exc}", status="failed") from exc
    finally:
        if previous_relax is None:
            os.environ.pop("OAUTHLIB_RELAX_TOKEN_SCOPE", None)
        else:
            os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = previous_relax

    creds = flow.credentials
    token_payload = _normalize_authorized_user_payload(json.loads(creds.to_json()))
    actual_scopes = list(getattr(creds, "granted_scopes", None) or [])
    if actual_scopes:
        token_payload["scopes"] = actual_scopes
    elif granted_scopes:
        token_payload["scopes"] = granted_scopes

    _secure_write_json(token_path(), token_payload)
    pending_path().unlink(missing_ok=True)
    _, missing_scopes = _status_from_token_payload(token_payload)
    return {
        "object": "gmail.oauth_callback",
        "provider": "gmail",
        "status": "connected" if not missing_scopes else "partial",
        "connected": not missing_scopes,
        "missing_scopes": missing_scopes,
        "message": "Gmail is connected for this Hermes profile.",
    }
