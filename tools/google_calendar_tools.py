"""Google Calendar-backed calendar tools for Hermes/Oryn."""

from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from hermes_constants import get_hermes_home
from tools.google_calendar_oauth import GOOGLE_CALENDAR_SCOPES, calendar_oauth_status
from tools.google_calendar_store import GoogleCalendarCache, normalize_google_event
from tools.registry import registry


CALENDAR_TOOLSET = "google_calendar"
CALENDAR_LIST_LIMIT_MAX = 500
CALENDAR_SYNC_LIMIT_MAX = 2500
CALENDAR_LIST_CACHE_TTL_SECONDS = 30.0
CALENDAR_READ_CACHE_TTL_SECONDS = 300.0


class CalendarError(RuntimeError):
    """Expected calendar operation failure surfaced as a JSON error."""


class TTLCache:
    def __init__(self, ttl_seconds: float):
        self.ttl_seconds = ttl_seconds
        self._values: Dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any:
        now = time.monotonic()
        with self._lock:
            item = self._values.get(key)
            if item is None:
                return None
            created, value = item
            if now - created > self.ttl_seconds:
                self._values.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._values[key] = (time.monotonic(), value)

    def clear(self) -> None:
        with self._lock:
            self._values.clear()


_list_cache = TTLCache(CALENDAR_LIST_CACHE_TTL_SECONDS)
_read_cache = TTLCache(CALENDAR_READ_CACHE_TTL_SECONDS)


def _calendar_cache() -> GoogleCalendarCache:
    return GoogleCalendarCache()


@dataclass(frozen=True)
class GoogleCalendarAccount:
    account_id: str
    email: str
    display_name: str
    is_default: bool
    enabled: bool
    status: str
    token_path: str
    client_secrets_path: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "provider": "google_calendar",
            "email": self.email,
            "display_name": self.display_name,
            "is_default": self.is_default,
            "enabled": self.enabled,
            "status": self.status,
        }


def _default_token_path() -> Path:
    return get_hermes_home() / "google_token.json"


def _default_client_secrets_path() -> Path:
    return get_hermes_home() / "google_client_secret.json"


def _configured_path(env_name: str, default: Path) -> Path:
    value = os.environ.get(env_name, "").strip()
    return Path(value).expanduser() if value else default


def _account_id(email: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.@+-]+", "-", email.strip().lower()) or "me"
    return f"google_calendar:{safe}"


def _configured_emails() -> List[str]:
    raw = (
        os.environ.get("HERMES_GOOGLE_CALENDAR_ACCOUNT_EMAILS", "").strip()
        or os.environ.get("HERMES_GMAIL_ACCOUNT_EMAILS", "").strip()
    )
    emails = [part.strip() for part in raw.split(",") if part.strip()]
    return emails or ["me"]


def _stored_token_scopes(token_path: str) -> List[str]:
    try:
        data = json.loads(Path(token_path).read_text(encoding="utf-8"))
    except Exception:
        return []
    scopes = data.get("scopes") or data.get("scope")
    if isinstance(scopes, str):
        return scopes.split()
    if isinstance(scopes, list):
        return [str(scope) for scope in scopes]
    return []


def _has_calendar_scope(token_path: Path) -> bool:
    scopes = set(_stored_token_scopes(str(token_path)))
    return all(scope in scopes for scope in GOOGLE_CALENDAR_SCOPES)


def load_calendar_accounts() -> List[GoogleCalendarAccount]:
    token_path = _configured_path("HERMES_GMAIL_TOKEN_PATH", _default_token_path())
    client_path = _configured_path(
        "HERMES_GMAIL_CLIENT_SECRETS_PATH",
        _default_client_secrets_path(),
    )
    token_exists = token_path.exists()
    client_exists = client_path.exists()
    if token_exists and _has_calendar_scope(token_path):
        status = "configured"
    elif token_exists:
        status = "missing_scopes"
    else:
        status = "unauthorized"
    if token_exists and not client_exists and status == "configured":
        status = "configured_without_client_secret"

    accounts = []
    for index, email in enumerate(_configured_emails()):
        accounts.append(
            GoogleCalendarAccount(
                account_id=_account_id(email),
                email=email,
                display_name=email,
                is_default=index == 0,
                enabled=token_exists and status != "missing_scopes",
                status=status,
                token_path=str(token_path),
                client_secrets_path=str(client_path),
            )
        )
    return accounts


def google_calendar_available() -> bool:
    return any(account.enabled for account in load_calendar_accounts())


def _resolve_account(account_id: Optional[str] = None) -> GoogleCalendarAccount:
    accounts = load_calendar_accounts()
    if not accounts:
        raise CalendarError("No Google Calendar accounts are configured")
    if account_id:
        for account in accounts:
            if account.account_id == account_id or account.email == account_id:
                if not account.enabled:
                    raise CalendarError("Google Calendar account is not authenticated")
                return account
        raise CalendarError(f"Unknown Google Calendar account: {account_id}")
    account = next((item for item in accounts if item.is_default), accounts[0])
    if not account.enabled:
        raise CalendarError("Google Calendar account is not authenticated")
    return account


def build_calendar_service(account: GoogleCalendarAccount):
    """Build a Google Calendar API service for an account.

    Kept small so tests can monkeypatch it with a fake service.
    """
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError as exc:
        try:
            from tools.lazy_deps import FeatureUnavailable, ensure

            ensure("skill.google_workspace", prompt=False)
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build
        except FeatureUnavailable as lazy_exc:
            raise CalendarError(f"Google API dependencies are not available: {lazy_exc}") from lazy_exc
        except ImportError as retry_exc:
            raise CalendarError("Google API dependencies are not installed") from retry_exc
        except Exception as install_exc:
            raise CalendarError(f"Google API dependencies could not be prepared: {install_exc}") from install_exc

    token_path = Path(account.token_path)
    if not token_path.exists():
        raise CalendarError("Google OAuth token is missing")

    creds = Credentials.from_authorized_user_file(
        str(token_path),
        _stored_token_scopes(str(token_path)) or list(GOOGLE_CALENDAR_SCOPES),
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_path.write_text(creds.to_json(), encoding="utf-8")
    if not creds.valid:
        raise CalendarError("Google OAuth token is invalid")
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def _execute(call: Any) -> Dict[str, Any]:
    result = call.execute()
    return result or {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _default_start() -> str:
    return (_now_utc() - timedelta(days=7)).isoformat().replace("+00:00", "Z")


def _default_end() -> str:
    return (_now_utc() + timedelta(days=60)).isoformat().replace("+00:00", "Z")


def _coerce_range(args: Dict[str, Any]) -> tuple[str, str]:
    start = str(args.get("start") or args.get("time_min") or args.get("from") or _default_start())
    end = str(args.get("end") or args.get("time_max") or args.get("to") or _default_end())
    return start, end


def _limit(args: Dict[str, Any], default: int = 250, maximum: int = CALENDAR_LIST_LIMIT_MAX) -> int:
    return max(1, min(int(args.get("limit") or args.get("max_results") or default), maximum))


def _calendar_id(args: Dict[str, Any]) -> str:
    return str(args.get("calendar_id") or args.get("calendar") or "primary")


def _require_approval(args: Dict[str, Any], operation: str) -> None:
    approved = bool(
        args.get("approved") or args.get("confirmed") or args.get("approval_confirmed")
    )
    if not approved:
        raise CalendarError(f"{operation} requires explicit approval")


def _calendar_from_google(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **item,
        "id": str(item.get("id") or "primary"),
        "summary": str(item.get("summary") or item.get("id") or "Calendar"),
    }


def _fetch_calendars(service: Any, account: GoogleCalendarAccount) -> List[Dict[str, Any]]:
    cache = _calendar_cache()
    calendars: List[Dict[str, Any]] = []
    page_token = ""
    synced_at = time.time()
    while True:
        result = _execute(
            service
            .calendarList()
            .list(pageToken=page_token or None, showHidden=True, minAccessRole="reader")
        )
        for item in result.get("items", []) or []:
            calendar = _calendar_from_google(item)
            calendars.append(calendar)
            cache.upsert_calendar(
                account_id=account.account_id,
                email=account.email,
                calendar=calendar,
                synced_at=synced_at,
            )
        page_token = str(result.get("nextPageToken") or "")
        if not page_token:
            break
    if not calendars:
        calendar = {"id": "primary", "summary": "Primary", "primary": True}
        cache.upsert_calendar(
            account_id=account.account_id,
            email=account.email,
            calendar=calendar,
            synced_at=synced_at,
        )
        calendars.append(calendar)
    return calendars


def list_calendar_accounts(args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    status = calendar_oauth_status()
    cache = _calendar_cache()
    accounts = []
    for account in load_calendar_accounts():
        data = account.to_dict()
        data["oauth"] = status
        calendars = cache.list_calendars(account_id=account.account_id)
        data["calendar_count"] = len(calendars)
        accounts.append(data)
    return {
        "object": "list",
        "provider": "google_calendar",
        "data": accounts,
        "configured": any(account["enabled"] for account in accounts),
        "oauth": status,
    }


def list_calendars(args: Dict[str, Any]) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    cache = _calendar_cache()
    cached = cache.list_calendars(account_id=account.account_id)
    if cached and not args.get("refresh"):
        return {
            "object": "list",
            "provider": "google_calendar",
            "account_id": account.account_id,
            "data": cached,
            "cache": "local_provider_cache",
            "cache_source": "local_provider_cache",
        }
    service = build_calendar_service(account)
    _fetch_calendars(service, account)
    return {
        "object": "list",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "data": cache.list_calendars(account_id=account.account_id),
        "cache": "updated",
        "cache_source": "google_calendar_api",
    }


def sync_calendar(args: Dict[str, Any]) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    start, end = _coerce_range(args)
    calendar_id = _calendar_id(args)
    limit = _limit(args, default=500, maximum=CALENDAR_SYNC_LIMIT_MAX)
    service = build_calendar_service(account)
    cache = _calendar_cache()
    if args.get("refresh_calendars", True):
        _fetch_calendars(service, account)
    synced_at = time.time()
    synced = 0
    next_page_token = str(args.get("page_token") or "")
    while synced < limit:
        result = _execute(
            service
            .events()
            .list(
                calendarId=calendar_id,
                timeMin=start,
                timeMax=end,
                singleEvents=True,
                showDeleted=True,
                orderBy="startTime",
                maxResults=min(2500, limit - synced),
                pageToken=next_page_token or None,
            )
        )
        events = result.get("items", []) or []
        for event in events:
            normalized = normalize_google_event(
                account_id=account.account_id,
                email=account.email,
                calendar_id=calendar_id,
                event=event,
                synced_at=synced_at,
            )
            cache.upsert_event(normalized)
            synced += 1
            if synced >= limit:
                break
        next_page_token = str(result.get("nextPageToken") or "")
        if not next_page_token or not events:
            break
    cache.record_sync_range(
        account_id=account.account_id,
        email=account.email,
        calendar_id=calendar_id,
        start=start,
        end=end,
        count=synced,
        synced_at=synced_at,
    )
    _list_cache.clear()
    _read_cache.clear()
    return {
        "object": "google_calendar.sync",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "email": account.email,
        "calendar_id": calendar_id,
        "start": start,
        "end": end,
        "synced": synced,
        "updated": synced,
        "next_page_token": next_page_token,
        "cache": "updated",
        "cache_source": "google_calendar_api",
        "synced_at": synced_at,
        "cache_status": cache.range_status(
            account_id=account.account_id,
            calendar_id=calendar_id,
            start=start,
            end=end,
        ),
    }


def _ensure_range(account: GoogleCalendarAccount, args: Dict[str, Any], start: str, end: str, calendar_id: str) -> None:
    cache = _calendar_cache()
    if cache.range_is_covered(
        account_id=account.account_id,
        calendar_id=calendar_id,
        start=start,
        end=end,
    ):
        return
    sync_calendar({**args, "account_id": account.account_id, "calendar_id": calendar_id, "start": start, "end": end})


def list_calendar_events(args: Dict[str, Any]) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    start, end = _coerce_range(args)
    calendar_id = _calendar_id(args)
    limit = _limit(args)
    page_token = str(args.get("page_token") or "")
    cache_key = json.dumps(
        {
            "op": "list",
            "account": account.account_id,
            "calendar": calendar_id,
            "start": start,
            "end": end,
            "limit": limit,
            "page": page_token,
        },
        sort_keys=True,
    )
    cached_payload = _list_cache.get(cache_key)
    if cached_payload is not None:
        return {**cached_payload, "cache": "hit"}
    _ensure_range(account, args, start, end, calendar_id)
    cache = _calendar_cache()
    rows, next_page_token = cache.list_events(
        account_id=account.account_id,
        calendar_id=calendar_id,
        start=start,
        end=end,
        include_deleted=bool(args.get("include_deleted")),
        limit=limit,
        page_token=page_token,
    )
    payload = {
        "object": "list",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "calendar_id": calendar_id,
        "start": start,
        "end": end,
        "data": rows,
        "next_page_token": next_page_token,
        "cache": "local_provider_cache",
        "cache_source": "local_provider_cache",
        "cache_status": cache.range_status(
            account_id=account.account_id,
            calendar_id=calendar_id,
            start=start,
            end=end,
        ),
    }
    _list_cache.set(cache_key, payload)
    return payload


def search_calendar_events(args: Dict[str, Any]) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    query = str(args.get("query") or args.get("q") or "")
    if not query:
        raise CalendarError("query is required")
    start, end = _coerce_range(args)
    calendar_id = _calendar_id(args)
    limit = _limit(args)
    page_token = str(args.get("page_token") or "")
    _ensure_range(account, args, start, end, calendar_id)
    cache = _calendar_cache()
    rows, next_page_token = cache.search_events(
        account_id=account.account_id,
        calendar_id=calendar_id,
        query=query,
        start=start,
        end=end,
        include_deleted=bool(args.get("include_deleted")),
        limit=limit,
        page_token=page_token,
    )
    return {
        "object": "list",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "calendar_id": calendar_id,
        "query": query,
        "start": start,
        "end": end,
        "data": rows,
        "next_page_token": next_page_token,
        "cache": "local_provider_cache",
        "cache_source": "local_provider_cache",
        "cache_status": cache.range_status(
            account_id=account.account_id,
            calendar_id=calendar_id,
            start=start,
            end=end,
        ),
    }


def read_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    calendar_id = _calendar_id(args)
    event_id = str(args.get("event_id") or args.get("message_id") or "")
    occurrence_id = str(args.get("occurrence_id") or "")
    if not event_id and not occurrence_id:
        raise CalendarError("event_id or occurrence_id is required")
    cache_key = f"{account.account_id}:{calendar_id}:{event_id}:{occurrence_id}"
    cached_payload = _read_cache.get(cache_key)
    if cached_payload is not None:
        return {**cached_payload, "cache": "hit"}
    cache = _calendar_cache()
    local = cache.get_event(
        account_id=account.account_id,
        calendar_id=calendar_id,
        event_id=event_id,
        occurrence_id=occurrence_id,
    )
    if local:
        payload = {
            "object": "google_calendar.event",
            "provider": "google_calendar",
            "account_id": account.account_id,
            **local,
            "cache": "local_provider_cache",
            "cache_source": "local_provider_cache",
        }
        _read_cache.set(cache_key, payload)
        return payload
    if not event_id:
        raise CalendarError("event_id is required for provider fallback")
    service = build_calendar_service(account)
    event = _execute(service.events().get(calendarId=calendar_id, eventId=event_id))
    normalized = normalize_google_event(
        account_id=account.account_id,
        email=account.email,
        calendar_id=calendar_id,
        event=event,
    )
    cache.upsert_event(normalized)
    payload = {
        "object": "google_calendar.event",
        "provider": "google_calendar",
        "account_id": account.account_id,
        **cache.get_event(
            account_id=account.account_id,
            calendar_id=calendar_id,
            event_id=event_id,
        ),
        "cache": "miss",
        "cache_source": "google_calendar_api",
    }
    _read_cache.set(cache_key, payload)
    return payload


def _event_body(args: Dict[str, Any]) -> Dict[str, Any]:
    summary = str(args.get("summary") or args.get("title") or "")
    start = str(args.get("start") or args.get("dtstart") or "")
    end = str(args.get("end") or args.get("dtend") or "")
    if not summary:
        raise CalendarError("summary is required")
    if not start:
        raise CalendarError("start is required")
    body: Dict[str, Any] = {"summary": summary}
    if args.get("description") is not None:
        body["description"] = str(args.get("description") or "")
    if args.get("location") is not None:
        body["location"] = str(args.get("location") or "")
    timezone_value = str(args.get("timezone") or args.get("timeZone") or "")
    all_day = bool(args.get("all_day"))
    if all_day:
        body["start"] = {"date": start.split("T", 1)[0]}
        body["end"] = {"date": (end or start).split("T", 1)[0]}
    else:
        body["start"] = {"dateTime": start}
        body["end"] = {"dateTime": end or start}
        if timezone_value:
            body["start"]["timeZone"] = timezone_value
            body["end"]["timeZone"] = timezone_value
    attendees = args.get("attendees")
    if isinstance(attendees, list):
        body["attendees"] = [
            item if isinstance(item, dict) else {"email": str(item)}
            for item in attendees
            if item
        ]
    recurrence = args.get("recurrence")
    if isinstance(recurrence, list):
        body["recurrence"] = [str(item) for item in recurrence]
    reminders = args.get("reminders")
    if isinstance(reminders, dict):
        body["reminders"] = reminders
    return body


def _event_patch_body(args: Dict[str, Any]) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    if args.get("summary") is not None or args.get("title") is not None:
        body["summary"] = str(args.get("summary") or args.get("title") or "")
    if args.get("description") is not None:
        body["description"] = str(args.get("description") or "")
    if args.get("location") is not None:
        body["location"] = str(args.get("location") or "")
    timezone_value = str(args.get("timezone") or args.get("timeZone") or "")
    all_day = bool(args.get("all_day"))
    if args.get("start") is not None or args.get("dtstart") is not None:
        start = str(args.get("start") or args.get("dtstart") or "")
        if all_day:
            body["start"] = {"date": start.split("T", 1)[0]}
        else:
            body["start"] = {"dateTime": start}
            if timezone_value:
                body["start"]["timeZone"] = timezone_value
    if args.get("end") is not None or args.get("dtend") is not None:
        end = str(args.get("end") or args.get("dtend") or "")
        if all_day:
            body["end"] = {"date": end.split("T", 1)[0]}
        else:
            body["end"] = {"dateTime": end}
            if timezone_value:
                body["end"]["timeZone"] = timezone_value
    attendees = args.get("attendees")
    if isinstance(attendees, list):
        body["attendees"] = [
            item if isinstance(item, dict) else {"email": str(item)}
            for item in attendees
            if item
        ]
    recurrence = args.get("recurrence")
    if isinstance(recurrence, list):
        body["recurrence"] = [str(item) for item in recurrence]
    reminders = args.get("reminders")
    if isinstance(reminders, dict):
        body["reminders"] = reminders
    if not body:
        raise CalendarError("at least one event field is required")
    return body


def create_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "create_calendar_event")
    account = _resolve_account(args.get("account_id"))
    calendar_id = _calendar_id(args)
    body = _event_body(args)
    cache = _calendar_cache()
    duplicate = cache.find_duplicate(
        account_id=account.account_id,
        calendar_id=calendar_id,
        summary=str(body.get("summary") or ""),
        start=str(body.get("start", {}).get("dateTime") or body.get("start", {}).get("date") or ""),
    )
    if duplicate:
        return {
            "status": "duplicate",
            "provider": "google_calendar",
            "account_id": account.account_id,
            "calendar_id": calendar_id,
            "event": duplicate,
            "event_id": duplicate["event_id"],
            "occurrence_id": duplicate["occurrence_id"],
        }
    service = build_calendar_service(account)
    event = _execute(service.events().insert(calendarId=calendar_id, body=body))
    normalized = normalize_google_event(
        account_id=account.account_id,
        email=account.email,
        calendar_id=calendar_id,
        event=event,
    )
    cache.upsert_event(normalized)
    _list_cache.clear()
    _read_cache.clear()
    return {
        "status": "created",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "calendar_id": calendar_id,
        "event_id": normalized["event_id"],
        "occurrence_id": normalized["occurrence_id"],
        "event": cache.get_event(
            account_id=account.account_id,
            calendar_id=calendar_id,
            event_id=normalized["event_id"],
        ),
    }


def update_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "update_calendar_event")
    account = _resolve_account(args.get("account_id"))
    calendar_id = _calendar_id(args)
    event_id = str(args.get("event_id") or "")
    if not event_id:
        raise CalendarError("event_id is required")
    body = _event_patch_body(args)
    service = build_calendar_service(account)
    event = _execute(service.events().patch(calendarId=calendar_id, eventId=event_id, body=body))
    normalized = normalize_google_event(
        account_id=account.account_id,
        email=account.email,
        calendar_id=calendar_id,
        event=event,
    )
    _calendar_cache().upsert_event(normalized)
    _list_cache.clear()
    _read_cache.clear()
    return {
        "status": "updated",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "calendar_id": calendar_id,
        "event_id": normalized["event_id"],
        "occurrence_id": normalized["occurrence_id"],
    }


def delete_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "delete_calendar_event")
    account = _resolve_account(args.get("account_id"))
    calendar_id = _calendar_id(args)
    event_id = str(args.get("event_id") or "")
    if not event_id:
        raise CalendarError("event_id is required")
    service = build_calendar_service(account)
    _execute(service.events().delete(calendarId=calendar_id, eventId=event_id))
    _calendar_cache().mark_deleted(
        account_id=account.account_id,
        calendar_id=calendar_id,
        event_id=event_id,
    )
    _list_cache.clear()
    _read_cache.clear()
    return {
        "status": "deleted",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "calendar_id": calendar_id,
        "event_id": event_id,
    }


def respond_to_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "respond_to_calendar_event")
    account = _resolve_account(args.get("account_id"))
    calendar_id = _calendar_id(args)
    event_id = str(args.get("event_id") or "")
    response_status = str(args.get("response_status") or args.get("status") or "").lower()
    if response_status not in {"accepted", "declined", "tentative", "needsaction"}:
        raise CalendarError("response_status must be accepted, declined, tentative, or needsAction")
    if not event_id:
        raise CalendarError("event_id is required")
    attendee_email = str(args.get("attendee_email") or account.email)
    service = build_calendar_service(account)
    current = _execute(service.events().get(calendarId=calendar_id, eventId=event_id))
    attendees = current.get("attendees") or []
    matched = False
    for attendee in attendees:
        if str(attendee.get("email") or "").lower() == attendee_email.lower():
            attendee["responseStatus"] = response_status
            matched = True
    if not matched:
        attendees.append({"email": attendee_email, "responseStatus": response_status})
    event = _execute(
        service
        .events()
        .patch(calendarId=calendar_id, eventId=event_id, body={"attendees": attendees})
    )
    normalized = normalize_google_event(
        account_id=account.account_id,
        email=account.email,
        calendar_id=calendar_id,
        event=event,
    )
    _calendar_cache().upsert_event(normalized)
    _list_cache.clear()
    _read_cache.clear()
    return {
        "status": "responded",
        "provider": "google_calendar",
        "account_id": account.account_id,
        "calendar_id": calendar_id,
        "event_id": normalized["event_id"],
        "response_status": response_status,
    }


def bulk_calendar_events(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "bulk_calendar_events")
    operation = str(args.get("operation") or "").strip().lower()
    event_ids = args.get("event_ids") or []
    if not isinstance(event_ids, list) or not event_ids:
        raise CalendarError("event_ids is required")
    results = []
    for event_id in event_ids:
        op_args = {**args, "event_id": event_id, "approved": True}
        if operation == "delete":
            results.append(delete_calendar_event(op_args))
        elif operation == "respond":
            results.append(respond_to_calendar_event(op_args))
        else:
            raise CalendarError("operation must be delete or respond")
    return {"status": "completed", "operation": operation, "results": results}


_HANDLERS = {
    "list_calendar_accounts": list_calendar_accounts,
    "list_calendars": list_calendars,
    "sync_calendar": sync_calendar,
    "list_calendar_events": list_calendar_events,
    "search_calendar_events": search_calendar_events,
    "read_calendar_event": read_calendar_event,
    "create_calendar_event": create_calendar_event,
    "update_calendar_event": update_calendar_event,
    "delete_calendar_event": delete_calendar_event,
    "respond_to_calendar_event": respond_to_calendar_event,
    "bulk_calendar_events": bulk_calendar_events,
}


def dispatch_calendar_tool(
    name: str, args: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    handler = _HANDLERS.get(name)
    if handler is None:
        raise CalendarError(f"Unknown calendar tool: {name}")
    return handler(args or {})


def _json_tool(name: str) -> Callable[[Dict[str, Any]], str]:
    def _handler(args: Dict[str, Any], **_: Any) -> str:
        try:
            return json.dumps(dispatch_calendar_tool(name, args), ensure_ascii=False)
        except CalendarError as exc:
            return json.dumps({"error": str(exc)}, ensure_ascii=False)

    return _handler


def _schema(
    name: str,
    description: str,
    properties: Dict[str, Any],
    required: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "parameters": {
            "type": "object",
            "properties": properties,
            "required": required or [],
        },
    }


_COMMON = {
    "account_id": {
        "type": "string",
        "description": "Google Calendar account id; defaults to configured default account.",
    },
    "calendar_id": {
        "type": "string",
        "description": "Google Calendar id; defaults to primary.",
    },
}


registry.register(
    name="list_calendar_accounts",
    toolset=CALENDAR_TOOLSET,
    schema=_schema("list_calendar_accounts", "List configured Google Calendar accounts.", {}),
    handler=_json_tool("list_calendar_accounts"),
)
registry.register(
    name="list_calendars",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "list_calendars",
        "List calendars for the active Google Calendar account.",
        {"account_id": _COMMON["account_id"], "refresh": {"type": "boolean"}},
    ),
    handler=_json_tool("list_calendars"),
    check_fn=google_calendar_available,
)
registry.register(
    name="sync_calendar",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "sync_calendar",
        "Sync a bounded Google Calendar date range into the local provider cache.",
        {**_COMMON, "start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}},
    ),
    handler=_json_tool("sync_calendar"),
    check_fn=google_calendar_available,
)
registry.register(
    name="list_calendar_events",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "list_calendar_events",
        "List calendar events in a date range.",
        {**_COMMON, "start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}, "page_token": {"type": "string"}},
    ),
    handler=_json_tool("list_calendar_events"),
    check_fn=google_calendar_available,
)
registry.register(
    name="search_calendar_events",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "search_calendar_events",
        "Search calendar events in a date range.",
        {**_COMMON, "query": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}},
        ["query"],
    ),
    handler=_json_tool("search_calendar_events"),
    check_fn=google_calendar_available,
)
registry.register(
    name="read_calendar_event",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "read_calendar_event",
        "Read one calendar event by event_id or occurrence_id.",
        {**_COMMON, "event_id": {"type": "string"}, "occurrence_id": {"type": "string"}},
    ),
    handler=_json_tool("read_calendar_event"),
    check_fn=google_calendar_available,
)
registry.register(
    name="create_calendar_event",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "create_calendar_event",
        "Create a calendar event. Requires approved=true.",
        {**_COMMON, "summary": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}, "description": {"type": "string"}, "location": {"type": "string"}, "attendees": {"type": "array", "items": {"type": "string"}}, "approved": {"type": "boolean"}},
        ["summary", "start", "approved"],
    ),
    handler=_json_tool("create_calendar_event"),
    check_fn=google_calendar_available,
)
registry.register(
    name="update_calendar_event",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "update_calendar_event",
        "Update a calendar event. Requires approved=true.",
        {**_COMMON, "event_id": {"type": "string"}, "summary": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}, "approved": {"type": "boolean"}},
        ["event_id", "approved"],
    ),
    handler=_json_tool("update_calendar_event"),
    check_fn=google_calendar_available,
)
registry.register(
    name="delete_calendar_event",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "delete_calendar_event",
        "Delete a calendar event. Requires approved=true.",
        {**_COMMON, "event_id": {"type": "string"}, "approved": {"type": "boolean"}},
        ["event_id", "approved"],
    ),
    handler=_json_tool("delete_calendar_event"),
    check_fn=google_calendar_available,
)
registry.register(
    name="respond_to_calendar_event",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "respond_to_calendar_event",
        "Respond to a calendar event invitation. Requires approved=true.",
        {**_COMMON, "event_id": {"type": "string"}, "response_status": {"type": "string"}, "approved": {"type": "boolean"}},
        ["event_id", "response_status", "approved"],
    ),
    handler=_json_tool("respond_to_calendar_event"),
    check_fn=google_calendar_available,
)
registry.register(
    name="bulk_calendar_events",
    toolset=CALENDAR_TOOLSET,
    schema=_schema(
        "bulk_calendar_events",
        "Apply an approved bulk calendar operation.",
        {**_COMMON, "event_ids": {"type": "array", "items": {"type": "string"}}, "operation": {"type": "string"}, "approved": {"type": "boolean"}},
        ["event_ids", "operation", "approved"],
    ),
    handler=_json_tool("bulk_calendar_events"),
    check_fn=google_calendar_available,
)
