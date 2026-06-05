"""Axon-backed Calendar route adapter for the API gateway.

This module preserves the existing /v1/calendar response shape while moving
truth to Oryn Spine and the Calendar outbox. It is intentionally imported only
when Axon Calendar is configured, so standalone Hermes checkouts can keep using
their legacy provider-cache tools.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

ORYN_CALENDAR_AXON_BACKED = "ORYN_CALENDAR_AXON_BACKED"
ORYN_CALENDAR_AXON_REPO_ROOT = "ORYN_CALENDAR_AXON_REPO_ROOT"
ORYN_SPINE_POSTGRES_DSN = "ORYN_SPINE_POSTGRES_DSN"


class CalendarError(RuntimeError):
    pass


def axon_calendar_requested(env: Optional[Dict[str, str]] = None) -> bool:
    values = env if env is not None else os.environ
    return _truthy(values.get(ORYN_CALENDAR_AXON_BACKED)) or bool(values.get(ORYN_SPINE_POSTGRES_DSN))


def dispatch_calendar_tool(name: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    handler = _HANDLERS.get(name)
    if handler is None:
        raise CalendarError(f"Unknown calendar tool: {name}")
    return handler(args or {})


def list_calendar_accounts(args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    with _spine_repository() as store:
        return _calendar_projection(store).accounts(account_id=_account_id(args or {}))


def list_calendars(args: Dict[str, Any]) -> Dict[str, Any]:
    with _spine_repository() as store:
        return _calendar_projection(store).calendars(account_id=_account_id(args))


def sync_calendar(args: Dict[str, Any]) -> Dict[str, Any]:
    start, end = _coerce_range(args)
    calendar_id = _calendar_id(args)
    limit = _limit(args, default=500, maximum=500)
    with _spine_repository() as store:
        payload = _calendar_projection(store).events(
            account_id=_account_id(args),
            calendar_id=calendar_id,
            start=start,
            end=end,
            limit=limit,
        )
    count = len(payload.get("events") or payload.get("data") or [])
    return {
        "object": "google_calendar.sync",
        "provider": "google_calendar",
        "account_id": _account_id(args),
        "calendar_id": calendar_id,
        "start": start,
        "end": end,
        "synced": count,
        "updated": 0,
        "next_page_token": None,
        "cache": "axon_spine",
        "cache_source": "axon_spine",
        "synced_at": time.time(),
        "cache_status": {"source": "axon_spine", "fresh": True, "covered": True, "last_count": count},
    }


def list_calendar_events(args: Dict[str, Any]) -> Dict[str, Any]:
    start, end = _coerce_range(args)
    with _spine_repository() as store:
        return _calendar_projection(store).events(
            account_id=_account_id(args),
            calendar_id=_calendar_id(args),
            start=start,
            end=end,
            limit=_limit(args),
        )


def search_calendar_events(args: Dict[str, Any]) -> Dict[str, Any]:
    query = str(args.get("query") or args.get("q") or "").strip().lower()
    payload = list_calendar_events(args)
    if query:
        events = []
        for event in payload.get("events") or []:
            text = " ".join(
                str(event.get(key) or "")
                for key in ("summary", "description", "location", "calendar_id", "event_id")
                if isinstance(event, dict)
            ).lower()
            if query in text:
                events.append(event)
        payload = {**payload, "events": events, "data": events}
    return {**payload, "query": query}


def read_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    event_id = str(args.get("event_id") or "").strip()
    occurrence_id = _optional_text(args.get("occurrence_id"))
    if not event_id and not occurrence_id:
        raise CalendarError("event_id or occurrence_id is required")
    with _spine_repository() as store:
        try:
            return _calendar_projection(store).event(
                account_id=_account_id(args),
                calendar_id=_calendar_id(args),
                event_id=event_id or occurrence_id or "",
                occurrence_id=occurrence_id,
            )
        except KeyError as exc:
            raise CalendarError(str(exc)) from exc


def create_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "create_calendar_event")
    payload = _event_payload(args)
    return _apply_write(op="create", entity_id=None, args=args, payload=payload, status="created")


def update_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "update_calendar_event")
    with _spine_and_outbox() as (store, outbox):
        entity = _resolve_calendar_event(store, args)
        payload = _event_payload(args)
        return _apply_write_with_resources(
            store,
            outbox,
            op="update",
            entity_id=entity.id,
            args=args,
            payload=payload,
            status="updated",
        )


def delete_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "delete_calendar_event")
    with _spine_and_outbox() as (store, outbox):
        entity = _resolve_calendar_event(store, args)
        return _apply_write_with_resources(
            store,
            outbox,
            op="delete",
            entity_id=entity.id,
            args=args,
            payload={},
            status="deleted",
        )


def respond_to_calendar_event(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "respond_to_calendar_event")
    response = str(args.get("response") or args.get("response_status") or "").strip()
    attendee = str(args.get("attendee_email") or args.get("attendee") or "").strip()
    if not response:
        raise CalendarError("response is required")
    if not attendee:
        raise CalendarError("attendee_email is required")
    with _spine_and_outbox() as (store, outbox):
        entity = _resolve_calendar_event(store, args)
        return _apply_write_with_resources(
            store,
            outbox,
            op="respond",
            entity_id=entity.id,
            args=args,
            payload={"response": response, "attendee_email": attendee},
            status="responded",
        )


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


def _apply_write(
    *,
    op: str,
    entity_id: str | None,
    args: Dict[str, Any],
    payload: Dict[str, Any],
    status: str,
) -> Dict[str, Any]:
    with _spine_and_outbox() as (store, outbox):
        return _apply_write_with_resources(
            store,
            outbox,
            op=op,
            entity_id=entity_id,
            args=args,
            payload=payload,
            status=status,
        )


def _apply_write_with_resources(
    store: Any,
    outbox: Any,
    *,
    op: str,
    entity_id: str | None,
    args: Dict[str, Any],
    payload: Dict[str, Any],
    status: str,
) -> Dict[str, Any]:
    try:
        from tools.oryn_spine.calendar_write_intents import CalendarWriteError, apply_calendar_write_intent
        from tools.oryn_spine.calendar_read import calendar_event_detail

        result = apply_calendar_write_intent(
            store,
            outbox,
            entity_id=entity_id,
            actor=str(args.get("actor") or "workspace-calendar"),
            op=op,
            payload=payload,
        )
    except (KeyError, CalendarWriteError) as exc:
        raise CalendarError(str(exc)) from exc

    event = calendar_event_detail(result.entity, account_id=_account_id(args))
    return {
        "status": status,
        "provider": "google_calendar",
        "account_id": _account_id(args),
        "calendar_id": event.get("calendar_id") or _calendar_id(args),
        "event_id": event.get("event_id"),
        "occurrence_id": event.get("occurrence_id"),
        "event": event,
        "intent_id": getattr(result.intent, "id", None) if result.intent is not None else None,
        "cache_source": "axon_spine",
    }


def _resolve_calendar_event(store: Any, args: Dict[str, Any]) -> Any:
    from tools.oryn_spine.calendar_read import resolve_calendar_event

    event_id = str(args.get("event_id") or "").strip()
    if not event_id:
        raise CalendarError("event_id is required")
    entity = resolve_calendar_event(
        store,
        calendar_id=_calendar_id(args),
        event_id=event_id,
        occurrence_id=_optional_text(args.get("occurrence_id")),
        missing_is_error=True,
    )
    return entity


def _event_payload(args: Dict[str, Any]) -> Dict[str, Any]:
    calendar_id = _calendar_id(args)
    summary = str(args.get("summary") or args.get("title") or "").strip()
    start = str(args.get("start") or args.get("dtstart") or "").strip()
    end = str(args.get("end") or args.get("dtend") or start).strip()
    if not summary:
        raise CalendarError("summary is required")
    if not start:
        raise CalendarError("start is required")
    payload: Dict[str, Any] = {
        "provider": "google",
        "calendar_id": calendar_id,
        "event_id": str(args.get("event_id") or _provisional_event_id(args)),
        "etag": str(args.get("etag") or '"oryn-pending"'),
        "summary": summary,
        "start_at": start,
        "end_at": end or start,
        "status": str(args.get("status") or "confirmed"),
        "attendees": [],
        "raw": {"id": str(args.get("event_id") or ""), "source": "oryn_workspace"},
    }
    for source, target in (
        ("description", "description"),
        ("location", "location"),
        ("timezone", "timezone"),
        ("timeZone", "timezone"),
    ):
        if args.get(source) is not None:
            payload[target] = str(args.get(source) or "")
    attendees = args.get("attendees")
    if isinstance(attendees, list):
        payload["attendees"] = [str(item.get("email") if isinstance(item, dict) else item) for item in attendees if item]
    recurrence = args.get("recurrence")
    if isinstance(recurrence, list):
        payload["raw"] = {"recurrence": [str(item) for item in recurrence]}
    payload["raw"].setdefault("id", payload["event_id"])
    return payload


def _provisional_event_id(args: Dict[str, Any]) -> str:
    body = {
        "calendar_id": _calendar_id(args),
        "summary": str(args.get("summary") or args.get("title") or ""),
        "start": str(args.get("start") or args.get("dtstart") or ""),
        "end": str(args.get("end") or args.get("dtend") or ""),
    }
    digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"orynpending{digest[:20]}"


@contextmanager
def _spine_repository() -> Iterator[Any]:
    with _spine_and_outbox() as (store, _outbox):
        yield store


@contextmanager
def _spine_and_outbox() -> Iterator[tuple[Any, Any]]:
    _prepare_oryn_import_path()
    from tools.oryn_dendrites.calendar_outbound import PostgresCalendarOutboxStore
    from tools.oryn_spine.postgres_config import connect_spine_postgres
    from tools.oryn_spine.postgres_repository import PostgresSpineRepository

    connection = connect_spine_postgres()
    try:
        outbox = PostgresCalendarOutboxStore(connection)
        outbox.initialize()
        yield PostgresSpineRepository(connection), outbox
    finally:
        close = getattr(connection, "close", None)
        if callable(close):
            close()


def _calendar_projection(store: Any) -> Any:
    _prepare_oryn_import_path()
    from tools.oryn_spine.hermes_projection import AxonCalendarHermesProjection

    return AxonCalendarHermesProjection(store)


def _prepare_oryn_import_path() -> None:
    repo_root = _oryn_repo_root()
    for path in (repo_root, repo_root / "modules" / "ovyon-context" / "src"):
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)
    import tools as hermes_tools

    parent_tools = str(repo_root / "tools")
    if parent_tools not in hermes_tools.__path__:
        hermes_tools.__path__.append(parent_tools)


def _oryn_repo_root() -> Path:
    configured = os.environ.get(ORYN_CALENDAR_AXON_REPO_ROOT)
    if configured:
        root = Path(configured).expanduser().resolve()
        if (root / "tools" / "oryn_spine" / "calendar_read.py").exists():
            return root
        raise CalendarError(f"{ORYN_CALENDAR_AXON_REPO_ROOT} does not point at an Oryn checkout")
    for parent in Path(__file__).resolve().parents:
        if (parent / "tools" / "oryn_spine" / "calendar_read.py").exists():
            return parent
    raise CalendarError(
        f"Axon Calendar requires {ORYN_CALENDAR_AXON_REPO_ROOT} or an embedded Oryn checkout with tools/oryn_spine."
    )


def _require_approval(args: Dict[str, Any], op_name: str) -> None:
    if not bool(args.get("approved")):
        raise CalendarError(f"{op_name} requires explicit approval")


def _coerce_range(args: Dict[str, Any]) -> tuple[str | None, str | None]:
    return _optional_text(args.get("start") or args.get("time_min") or args.get("from")), _optional_text(
        args.get("end") or args.get("time_max") or args.get("to")
    )


def _limit(args: Dict[str, Any], default: int = 250, maximum: int = 500) -> int:
    try:
        value = int(args.get("limit") or args.get("max_results") or default)
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, maximum))


def _calendar_id(args: Dict[str, Any]) -> str:
    return str(args.get("calendar_id") or args.get("calendar") or "primary")


def _account_id(args: Dict[str, Any]) -> str | None:
    return _optional_text(args.get("account_id"))


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


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
