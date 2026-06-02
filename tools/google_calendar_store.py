"""Profile-scoped Google Calendar provider cache for Hermes.

This cache is provider state, not Oryn spine truth. Rows can be deleted and
rebuilt from Google Calendar sync without deleting Oryn domain entities.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from hermes_constants import get_hermes_home


GOOGLE_CALENDAR_CACHE_SCHEMA_VERSION = 1


def default_calendar_cache_path() -> Path:
    configured = os.environ.get("HERMES_GOOGLE_CALENDAR_CACHE_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    return get_hermes_home() / "calendar" / "google_calendar_provider_cache.sqlite3"


def _json_dump(value: Any) -> str:
    return json.dumps(value if value is not None else None, ensure_ascii=False, sort_keys=True)


def _json_load(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return fallback


def _parse_offset(page_token: str) -> int:
    try:
        return max(0, int(page_token or 0))
    except (TypeError, ValueError):
        return 0


def _parse_instant(value: str, *, all_day: bool = False, end_of_day: bool = False) -> float:
    if not value:
        return 0.0
    raw = value.strip()
    try:
        if "T" not in raw:
            suffix = "23:59:59" if end_of_day else "00:00:00"
            dt = datetime.fromisoformat(f"{raw}T{suffix}+00:00")
        else:
            normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    except ValueError:
        return 0.0 if all_day else time.time()


def _event_time(payload: Dict[str, Any], key: str) -> tuple[str, str, bool]:
    data = payload.get(key) or {}
    if not isinstance(data, dict):
        return "", "", False
    if data.get("date"):
        return str(data.get("date") or ""), str(data.get("timeZone") or ""), True
    return str(data.get("dateTime") or ""), str(data.get("timeZone") or ""), False


def normalize_google_event(
    *,
    account_id: str,
    email: str,
    calendar_id: str,
    event: Dict[str, Any],
    synced_at: Optional[float] = None,
) -> Dict[str, Any]:
    start_value, start_tz, start_all_day = _event_time(event, "start")
    end_value, end_tz, end_all_day = _event_time(event, "end")
    original = event.get("originalStartTime") or {}
    original_value = ""
    original_tz = ""
    if isinstance(original, dict):
        original_value = str(original.get("dateTime") or original.get("date") or "")
        original_tz = str(original.get("timeZone") or "")
    event_id = str(event.get("id") or "")
    occurrence_id = f"{calendar_id}:{event_id}:{original_value or start_value}"
    all_day = bool(start_all_day or end_all_day)
    now = synced_at or time.time()
    return {
        "account_id": account_id,
        "email": email,
        "calendar_id": calendar_id,
        "event_id": event_id,
        "occurrence_id": occurrence_id,
        "recurring_event_id": str(event.get("recurringEventId") or ""),
        "ical_uid": str(event.get("iCalUID") or ""),
        "etag": str(event.get("etag") or ""),
        "status": str(event.get("status") or ""),
        "summary": str(event.get("summary") or ""),
        "description": str(event.get("description") or ""),
        "location": str(event.get("location") or ""),
        "html_link": str(event.get("htmlLink") or ""),
        "hangout_link": str(event.get("hangoutLink") or ""),
        "start": start_value,
        "start_timezone": start_tz,
        "end": end_value,
        "end_timezone": end_tz,
        "all_day": all_day,
        "start_epoch": _parse_instant(start_value, all_day=all_day),
        "end_epoch": _parse_instant(end_value, all_day=all_day, end_of_day=all_day),
        "original_start": original_value,
        "original_start_timezone": original_tz,
        "updated": str(event.get("updated") or ""),
        "created": str(event.get("created") or ""),
        "creator": event.get("creator") or {},
        "organizer": event.get("organizer") or {},
        "attendees": event.get("attendees") or [],
        "conference_data": event.get("conferenceData") or {},
        "recurrence": event.get("recurrence") or [],
        "reminders": event.get("reminders") or {},
        "source": event.get("source") or {},
        "raw": event,
        "synced_at": now,
        "deleted": str(event.get("status") or "").lower() == "cancelled",
    }


class GoogleCalendarCache:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path or default_calendar_cache_path()

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=30)
        conn.row_factory = sqlite3.Row
        from hermes_state import apply_wal_with_fallback

        apply_wal_with_fallback(conn, db_label=self.path.name)
        conn.execute("PRAGMA foreign_keys=ON")
        self._ensure_schema(conn)
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS google_calendar_schema_version (
              version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS google_calendar_calendars (
              account_id TEXT NOT NULL,
              email TEXT NOT NULL,
              calendar_id TEXT NOT NULL,
              summary TEXT NOT NULL DEFAULT '',
              description TEXT NOT NULL DEFAULT '',
              timezone TEXT NOT NULL DEFAULT '',
              primary_calendar INTEGER NOT NULL DEFAULT 0,
              access_role TEXT NOT NULL DEFAULT '',
              color_id TEXT NOT NULL DEFAULT '',
              background_color TEXT NOT NULL DEFAULT '',
              foreground_color TEXT NOT NULL DEFAULT '',
              selected INTEGER NOT NULL DEFAULT 1,
              hidden INTEGER NOT NULL DEFAULT 0,
              synced_at REAL NOT NULL,
              updated_at REAL NOT NULL,
              PRIMARY KEY (account_id, calendar_id)
            );

            CREATE TABLE IF NOT EXISTS google_calendar_events (
              account_id TEXT NOT NULL,
              email TEXT NOT NULL,
              calendar_id TEXT NOT NULL,
              event_id TEXT NOT NULL,
              occurrence_id TEXT NOT NULL,
              recurring_event_id TEXT NOT NULL DEFAULT '',
              ical_uid TEXT NOT NULL DEFAULT '',
              etag TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT '',
              summary TEXT NOT NULL DEFAULT '',
              description TEXT NOT NULL DEFAULT '',
              location TEXT NOT NULL DEFAULT '',
              html_link TEXT NOT NULL DEFAULT '',
              hangout_link TEXT NOT NULL DEFAULT '',
              start_value TEXT NOT NULL DEFAULT '',
              start_timezone TEXT NOT NULL DEFAULT '',
              end_value TEXT NOT NULL DEFAULT '',
              end_timezone TEXT NOT NULL DEFAULT '',
              all_day INTEGER NOT NULL DEFAULT 0,
              start_epoch REAL NOT NULL DEFAULT 0,
              end_epoch REAL NOT NULL DEFAULT 0,
              original_start TEXT NOT NULL DEFAULT '',
              original_start_timezone TEXT NOT NULL DEFAULT '',
              updated TEXT NOT NULL DEFAULT '',
              created TEXT NOT NULL DEFAULT '',
              creator_json TEXT NOT NULL DEFAULT '{}',
              organizer_json TEXT NOT NULL DEFAULT '{}',
              attendees_json TEXT NOT NULL DEFAULT '[]',
              conference_json TEXT NOT NULL DEFAULT '{}',
              recurrence_json TEXT NOT NULL DEFAULT '[]',
              reminders_json TEXT NOT NULL DEFAULT '{}',
              source_json TEXT NOT NULL DEFAULT '{}',
              raw_json TEXT NOT NULL DEFAULT '{}',
              synced_at REAL NOT NULL,
              deleted INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (account_id, calendar_id, occurrence_id)
            );

            CREATE TABLE IF NOT EXISTS google_calendar_sync_ranges (
              account_id TEXT NOT NULL,
              email TEXT NOT NULL,
              calendar_id TEXT NOT NULL,
              start_epoch REAL NOT NULL,
              end_epoch REAL NOT NULL,
              start_value TEXT NOT NULL,
              end_value TEXT NOT NULL,
              last_synced_at REAL NOT NULL,
              last_count INTEGER NOT NULL DEFAULT 0,
              last_error TEXT NOT NULL DEFAULT '',
              PRIMARY KEY (account_id, calendar_id, start_value, end_value)
            );

            CREATE INDEX IF NOT EXISTS idx_google_calendar_events_range
              ON google_calendar_events(account_id, calendar_id, start_epoch, end_epoch);
            CREATE INDEX IF NOT EXISTS idx_google_calendar_events_summary
              ON google_calendar_events(account_id, calendar_id, summary);
            CREATE INDEX IF NOT EXISTS idx_google_calendar_events_event_id
              ON google_calendar_events(account_id, calendar_id, event_id);
            """
        )
        row = conn.execute("SELECT version FROM google_calendar_schema_version LIMIT 1").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO google_calendar_schema_version(version) VALUES (?)",
                (GOOGLE_CALENDAR_CACHE_SCHEMA_VERSION,),
            )
        conn.commit()

    def upsert_calendar(
        self,
        *,
        account_id: str,
        email: str,
        calendar: Dict[str, Any],
        synced_at: Optional[float] = None,
    ) -> None:
        now = synced_at or time.time()
        calendar_id = str(calendar.get("id") or calendar.get("calendar_id") or "")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO google_calendar_calendars (
                  account_id, email, calendar_id, summary, description, timezone,
                  primary_calendar, access_role, color_id, background_color,
                  foreground_color, selected, hidden, synced_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, calendar_id) DO UPDATE SET
                  email = excluded.email,
                  summary = excluded.summary,
                  description = excluded.description,
                  timezone = excluded.timezone,
                  primary_calendar = excluded.primary_calendar,
                  access_role = excluded.access_role,
                  color_id = excluded.color_id,
                  background_color = excluded.background_color,
                  foreground_color = excluded.foreground_color,
                  selected = excluded.selected,
                  hidden = excluded.hidden,
                  synced_at = excluded.synced_at,
                  updated_at = excluded.updated_at
                """,
                (
                    account_id,
                    email,
                    calendar_id,
                    str(calendar.get("summary") or ""),
                    str(calendar.get("description") or ""),
                    str(calendar.get("timeZone") or calendar.get("timezone") or ""),
                    1 if calendar.get("primary") or calendar.get("primary_calendar") else 0,
                    str(calendar.get("accessRole") or calendar.get("access_role") or ""),
                    str(calendar.get("colorId") or calendar.get("color_id") or ""),
                    str(calendar.get("backgroundColor") or calendar.get("background_color") or ""),
                    str(calendar.get("foregroundColor") or calendar.get("foreground_color") or ""),
                    1 if calendar.get("selected", True) else 0,
                    1 if calendar.get("hidden", False) else 0,
                    now,
                    now,
                ),
            )
            conn.commit()

    def list_calendars(self, *, account_id: str) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM google_calendar_calendars
                WHERE account_id = ?
                ORDER BY primary_calendar DESC, summary COLLATE NOCASE ASC, calendar_id ASC
                """,
                (account_id,),
            ).fetchall()
        return [_row_to_calendar(row) for row in rows]

    def upsert_event(self, event: Dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO google_calendar_events (
                  account_id, email, calendar_id, event_id, occurrence_id,
                  recurring_event_id, ical_uid, etag, status, summary,
                  description, location, html_link, hangout_link, start_value,
                  start_timezone, end_value, end_timezone, all_day, start_epoch,
                  end_epoch, original_start, original_start_timezone, updated,
                  created, creator_json, organizer_json, attendees_json,
                  conference_json, recurrence_json, reminders_json, source_json,
                  raw_json, synced_at, deleted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, calendar_id, occurrence_id) DO UPDATE SET
                  email = excluded.email,
                  event_id = excluded.event_id,
                  recurring_event_id = excluded.recurring_event_id,
                  ical_uid = excluded.ical_uid,
                  etag = excluded.etag,
                  status = excluded.status,
                  summary = excluded.summary,
                  description = excluded.description,
                  location = excluded.location,
                  html_link = excluded.html_link,
                  hangout_link = excluded.hangout_link,
                  start_value = excluded.start_value,
                  start_timezone = excluded.start_timezone,
                  end_value = excluded.end_value,
                  end_timezone = excluded.end_timezone,
                  all_day = excluded.all_day,
                  start_epoch = excluded.start_epoch,
                  end_epoch = excluded.end_epoch,
                  original_start = excluded.original_start,
                  original_start_timezone = excluded.original_start_timezone,
                  updated = excluded.updated,
                  created = excluded.created,
                  creator_json = excluded.creator_json,
                  organizer_json = excluded.organizer_json,
                  attendees_json = excluded.attendees_json,
                  conference_json = excluded.conference_json,
                  recurrence_json = excluded.recurrence_json,
                  reminders_json = excluded.reminders_json,
                  source_json = excluded.source_json,
                  raw_json = excluded.raw_json,
                  synced_at = excluded.synced_at,
                  deleted = excluded.deleted
                """,
                (
                    event["account_id"],
                    event["email"],
                    event["calendar_id"],
                    event["event_id"],
                    event["occurrence_id"],
                    event["recurring_event_id"],
                    event["ical_uid"],
                    event["etag"],
                    event["status"],
                    event["summary"],
                    event["description"],
                    event["location"],
                    event["html_link"],
                    event["hangout_link"],
                    event["start"],
                    event["start_timezone"],
                    event["end"],
                    event["end_timezone"],
                    1 if event["all_day"] else 0,
                    event["start_epoch"],
                    event["end_epoch"],
                    event["original_start"],
                    event["original_start_timezone"],
                    event["updated"],
                    event["created"],
                    _json_dump(event["creator"]),
                    _json_dump(event["organizer"]),
                    _json_dump(event["attendees"]),
                    _json_dump(event["conference_data"]),
                    _json_dump(event["recurrence"]),
                    _json_dump(event["reminders"]),
                    _json_dump(event["source"]),
                    _json_dump(event["raw"]),
                    event["synced_at"],
                    1 if event["deleted"] else 0,
                ),
            )
            conn.commit()

    def mark_deleted(self, *, account_id: str, calendar_id: str, event_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE google_calendar_events
                SET deleted = 1, status = 'cancelled', synced_at = ?
                WHERE account_id = ? AND calendar_id = ? AND event_id = ?
                """,
                (time.time(), account_id, calendar_id, event_id),
            )
            conn.commit()

    def record_sync_range(
        self,
        *,
        account_id: str,
        email: str,
        calendar_id: str,
        start: str,
        end: str,
        count: int,
        synced_at: Optional[float] = None,
        error: str = "",
    ) -> None:
        now = synced_at or time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO google_calendar_sync_ranges (
                  account_id, email, calendar_id, start_epoch, end_epoch,
                  start_value, end_value, last_synced_at, last_count, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, calendar_id, start_value, end_value) DO UPDATE SET
                  email = excluded.email,
                  last_synced_at = excluded.last_synced_at,
                  last_count = excluded.last_count,
                  last_error = excluded.last_error
                """,
                (
                    account_id,
                    email,
                    calendar_id,
                    _parse_instant(start),
                    _parse_instant(end),
                    start,
                    end,
                    now,
                    count,
                    error,
                ),
            )
            conn.commit()

    def range_status(self, *, account_id: str, calendar_id: str, start: str, end: str) -> Dict[str, Any]:
        start_epoch = _parse_instant(start)
        end_epoch = _parse_instant(end)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM google_calendar_sync_ranges
                WHERE account_id = ? AND calendar_id = ?
                  AND start_epoch <= ? AND end_epoch >= ?
                ORDER BY last_synced_at DESC
                LIMIT 1
                """,
                (account_id, calendar_id, start_epoch, end_epoch),
            ).fetchone()
            total = conn.execute(
                """
                SELECT COUNT(*) AS count FROM google_calendar_events
                WHERE account_id = ? AND calendar_id = ?
                """,
                (account_id, calendar_id),
            ).fetchone()
        return {
            "cache_source": "local_provider_cache",
            "cached": row is not None,
            "covered": row is not None,
            "last_synced_at": row["last_synced_at"] if row else None,
            "last_count": row["last_count"] if row else 0,
            "last_error": row["last_error"] if row else "",
            "event_count": total["count"] if total else 0,
        }

    def range_is_covered(self, *, account_id: str, calendar_id: str, start: str, end: str) -> bool:
        return bool(self.range_status(account_id=account_id, calendar_id=calendar_id, start=start, end=end)["covered"])

    def list_events(
        self,
        *,
        account_id: str,
        calendar_id: str,
        start: str,
        end: str,
        include_deleted: bool = False,
        limit: int = 250,
        page_token: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        offset = _parse_offset(page_token)
        where, params = self._event_range_where(
            account_id=account_id,
            calendar_id=calendar_id,
            start=start,
            end=end,
            include_deleted=include_deleted,
        )
        rows, total = self._select_events(where, params, limit=limit, offset=offset)
        next_token = str(offset + limit) if offset + limit < total else ""
        return [_row_to_event(row) for row in rows], next_token

    def search_events(
        self,
        *,
        account_id: str,
        calendar_id: str,
        query: str,
        start: str,
        end: str,
        include_deleted: bool = False,
        limit: int = 250,
        page_token: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        offset = _parse_offset(page_token)
        where, params = self._event_range_where(
            account_id=account_id,
            calendar_id=calendar_id,
            start=start,
            end=end,
            include_deleted=include_deleted,
        )
        term = f"%{query.lower()}%"
        where += (
            " AND (LOWER(summary) LIKE ? OR LOWER(description) LIKE ? "
            "OR LOWER(location) LIKE ? OR LOWER(attendees_json) LIKE ?)"
        )
        params.extend([term] * 4)
        rows, total = self._select_events(where, params, limit=limit, offset=offset)
        next_token = str(offset + limit) if offset + limit < total else ""
        return [_row_to_event(row) for row in rows], next_token

    def get_event(
        self,
        *,
        account_id: str,
        calendar_id: str,
        event_id: str = "",
        occurrence_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            if occurrence_id:
                row = conn.execute(
                    """
                    SELECT * FROM google_calendar_events
                    WHERE account_id = ? AND calendar_id = ? AND occurrence_id = ?
                    """,
                    (account_id, calendar_id, occurrence_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM google_calendar_events
                    WHERE account_id = ? AND calendar_id = ? AND event_id = ?
                    ORDER BY original_start ASC, occurrence_id ASC
                    LIMIT 1
                    """,
                    (account_id, calendar_id, event_id),
                ).fetchone()
        return _row_to_event(row) if row else None

    def find_duplicate(
        self,
        *,
        account_id: str,
        calendar_id: str,
        summary: str,
        start: str,
    ) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM google_calendar_events
                WHERE account_id = ? AND calendar_id = ?
                  AND deleted = 0
                  AND LOWER(summary) = ?
                  AND start_value = ?
                LIMIT 1
                """,
                (account_id, calendar_id, summary.strip().lower(), start),
            ).fetchone()
        return _row_to_event(row) if row else None

    def _event_range_where(
        self,
        *,
        account_id: str,
        calendar_id: str,
        start: str,
        end: str,
        include_deleted: bool,
    ) -> Tuple[str, List[Any]]:
        where = "account_id = ? AND calendar_id = ? AND end_epoch >= ? AND start_epoch <= ?"
        params: List[Any] = [account_id, calendar_id, _parse_instant(start), _parse_instant(end)]
        if not include_deleted:
            where += " AND deleted = 0"
        return where, params

    def _select_events(
        self,
        where: str,
        params: List[Any],
        *,
        limit: int,
        offset: int,
    ) -> Tuple[List[sqlite3.Row], int]:
        with self.connect() as conn:
            total_row = conn.execute(
                f"SELECT COUNT(*) AS count FROM google_calendar_events WHERE {where}",
                params,
            ).fetchone()
            rows = conn.execute(
                f"""
                SELECT * FROM google_calendar_events
                WHERE {where}
                ORDER BY start_epoch ASC, end_epoch ASC, summary COLLATE NOCASE ASC
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
        return list(rows), int(total_row["count"] if total_row else 0)


def _row_to_calendar(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "account_id": row["account_id"],
        "provider": "google_calendar",
        "email": row["email"],
        "calendar_id": row["calendar_id"],
        "summary": row["summary"],
        "name": row["summary"],
        "description": row["description"],
        "timezone": row["timezone"],
        "primary": bool(row["primary_calendar"]),
        "access_role": row["access_role"],
        "color_id": row["color_id"],
        "background_color": row["background_color"],
        "foreground_color": row["foreground_color"],
        "selected": bool(row["selected"]),
        "hidden": bool(row["hidden"]),
        "synced_at": row["synced_at"],
        "updated_at": row["updated_at"],
        "cache_source": "local_provider_cache",
    }


def _row_to_event(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "account_id": row["account_id"],
        "provider": "google_calendar",
        "email": row["email"],
        "calendar_id": row["calendar_id"],
        "event_id": row["event_id"],
        "message_id": row["event_id"],
        "occurrence_id": row["occurrence_id"],
        "recurring_event_id": row["recurring_event_id"],
        "iCalUID": row["ical_uid"],
        "ical_uid": row["ical_uid"],
        "etag": row["etag"],
        "status": row["status"],
        "summary": row["summary"],
        "title": row["summary"],
        "description": row["description"],
        "location": row["location"],
        "html_link": row["html_link"],
        "hangout_link": row["hangout_link"],
        "start": row["start_value"],
        "start_timezone": row["start_timezone"],
        "end": row["end_value"],
        "end_timezone": row["end_timezone"],
        "all_day": bool(row["all_day"]),
        "original_start": row["original_start"],
        "original_start_timezone": row["original_start_timezone"],
        "updated": row["updated"],
        "created": row["created"],
        "creator": _json_load(row["creator_json"], {}),
        "organizer": _json_load(row["organizer_json"], {}),
        "attendees": _json_load(row["attendees_json"], []),
        "conference_data": _json_load(row["conference_json"], {}),
        "recurrence": _json_load(row["recurrence_json"], []),
        "reminders": _json_load(row["reminders_json"], {}),
        "source": _json_load(row["source_json"], {}),
        "deleted": bool(row["deleted"]),
        "cache_source": "local_provider_cache",
        "cached_at": row["synced_at"],
    }
