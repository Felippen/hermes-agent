"""Profile-scoped Gmail provider cache for Hermes mail tools.

This cache is deliberately provider state, not Oryn spine truth. Rows can be
deleted and rebuilt from Gmail sync without deleting Oryn entities or Gmail mail.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from hermes_constants import get_hermes_home


GMAIL_MAIL_CACHE_SCHEMA_VERSION = 1


def default_mail_cache_path() -> Path:
    configured = os.environ.get("HERMES_GMAIL_MAIL_CACHE_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    return get_hermes_home() / "mail" / "gmail_provider_cache.sqlite3"


def _json_list(value: Any) -> str:
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return "[]"


def _load_json_list(value: Any) -> List[Any]:
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return loaded if isinstance(loaded, list) else []


def _like_label(label: str) -> str:
    return f'%"{label}"%'


class GmailMailCache:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path or default_mail_cache_path()

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        self._ensure_schema(conn)
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS gmail_mail_schema_version (
              version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS gmail_mail_messages (
              account_id TEXT NOT NULL,
              email TEXT NOT NULL,
              message_id TEXT NOT NULL,
              thread_id TEXT NOT NULL DEFAULT '',
              from_header TEXT NOT NULL DEFAULT '',
              to_header TEXT NOT NULL DEFAULT '',
              cc_header TEXT NOT NULL DEFAULT '',
              date_header TEXT NOT NULL DEFAULT '',
              subject TEXT NOT NULL DEFAULT '',
              snippet TEXT NOT NULL DEFAULT '',
              labels_json TEXT NOT NULL DEFAULT '[]',
              is_read INTEGER NOT NULL DEFAULT 1,
              is_starred INTEGER NOT NULL DEFAULT 0,
              message_id_header TEXT NOT NULL DEFAULT '',
              reply_to TEXT NOT NULL DEFAULT '',
              text_body TEXT NOT NULL DEFAULT '',
              html_body TEXT NOT NULL DEFAULT '',
              attachments_json TEXT NOT NULL DEFAULT '[]',
              synced_at REAL NOT NULL,
              updated_at REAL NOT NULL,
              PRIMARY KEY (account_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS gmail_mail_sync_state (
              account_id TEXT NOT NULL,
              email TEXT NOT NULL,
              label TEXT NOT NULL DEFAULT '',
              query TEXT NOT NULL DEFAULT '',
              last_synced_at REAL NOT NULL,
              last_limit INTEGER NOT NULL DEFAULT 0,
              last_count INTEGER NOT NULL DEFAULT 0,
              last_error TEXT NOT NULL DEFAULT '',
              PRIMARY KEY (account_id, label, query)
            );

            CREATE INDEX IF NOT EXISTS idx_gmail_mail_messages_account_updated
              ON gmail_mail_messages(account_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_gmail_mail_messages_thread
              ON gmail_mail_messages(account_id, thread_id);
            CREATE INDEX IF NOT EXISTS idx_gmail_mail_messages_subject
              ON gmail_mail_messages(account_id, subject);
            """
        )
        row = conn.execute("SELECT version FROM gmail_mail_schema_version LIMIT 1").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO gmail_mail_schema_version(version) VALUES (?)",
                (GMAIL_MAIL_CACHE_SCHEMA_VERSION,),
            )
        conn.commit()

    def upsert_message(
        self,
        *,
        account_id: str,
        email: str,
        message: Dict[str, Any],
        synced_at: Optional[float] = None,
    ) -> None:
        now = synced_at or time.time()
        labels = list(message.get("labels") or [])
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO gmail_mail_messages (
                  account_id, email, message_id, thread_id, from_header, to_header,
                  cc_header, date_header, subject, snippet, labels_json, is_read,
                  is_starred, message_id_header, reply_to, text_body, html_body,
                  attachments_json, synced_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, message_id) DO UPDATE SET
                  email = excluded.email,
                  thread_id = excluded.thread_id,
                  from_header = excluded.from_header,
                  to_header = excluded.to_header,
                  cc_header = excluded.cc_header,
                  date_header = excluded.date_header,
                  subject = excluded.subject,
                  snippet = excluded.snippet,
                  labels_json = excluded.labels_json,
                  is_read = excluded.is_read,
                  is_starred = excluded.is_starred,
                  message_id_header = CASE
                    WHEN excluded.message_id_header != '' THEN excluded.message_id_header
                    ELSE gmail_mail_messages.message_id_header
                  END,
                  reply_to = CASE
                    WHEN excluded.reply_to != '' THEN excluded.reply_to
                    ELSE gmail_mail_messages.reply_to
                  END,
                  text_body = CASE
                    WHEN excluded.text_body != '' THEN excluded.text_body
                    ELSE gmail_mail_messages.text_body
                  END,
                  html_body = CASE
                    WHEN excluded.html_body != '' THEN excluded.html_body
                    ELSE gmail_mail_messages.html_body
                  END,
                  attachments_json = CASE
                    WHEN excluded.attachments_json != '[]' THEN excluded.attachments_json
                    ELSE gmail_mail_messages.attachments_json
                  END,
                  synced_at = excluded.synced_at,
                  updated_at = excluded.updated_at
                """,
                (
                    account_id,
                    email,
                    str(message.get("message_id") or ""),
                    str(message.get("thread_id") or ""),
                    str(message.get("from") or ""),
                    str(message.get("to") or ""),
                    str(message.get("cc") or ""),
                    str(message.get("date") or ""),
                    str(message.get("subject") or ""),
                    str(message.get("snippet") or ""),
                    _json_list(labels),
                    1 if message.get("is_read", True) else 0,
                    1 if message.get("is_starred", False) else 0,
                    str(message.get("message_id_header") or ""),
                    str(message.get("reply_to") or ""),
                    str(message.get("text_body") or ""),
                    str(message.get("html_body") or ""),
                    _json_list(message.get("attachments") or []),
                    now,
                    now,
                ),
            )
            conn.commit()

    def record_sync(
        self,
        *,
        account_id: str,
        email: str,
        label: str = "",
        query: str = "",
        limit: int = 0,
        count: int = 0,
        synced_at: Optional[float] = None,
        error: str = "",
    ) -> None:
        now = synced_at or time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO gmail_mail_sync_state (
                  account_id, email, label, query, last_synced_at, last_limit,
                  last_count, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, label, query) DO UPDATE SET
                  email = excluded.email,
                  last_synced_at = excluded.last_synced_at,
                  last_limit = excluded.last_limit,
                  last_count = excluded.last_count,
                  last_error = excluded.last_error
                """,
                (account_id, email, label, query, now, limit, count, error),
            )
            conn.commit()

    def sync_status(self, account_id: str, *, label: str = "", query: str = "") -> Dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM gmail_mail_sync_state
                WHERE account_id = ? AND label = ? AND query = ?
                """,
                (account_id, label, query),
            ).fetchone()
            total = conn.execute(
                "SELECT COUNT(*) AS count FROM gmail_mail_messages WHERE account_id = ?",
                (account_id,),
            ).fetchone()
        return {
            "cache_source": "local_provider_cache",
            "cached": row is not None,
            "last_synced_at": row["last_synced_at"] if row else None,
            "last_limit": row["last_limit"] if row else 0,
            "last_count": row["last_count"] if row else 0,
            "last_error": row["last_error"] if row else "",
            "message_count": total["count"] if total else 0,
        }

    def has_sync_state(self, account_id: str, *, label: str = "", query: str = "") -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM gmail_mail_sync_state
                WHERE account_id = ? AND label = ? AND query = ?
                """,
                (account_id, label, query),
            ).fetchone()
        return row is not None

    def list_messages(
        self,
        *,
        account_id: str,
        label: str = "",
        limit: int = 25,
        page_token: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        offset = _parse_offset(page_token)
        where, params = _label_where(account_id, label)
        rows, total = self._select(where, params, limit=limit, offset=offset)
        next_token = str(offset + limit) if offset + limit < total else ""
        return [_row_to_message(row) for row in rows], next_token

    def search_messages(
        self,
        *,
        account_id: str,
        query: str,
        label: str = "",
        limit: int = 25,
        page_token: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        offset = _parse_offset(page_token)
        where, params = _label_where(account_id, label)
        term = f"%{query.lower()}%"
        where += (
            " AND (LOWER(from_header) LIKE ? OR LOWER(to_header) LIKE ? "
            "OR LOWER(cc_header) LIKE ? OR LOWER(subject) LIKE ? "
            "OR LOWER(snippet) LIKE ? OR LOWER(text_body) LIKE ? "
            "OR LOWER(html_body) LIKE ? OR LOWER(labels_json) LIKE ?)"
        )
        params.extend([term] * 8)
        rows, total = self._select(where, params, limit=limit, offset=offset)
        next_token = str(offset + limit) if offset + limit < total else ""
        return [_row_to_message(row) for row in rows], next_token

    def get_message(self, *, account_id: str, message_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM gmail_mail_messages
                WHERE account_id = ? AND message_id = ?
                """,
                (account_id, message_id),
            ).fetchone()
        return _row_to_message(row) if row else None

    def apply_label_delta(
        self,
        *,
        account_id: str,
        message_id: str,
        add_labels: Optional[List[str]] = None,
        remove_labels: Optional[List[str]] = None,
    ) -> None:
        current = self.get_message(account_id=account_id, message_id=message_id)
        if not current:
            return
        labels = set(str(label) for label in current.get("labels") or [])
        labels.update(add_labels or [])
        labels.difference_update(remove_labels or [])
        current["labels"] = sorted(labels)
        current["is_read"] = "UNREAD" not in labels
        current["is_starred"] = "STARRED" in labels
        self.upsert_message(account_id=account_id, email=str(current.get("email") or ""), message=current)

    def mark_trashed(self, *, account_id: str, message_id: str) -> None:
        self.apply_label_delta(
            account_id=account_id,
            message_id=message_id,
            add_labels=["TRASH"],
            remove_labels=["INBOX"],
        )

    def _select(
        self,
        where: str,
        params: List[Any],
        *,
        limit: int,
        offset: int,
    ) -> Tuple[List[sqlite3.Row], int]:
        with self.connect() as conn:
            total_row = conn.execute(
                f"SELECT COUNT(*) AS count FROM gmail_mail_messages WHERE {where}",
                params,
            ).fetchone()
            rows = conn.execute(
                f"""
                SELECT * FROM gmail_mail_messages
                WHERE {where}
                ORDER BY updated_at DESC, message_id DESC
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
        return list(rows), int(total_row["count"] if total_row else 0)


def _parse_offset(page_token: str) -> int:
    try:
        return max(0, int(page_token or 0))
    except (TypeError, ValueError):
        return 0


def _label_where(account_id: str, label: str) -> Tuple[str, List[Any]]:
    normalized = (label or "").strip().lower()
    where = "account_id = ?"
    params: List[Any] = [account_id]
    mapping = {
        "inbox": ("labels_json LIKE ?", [_like_label("INBOX")]),
        "sent": ("labels_json LIKE ?", [_like_label("SENT")]),
        "drafts": ("labels_json LIKE ?", [_like_label("DRAFT")]),
        "trash": ("labels_json LIKE ?", [_like_label("TRASH")]),
        "spam": ("labels_json LIKE ?", [_like_label("SPAM")]),
        "starred": ("labels_json LIKE ?", [_like_label("STARRED")]),
        "unread": ("labels_json LIKE ?", [_like_label("UNREAD")]),
        "important": ("labels_json LIKE ?", [_like_label("IMPORTANT")]),
        "archive": (
            "labels_json NOT LIKE ? AND labels_json NOT LIKE ? AND labels_json NOT LIKE ? "
            "AND labels_json NOT LIKE ? AND labels_json NOT LIKE ?",
            [
                _like_label("INBOX"),
                _like_label("SENT"),
                _like_label("DRAFT"),
                _like_label("TRASH"),
                _like_label("SPAM"),
            ],
        ),
    }
    if normalized in mapping:
        clause, values = mapping[normalized]
        where += f" AND {clause}"
        params.extend(values)
    elif normalized:
        where += " AND labels_json LIKE ?"
        params.append(_like_label(label))
    return where, params


def _row_to_message(row: sqlite3.Row) -> Dict[str, Any]:
    labels = [str(label) for label in _load_json_list(row["labels_json"])]
    return {
        "message_id": row["message_id"],
        "thread_id": row["thread_id"],
        "email": row["email"],
        "from": row["from_header"],
        "to": row["to_header"],
        "cc": row["cc_header"],
        "date": row["date_header"],
        "subject": row["subject"],
        "snippet": row["snippet"],
        "labels": labels,
        "is_read": bool(row["is_read"]),
        "is_starred": bool(row["is_starred"]),
        "message_id_header": row["message_id_header"],
        "reply_to": row["reply_to"],
        "text_body": row["text_body"],
        "html_body": row["html_body"],
        "attachments": _load_json_list(row["attachments_json"]),
        "cache_source": "local_provider_cache",
        "cached_at": row["updated_at"],
    }
