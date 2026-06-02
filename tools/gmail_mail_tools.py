"""Gmail-backed mail tools for Hermes/Oryn.

The module keeps Google client imports lazy so Hermes can boot without the
optional google extra. Tests can inject a fake Gmail service by monkeypatching
``build_gmail_service``.
"""

from __future__ import annotations

import base64
import html
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from hermes_constants import get_hermes_home
from tools.registry import registry


SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
]

LIST_CACHE_TTL_SECONDS = 30.0
READ_CACHE_TTL_SECONDS = 300.0
MAIL_TOOLSET = "gmail_mail"

NORMALIZED_VIEW_TO_GMAIL_QUERY = {
    "inbox": "in:inbox",
    "sent": "in:sent",
    "drafts": "in:drafts",
    "trash": "in:trash",
    "spam": "in:spam",
    "archive": "-in:inbox -in:sent -in:drafts -in:trash -in:spam",
    "starred": "is:starred",
    "unread": "is:unread",
    "important": "is:important",
}


class MailError(RuntimeError):
    """Expected mail operation failure surfaced as a JSON error."""


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


_list_cache = TTLCache(LIST_CACHE_TTL_SECONDS)
_read_cache = TTLCache(READ_CACHE_TTL_SECONDS)


@dataclass(frozen=True)
class GmailAccount:
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
            "provider": "gmail",
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
    return f"gmail:{safe}"


def _configured_emails() -> List[str]:
    raw = os.environ.get("HERMES_GMAIL_ACCOUNT_EMAILS", "").strip()
    emails = [part.strip() for part in raw.split(",") if part.strip()]
    return emails or ["me"]


def load_gmail_accounts() -> List[GmailAccount]:
    token_path = _configured_path("HERMES_GMAIL_TOKEN_PATH", _default_token_path())
    client_path = _configured_path(
        "HERMES_GMAIL_CLIENT_SECRETS_PATH",
        _default_client_secrets_path(),
    )
    token_exists = token_path.exists()
    client_exists = client_path.exists()
    status = "configured" if token_exists else "unauthorized"
    if token_exists and not client_exists:
        status = "configured_without_client_secret"

    accounts = []
    for index, email in enumerate(_configured_emails()):
        accounts.append(
            GmailAccount(
                account_id=_account_id(email),
                email=email,
                display_name=email,
                is_default=index == 0,
                enabled=token_exists,
                status=status,
                token_path=str(token_path),
                client_secrets_path=str(client_path),
            )
        )
    return accounts


def gmail_mail_available() -> bool:
    return any(account.enabled for account in load_gmail_accounts())


def _resolve_account(account_id: Optional[str] = None) -> GmailAccount:
    accounts = load_gmail_accounts()
    if not accounts:
        raise MailError("No Gmail accounts are configured")
    if account_id:
        for account in accounts:
            if account.account_id == account_id or account.email == account_id:
                if not account.enabled:
                    raise MailError("Gmail account is not authenticated")
                return account
        raise MailError(f"Unknown Gmail account: {account_id}")
    account = next((item for item in accounts if item.is_default), accounts[0])
    if not account.enabled:
        raise MailError("Gmail account is not authenticated")
    return account


def _stored_token_scopes(token_path: str) -> List[str]:
    try:
        data = json.loads(Path(token_path).read_text(encoding="utf-8"))
    except Exception:
        return list(SCOPES)
    scopes = data.get("scopes")
    if isinstance(scopes, list) and scopes:
        return [str(scope) for scope in scopes]
    return list(SCOPES)


def build_gmail_service(account: GmailAccount):
    """Build a Gmail API service for an account.

    Kept as a small function so tests can replace it with a fake service.
    """
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise MailError("Google API dependencies are not installed") from exc

    token_path = Path(account.token_path)
    if not token_path.exists():
        raise MailError("Gmail OAuth token is missing")

    creds = Credentials.from_authorized_user_file(
        str(token_path),
        _stored_token_scopes(str(token_path)),
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_path.write_text(creds.to_json(), encoding="utf-8")
    if not creds.valid:
        raise MailError("Gmail OAuth token is invalid")
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _execute(call: Any) -> Dict[str, Any]:
    result = call.execute()
    return result or {}


def _headers_dict(msg: Dict[str, Any]) -> Dict[str, str]:
    headers = msg.get("payload", {}).get("headers", [])
    return {
        str(header.get("name", "")).lower(): str(header.get("value", ""))
        for header in headers
        if header.get("name")
    }


def _decode_body(data: str) -> str:
    if not data:
        return ""
    padded = data + ("=" * (-len(data) % 4))
    return base64.urlsafe_b64decode(padded.encode("utf-8")).decode(
        "utf-8",
        errors="replace",
    )


def _walk_parts(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    yield payload
    for part in payload.get("parts") or []:
        if isinstance(part, dict):
            yield from _walk_parts(part)


def _extract_bodies_and_attachments(
    msg: Dict[str, Any],
) -> tuple[str, str, List[Dict[str, Any]]]:
    text_body = ""
    html_body = ""
    attachments: List[Dict[str, Any]] = []
    payload = msg.get("payload", {})

    for part in _walk_parts(payload):
        mime_type = str(part.get("mimeType", ""))
        body = part.get("body", {}) or {}
        filename = str(part.get("filename", "") or "")
        if filename:
            attachments.append({
                "filename": filename,
                "mime_type": mime_type,
                "size": body.get("size", 0),
                "attachment_id": body.get("attachmentId", ""),
            })
            continue
        data = body.get("data", "")
        if not data:
            continue
        if mime_type == "text/plain" and not text_body:
            text_body = _decode_body(data)
        elif mime_type == "text/html" and not html_body:
            html_body = _decode_body(data)

    if not text_body and html_body:
        text_body = re.sub(
            r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(html_body))
        ).strip()
    return text_body, html_body, attachments


def _message_row(msg: Dict[str, Any]) -> Dict[str, Any]:
    headers = _headers_dict(msg)
    labels = list(msg.get("labelIds") or [])
    return {
        "message_id": msg.get("id", ""),
        "thread_id": msg.get("threadId", ""),
        "from": headers.get("from", ""),
        "to": headers.get("to", ""),
        "cc": headers.get("cc", ""),
        "date": headers.get("date", ""),
        "subject": headers.get("subject", ""),
        "snippet": msg.get("snippet", ""),
        "labels": labels,
        "is_read": "UNREAD" not in labels,
        "is_starred": "STARRED" in labels,
    }


def _message_full(msg: Dict[str, Any]) -> Dict[str, Any]:
    row = _message_row(msg)
    headers = _headers_dict(msg)
    text_body, html_body, attachments = _extract_bodies_and_attachments(msg)
    row.update({
        "message_id_header": headers.get("message-id", ""),
        "reply_to": headers.get("reply-to", ""),
        "text_body": text_body,
        "html_body": html_body,
        "attachments": attachments,
    })
    return row


def _view_query(label: Optional[str], query: str = "") -> str:
    parts = []
    if label:
        normalized = label.strip().lower()
        mapped = NORMALIZED_VIEW_TO_GMAIL_QUERY.get(normalized)
        if mapped:
            parts.append(mapped)
        else:
            parts.append(f"label:{label}")
    if query:
        parts.append(query)
    return " ".join(parts).strip()


def list_email_accounts(args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    accounts = [account.to_dict() for account in load_gmail_accounts()]
    return {
        "object": "list",
        "provider": "gmail",
        "data": accounts,
        "configured": any(account["enabled"] for account in accounts),
    }


def _list_or_search(args: Dict[str, Any], *, search: bool) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    limit = max(1, min(int(args.get("limit") or args.get("max_results") or 25), 100))
    page_token = str(args.get("page_token") or "")
    label = str(
        args.get("label") or args.get("folder") or ("inbox" if not search else "")
    )
    query = str(args.get("query") or "")
    gmail_query = _view_query(label, query)
    cache_key = json.dumps(
        {
            "op": "search" if search else "list",
            "account": account.account_id,
            "query": gmail_query,
            "limit": limit,
            "page": page_token,
        },
        sort_keys=True,
    )
    cached = _list_cache.get(cache_key)
    if cached is not None:
        return {**cached, "cache": "hit"}

    service = build_gmail_service(account)
    list_call = (
        service
        .users()
        .messages()
        .list(
            userId="me",
            q=gmail_query,
            maxResults=limit,
            pageToken=page_token or None,
        )
    )
    results = _execute(list_call)
    rows: List[Dict[str, Any]] = []
    for meta in results.get("messages", []) or []:
        message_id = meta.get("id")
        if not message_id:
            continue
        msg = _execute(
            service
            .users()
            .messages()
            .get(
                userId="me",
                id=message_id,
                format="metadata",
                metadataHeaders=["From", "To", "Cc", "Subject", "Date"],
            )
        )
        rows.append(_message_row(msg))

    payload = {
        "object": "list",
        "provider": "gmail",
        "account_id": account.account_id,
        "label": label,
        "query": gmail_query,
        "data": rows,
        "next_page_token": results.get("nextPageToken", ""),
        "cache": "miss",
    }
    _list_cache.set(cache_key, payload)
    return payload


def list_emails(args: Dict[str, Any]) -> Dict[str, Any]:
    return _list_or_search(args, search=False)


def search_emails(args: Dict[str, Any]) -> Dict[str, Any]:
    return _list_or_search(args, search=True)


def read_email(args: Dict[str, Any]) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    message_id = str(args.get("message_id") or args.get("uid") or "")
    if not message_id:
        raise MailError("message_id is required")
    cache_key = f"{account.account_id}:{message_id}"
    cached = _read_cache.get(cache_key)
    if cached is not None:
        return {**cached, "cache": "hit"}
    service = build_gmail_service(account)
    msg = _execute(
        service
        .users()
        .messages()
        .get(
            userId="me",
            id=message_id,
            format="full",
        )
    )
    payload = {
        "object": "gmail.message",
        "provider": "gmail",
        "account_id": account.account_id,
        **_message_full(msg),
        "cache": "miss",
    }
    _read_cache.set(cache_key, payload)
    return payload


def _require_approval(args: Dict[str, Any], operation: str) -> None:
    approved = bool(
        args.get("approved") or args.get("confirmed") or args.get("approval_confirmed")
    )
    if not approved:
        raise MailError(f"{operation} requires explicit approval")


def _split_recipients(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item).strip() for item in value if str(item).strip())
    return str(value or "").strip()


def _build_message(
    *,
    to: Any,
    subject: str,
    body: str,
    cc: Any = None,
    bcc: Any = None,
    from_header: str = "",
    html_body: bool = False,
    in_reply_to: str = "",
    references: str = "",
) -> str:
    message = MIMEText(body, "html" if html_body else "plain", "utf-8")
    message["To"] = _split_recipients(to)
    message["Subject"] = subject
    if cc:
        message["Cc"] = _split_recipients(cc)
    if bcc:
        message["Bcc"] = _split_recipients(bcc)
    if from_header:
        message["From"] = from_header
    if in_reply_to:
        message["In-Reply-To"] = in_reply_to
        message["References"] = references or in_reply_to
    return base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")


def send_email(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "send_email")
    account = _resolve_account(args.get("account_id"))
    to = args.get("to")
    subject = str(args.get("subject") or "")
    body_text = str(args.get("body") or args.get("text_body") or "")
    if not _split_recipients(to):
        raise MailError("to is required")
    if not subject:
        raise MailError("subject is required")
    if not body_text:
        raise MailError("body is required")
    service = build_gmail_service(account)
    body = {
        "raw": _build_message(
            to=to,
            cc=args.get("cc"),
            bcc=args.get("bcc"),
            subject=subject,
            body=body_text,
            from_header=str(args.get("from") or args.get("from_header") or ""),
            html_body=bool(args.get("html") or args.get("is_html")),
        )
    }
    if args.get("thread_id"):
        body["threadId"] = str(args["thread_id"])
    result = _execute(service.users().messages().send(userId="me", body=body))
    return {
        "status": "sent",
        "provider": "gmail",
        "account_id": account.account_id,
        "message_id": result.get("id", ""),
        "thread_id": result.get("threadId", ""),
    }


def reply_to_email(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "reply_to_email")
    account = _resolve_account(args.get("account_id"))
    message_id = str(args.get("message_id") or "")
    reply_body = str(args.get("body") or args.get("text_body") or "")
    if not message_id:
        raise MailError("message_id is required")
    if not reply_body:
        raise MailError("body is required")
    service = build_gmail_service(account)
    original = _execute(
        service
        .users()
        .messages()
        .get(
            userId="me",
            id=message_id,
            format="metadata",
            metadataHeaders=["From", "Subject", "Message-ID", "References"],
        )
    )
    headers = _headers_dict(original)
    subject = headers.get("subject", "")
    if subject and not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    result = _execute(
        service
        .users()
        .messages()
        .send(
            userId="me",
            body={
                "raw": _build_message(
                    to=headers.get("from", ""),
                    subject=subject or "Re:",
                    body=reply_body,
                    from_header=str(args.get("from") or args.get("from_header") or ""),
                    html_body=bool(args.get("html") or args.get("is_html")),
                    in_reply_to=headers.get("message-id", ""),
                    references=headers.get("references", "")
                    or headers.get("message-id", ""),
                ),
                "threadId": original.get("threadId", ""),
            },
        )
    )
    return {
        "status": "sent",
        "provider": "gmail",
        "account_id": account.account_id,
        "message_id": result.get("id", ""),
        "thread_id": result.get("threadId", ""),
    }


def _modify_message(
    args: Dict[str, Any],
    *,
    add_labels: Optional[List[str]] = None,
    remove_labels: Optional[List[str]] = None,
) -> Dict[str, Any]:
    account = _resolve_account(args.get("account_id"))
    message_id = str(args.get("message_id") or "")
    if not message_id:
        raise MailError("message_id is required")
    service = build_gmail_service(account)
    result = _execute(
        service
        .users()
        .messages()
        .modify(
            userId="me",
            id=message_id,
            body={
                "addLabelIds": add_labels or [],
                "removeLabelIds": remove_labels or [],
            },
        )
    )
    _read_cache.clear()
    _list_cache.clear()
    return {
        "status": "modified",
        "provider": "gmail",
        "account_id": account.account_id,
        "message_id": result.get("id", message_id),
        "labels": result.get("labelIds", []),
    }


def archive_email(args: Dict[str, Any]) -> Dict[str, Any]:
    return _modify_message(args, remove_labels=["INBOX"])


def delete_email(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "delete_email")
    account = _resolve_account(args.get("account_id"))
    message_id = str(args.get("message_id") or "")
    if not message_id:
        raise MailError("message_id is required")
    service = build_gmail_service(account)
    result = _execute(service.users().messages().trash(userId="me", id=message_id))
    _read_cache.clear()
    _list_cache.clear()
    return {
        "status": "trashed",
        "provider": "gmail",
        "account_id": account.account_id,
        "message_id": result.get("id", message_id),
        "labels": result.get("labelIds", []),
    }


def mark_email_read(args: Dict[str, Any]) -> Dict[str, Any]:
    unread = bool(args.get("unread") or args.get("mark_unread"))
    return _modify_message(
        args,
        add_labels=["UNREAD"] if unread else [],
        remove_labels=[] if unread else ["UNREAD"],
    )


def bulk_email(args: Dict[str, Any]) -> Dict[str, Any]:
    _require_approval(args, "bulk_email")
    operation = str(args.get("operation") or "").strip().lower()
    message_ids = args.get("message_ids") or []
    if not isinstance(message_ids, list) or not message_ids:
        raise MailError("message_ids is required")
    results = []
    for message_id in message_ids:
        op_args = {**args, "message_id": message_id, "approved": True}
        if operation == "archive":
            results.append(archive_email(op_args))
        elif operation == "delete":
            results.append(delete_email(op_args))
        elif operation == "mark_read":
            results.append(mark_email_read(op_args))
        elif operation == "mark_unread":
            results.append(mark_email_read({**op_args, "unread": True}))
        else:
            raise MailError(
                "operation must be archive, delete, mark_read, or mark_unread"
            )
    return {"status": "completed", "operation": operation, "results": results}


def build_summary_prompt(message: Dict[str, Any]) -> str:
    return (
        "Summarize this email for an Oryn user. Focus on sender intent, "
        "requested actions, deadlines, and risks. Keep it concise.\n\n"
        f"From: {message.get('from', '')}\n"
        f"Subject: {message.get('subject', '')}\n"
        f"Body:\n{message.get('text_body', '')[:12000]}"
    )


def build_reply_prompt(message: Dict[str, Any], intent: str = "") -> str:
    return (
        "Draft a reply to this email. Do not claim the email was sent. "
        "Preserve factual uncertainty and keep a professional tone.\n\n"
        f"User intent: {intent}\n"
        f"From: {message.get('from', '')}\n"
        f"Subject: {message.get('subject', '')}\n"
        f"Body:\n{message.get('text_body', '')[:12000]}"
    )


_ai_generator: Optional[Callable[[str], str]] = None


def set_mail_ai_generator(generator: Optional[Callable[[str], str]]) -> None:
    global _ai_generator
    _ai_generator = generator


def _generate_ai_text(prompt: str) -> Dict[str, Any]:
    if _ai_generator is None:
        try:
            from run_agent import AIAgent

            agent = AIAgent(
                max_iterations=1,
                enabled_toolsets=[],
                quiet_mode=True,
                skip_context_files=True,
                skip_memory=True,
            )
            return {
                "status": "generated",
                "text": agent.chat(prompt),
                "prompt": prompt,
                "model_routing": "hermes",
            }
        except Exception as exc:
            return {
                "status": "prompt_ready",
                "text": "",
                "prompt": prompt,
                "model_routing": "unavailable",
                "error": str(exc),
            }
    return {
        "status": "generated",
        "text": _ai_generator(prompt),
        "prompt": prompt,
        "model_routing": "hermes",
    }


def summarize_email(args: Dict[str, Any]) -> Dict[str, Any]:
    message = read_email(args)
    ai = _generate_ai_text(build_summary_prompt(message))
    return {
        "object": "gmail.summary",
        "provider": "gmail",
        "account_id": message.get("account_id", ""),
        "message_id": message.get("message_id", ""),
        "summary": ai["text"],
        **ai,
    }


def draft_email_reply(args: Dict[str, Any]) -> Dict[str, Any]:
    message = read_email(args)
    intent = str(args.get("intent") or args.get("instructions") or "")
    ai = _generate_ai_text(build_reply_prompt(message, intent))
    return {
        "object": "gmail.draft_reply",
        "provider": "gmail",
        "account_id": message.get("account_id", ""),
        "message_id": message.get("message_id", ""),
        "draft_reply": ai["text"],
        **ai,
    }


_HANDLERS = {
    "list_email_accounts": list_email_accounts,
    "list_emails": list_emails,
    "search_emails": search_emails,
    "read_email": read_email,
    "send_email": send_email,
    "reply_to_email": reply_to_email,
    "archive_email": archive_email,
    "delete_email": delete_email,
    "mark_email_read": mark_email_read,
    "bulk_email": bulk_email,
    "summarize_email": summarize_email,
    "draft_email_reply": draft_email_reply,
}


def dispatch_mail_tool(
    name: str, args: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    handler = _HANDLERS.get(name)
    if handler is None:
        raise MailError(f"Unknown mail tool: {name}")
    return handler(args or {})


def _json_tool(name: str) -> Callable[[Dict[str, Any]], str]:
    def _handler(args: Dict[str, Any], **_: Any) -> str:
        try:
            return json.dumps(dispatch_mail_tool(name, args), ensure_ascii=False)
        except MailError as exc:
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
        "description": "Gmail account id; defaults to the configured default account.",
    },
}


registry.register(
    name="list_email_accounts",
    toolset=MAIL_TOOLSET,
    schema=_schema("list_email_accounts", "List configured Gmail mail accounts.", {}),
    handler=_json_tool("list_email_accounts"),
)
registry.register(
    name="list_emails",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "list_emails",
        "List Gmail messages in a normalized mailbox view.",
        {
            **_COMMON,
            "label": {
                "type": "string",
                "description": (
                    "Mailbox view: inbox, sent, drafts, trash, spam, archive, "
                    "starred, unread, important."
                ),
            },
            "limit": {
                "type": "integer",
                "description": "Maximum messages to return, 1-100.",
            },
            "page_token": {"type": "string", "description": "Gmail pagination token."},
        },
    ),
    handler=_json_tool("list_emails"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="search_emails",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "search_emails",
        "Search Gmail messages with Gmail search syntax.",
        {
            **_COMMON,
            "query": {"type": "string"},
            "limit": {"type": "integer"},
            "page_token": {"type": "string"},
        },
        ["query"],
    ),
    handler=_json_tool("search_emails"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="read_email",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "read_email",
        "Read one Gmail message by message_id.",
        {**_COMMON, "message_id": {"type": "string"}},
        ["message_id"],
    ),
    handler=_json_tool("read_email"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="send_email",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "send_email",
        (
            "Send a Gmail message. Requires approved=true after explicit user "
            "confirmation."
        ),
        {
            **_COMMON,
            "to": {"type": "array", "items": {"type": "string"}},
            "cc": {"type": "array", "items": {"type": "string"}},
            "bcc": {"type": "array", "items": {"type": "string"}},
            "subject": {"type": "string"},
            "body": {"type": "string"},
            "approved": {"type": "boolean"},
        },
        ["to", "subject", "body", "approved"],
    ),
    handler=_json_tool("send_email"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="reply_to_email",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "reply_to_email",
        "Reply to a Gmail message. Requires approved=true.",
        {
            **_COMMON,
            "message_id": {"type": "string"},
            "body": {"type": "string"},
            "approved": {"type": "boolean"},
        },
        ["message_id", "body", "approved"],
    ),
    handler=_json_tool("reply_to_email"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="archive_email",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "archive_email",
        "Archive one Gmail message by removing INBOX.",
        {**_COMMON, "message_id": {"type": "string"}},
        ["message_id"],
    ),
    handler=_json_tool("archive_email"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="delete_email",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "delete_email",
        "Move one Gmail message to trash. Requires approved=true.",
        {**_COMMON, "message_id": {"type": "string"}, "approved": {"type": "boolean"}},
        ["message_id", "approved"],
    ),
    handler=_json_tool("delete_email"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="mark_email_read",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "mark_email_read",
        "Mark one Gmail message read or unread.",
        {**_COMMON, "message_id": {"type": "string"}, "unread": {"type": "boolean"}},
        ["message_id"],
    ),
    handler=_json_tool("mark_email_read"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="bulk_email",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "bulk_email",
        "Apply an approved bulk Gmail operation.",
        {
            **_COMMON,
            "message_ids": {"type": "array", "items": {"type": "string"}},
            "operation": {"type": "string"},
            "approved": {"type": "boolean"},
        },
        ["message_ids", "operation", "approved"],
    ),
    handler=_json_tool("bulk_email"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="summarize_email",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "summarize_email",
        "Build or generate a manual summary for a Gmail message.",
        {**_COMMON, "message_id": {"type": "string"}},
        ["message_id"],
    ),
    handler=_json_tool("summarize_email"),
    check_fn=gmail_mail_available,
)
registry.register(
    name="draft_email_reply",
    toolset=MAIL_TOOLSET,
    schema=_schema(
        "draft_email_reply",
        "Build or generate a manual draft reply for a Gmail message.",
        {**_COMMON, "message_id": {"type": "string"}, "intent": {"type": "string"}},
        ["message_id"],
    ),
    handler=_json_tool("draft_email_reply"),
    check_fn=gmail_mail_available,
)
