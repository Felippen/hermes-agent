import base64
import json

import pytest

from tools import gmail_mail_tools as mail


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("utf-8").rstrip("=")


class FakeCall:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeMessages:
    def __init__(self, service):
        self.service = service

    def list(self, **kwargs):
        self.service.list_calls.append(kwargs)
        if self.service.pages is not None:
            page_token = kwargs.get("pageToken") or ""
            ids, next_page_token = self.service.pages.get(page_token, ([], ""))
            return FakeCall({
                "messages": [{"id": key} for key in ids],
                "nextPageToken": next_page_token,
            })
        if kwargs.get("pageToken"):
            return FakeCall({"messages": []})
        return FakeCall({
            "messages": [{"id": key} for key in self.service.messages.keys()],
            "nextPageToken": "next-page",
        })

    def get(self, **kwargs):
        self.service.get_calls.append(kwargs)
        return FakeCall(self.service.messages[kwargs["id"]])

    def send(self, **kwargs):
        self.service.send_calls.append(kwargs)
        return FakeCall({
            "id": "sent-1",
            "threadId": kwargs["body"].get("threadId", "thread-sent"),
        })

    def modify(self, **kwargs):
        self.service.modify_calls.append(kwargs)
        return FakeCall({
            "id": kwargs["id"],
            "labelIds": kwargs["body"].get("addLabelIds", []),
        })

    def trash(self, **kwargs):
        self.service.trash_calls.append(kwargs)
        return FakeCall({"id": kwargs["id"], "labelIds": ["TRASH"]})


class FakeUsers:
    def __init__(self, service):
        self.service = service

    def messages(self):
        return FakeMessages(self.service)


class FakeGmailService:
    def __init__(self, messages, pages=None):
        self.messages = messages
        self.pages = pages
        self.list_calls = []
        self.get_calls = []
        self.send_calls = []
        self.modify_calls = []
        self.trash_calls = []

    def users(self):
        return FakeUsers(self)


@pytest.fixture(autouse=True)
def clear_mail_state(monkeypatch, tmp_path):
    token_path = tmp_path / "google_token.json"
    token_path.write_text(json.dumps({"token": "tok", "scopes": mail.SCOPES}))
    monkeypatch.setenv("HERMES_GMAIL_TOKEN_PATH", str(token_path))
    monkeypatch.setenv("HERMES_GMAIL_ACCOUNT_EMAILS", "user@example.com")
    monkeypatch.setenv("HERMES_GMAIL_MAIL_CACHE_PATH", str(tmp_path / "gmail.sqlite3"))
    monkeypatch.delenv("HERMES_GMAIL_CLIENT_SECRETS_PATH", raising=False)
    mail._list_cache.clear()
    mail._read_cache.clear()
    mail.set_mail_ai_generator(None)
    yield
    mail._list_cache.clear()
    mail._read_cache.clear()
    mail.set_mail_ai_generator(None)


def _message(message_id="msg-1", labels=None):
    return {
        "id": message_id,
        "threadId": "thread-1",
        "snippet": "hello snippet",
        "labelIds": labels or ["INBOX", "UNREAD", "STARRED"],
        "payload": {
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "user@example.com"},
                {"name": "Cc", "value": "cc@example.com"},
                {"name": "Subject", "value": "Status"},
                {"name": "Date", "value": "Mon, 01 Jun 2026 10:00:00 +0000"},
                {"name": "Message-ID", "value": "<msg-1@example.com>"},
            ],
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": _b64("Plain body")},
                },
                {
                    "mimeType": "text/html",
                    "body": {"data": _b64("<p>HTML body</p>")},
                },
                {
                    "filename": "report.pdf",
                    "mimeType": "application/pdf",
                    "body": {"attachmentId": "att-1", "size": 123},
                },
            ],
        },
    }


def test_list_accounts_from_profile_env():
    result = mail.list_email_accounts()
    assert result["configured"] is True
    assert result["data"][0]["account_id"] == "gmail:user@example.com"
    assert result["data"][0]["provider"] == "gmail"


def test_list_emails_maps_view_and_uses_cache(monkeypatch):
    fake = FakeGmailService({"msg-1": _message()})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    first = mail.list_emails({"label": "archive", "limit": 10})
    second = mail.list_emails({"label": "archive", "limit": 10})

    assert first["cache"] == "miss"
    assert second["cache"] == "hit"
    assert fake.list_calls[0]["q"] == "-in:inbox -in:sent -in:drafts -in:trash -in:spam"
    assert first["data"][0]["message_id"] == "msg-1"
    assert first["data"][0]["is_read"] is False
    assert first["data"][0]["is_starred"] is True
    assert len(fake.list_calls) == 1


def test_read_email_parses_body_attachment_and_cache(monkeypatch):
    fake = FakeGmailService({"msg-1": _message()})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    first = mail.read_email({"message_id": "msg-1"})
    second = mail.read_email({"message_id": "msg-1"})

    assert first["text_body"] == "Plain body"
    assert first["html_body"] == "<p>HTML body</p>"
    assert first["attachments"] == [
        {
            "filename": "report.pdf",
            "mime_type": "application/pdf",
            "size": 123,
            "attachment_id": "att-1",
        }
    ]
    assert second["cache"] == "hit"
    assert len(fake.get_calls) == 1


def test_sync_email_populates_provider_cache_for_list_and_read(monkeypatch):
    fake = FakeGmailService({"msg-1": _message()})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    sync = mail.sync_email({"label": "inbox", "limit": 10})
    assert sync["synced"] == 1
    assert sync["cache_source"] == "gmail_api"

    mail._list_cache.clear()
    mail._read_cache.clear()

    def fail_build(_account):
        pytest.fail("Gmail API should not be called for synced cache reads")

    monkeypatch.setattr(mail, "build_gmail_service", fail_build)
    listed = mail.list_emails({"label": "inbox", "limit": 10})
    read = mail.read_email({"message_id": "msg-1"})

    assert listed["cache_source"] == "local_provider_cache"
    assert listed["data"][0]["message_id"] == "msg-1"
    assert read["cache_source"] == "local_provider_cache"
    assert read["text_body"] == "Plain body"


def test_sync_email_follows_gmail_pages_until_requested_limit(monkeypatch):
    messages = {
        "sent-1": _message("sent-1", labels=["SENT"]),
        "sent-2": _message("sent-2", labels=["SENT"]),
        "sent-3": _message("sent-3", labels=["SENT"]),
        "sent-4": _message("sent-4", labels=["SENT"]),
    }
    fake = FakeGmailService(
        messages,
        pages={
            "": (["sent-1", "sent-2"], "page-2"),
            "page-2": (["sent-3"], "page-3"),
            "page-3": (["sent-4"], ""),
        },
    )
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    sync = mail.sync_email({"label": "sent", "limit": 3})
    listed = mail.list_emails({"label": "sent", "limit": 10})

    assert sync["synced"] == 3
    assert sync["next_page_token"] == "page-3"
    assert [call["pageToken"] for call in fake.list_calls] == [None, "page-2"]
    assert [call["maxResults"] for call in fake.list_calls] == [3, 1]
    assert {row["message_id"] for row in listed["data"]} == {"sent-1", "sent-2", "sent-3"}


def test_archive_reconciles_cached_inbox_view(monkeypatch):
    fake = FakeGmailService({"msg-1": _message()})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    mail.sync_email({"label": "inbox", "limit": 10})
    archived = mail.archive_email({"message_id": "msg-1"})
    inbox = mail.list_emails({"label": "inbox", "limit": 10})
    row = mail.read_email({"message_id": "msg-1"})

    assert archived["status"] == "modified"
    assert inbox["cache_source"] == "local_provider_cache"
    assert inbox["data"] == []
    assert "INBOX" not in row["labels"]


def test_send_requires_approval(monkeypatch):
    fake = FakeGmailService({"msg-1": _message()})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    with pytest.raises(mail.MailError, match="requires explicit approval"):
        mail.send_email({"to": ["to@example.com"], "subject": "Hi", "body": "Body"})

    result = mail.send_email({
        "to": ["to@example.com"],
        "subject": "Hi",
        "body": "Body",
        "approved": True,
    })
    assert result["status"] == "sent"
    assert fake.send_calls


def test_reply_builds_threaded_send(monkeypatch):
    fake = FakeGmailService({"msg-1": _message()})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    result = mail.reply_to_email({
        "message_id": "msg-1",
        "body": "Thanks",
        "approved": True,
    })

    assert result["thread_id"] == "thread-1"
    sent_body = fake.send_calls[0]["body"]
    assert sent_body["threadId"] == "thread-1"


def test_mutations_and_bulk_require_approval(monkeypatch):
    fake = FakeGmailService({"msg-1": _message(), "msg-2": _message("msg-2")})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)

    archived = mail.archive_email({"message_id": "msg-1"})
    assert archived["status"] == "modified"
    assert fake.modify_calls[-1]["body"]["removeLabelIds"] == ["INBOX"]

    with pytest.raises(mail.MailError, match="requires explicit approval"):
        mail.delete_email({"message_id": "msg-1"})

    bulk = mail.bulk_email({
        "message_ids": ["msg-1", "msg-2"],
        "operation": "mark_unread",
        "approved": True,
    })
    assert bulk["status"] == "completed"
    assert fake.modify_calls[-1]["body"]["addLabelIds"] == ["UNREAD"]


def test_manual_ai_prompt_generation(monkeypatch):
    fake = FakeGmailService({"msg-1": _message()})
    monkeypatch.setattr(mail, "build_gmail_service", lambda account: fake)
    mail.set_mail_ai_generator(lambda prompt: "Generated: " + prompt.splitlines()[0])

    summary = mail.summarize_email({"message_id": "msg-1"})
    draft = mail.draft_email_reply({"message_id": "msg-1", "intent": "Say yes"})

    assert summary["status"] == "generated"
    assert summary["summary"].startswith("Generated: Summarize this email")
    assert "requested actions" in summary["prompt"]
    assert draft["draft_reply"].startswith("Generated: Draft a reply")
    assert "User intent: Say yes" in draft["prompt"]
