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
    def __init__(self, messages):
        self.messages = messages
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
