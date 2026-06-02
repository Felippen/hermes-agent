from tools.gmail_mail_store import GmailMailCache


def _message(message_id="msg-1", labels=None):
    return {
        "message_id": message_id,
        "thread_id": "thread-1",
        "from": "sender@example.com",
        "to": "user@example.com",
        "cc": "cc@example.com",
        "date": "Mon, 01 Jun 2026 10:00:00 +0000",
        "subject": "Status update",
        "snippet": "hello snippet",
        "labels": labels or ["INBOX", "UNREAD", "STARRED"],
        "is_read": False,
        "is_starred": True,
        "message_id_header": "<msg-1@example.com>",
        "reply_to": "reply@example.com",
        "text_body": "Plain body about AlphaOmega",
        "html_body": "<p>Plain body about AlphaOmega</p>",
        "attachments": [
            {
                "filename": "report.pdf",
                "mime_type": "application/pdf",
                "size": 123,
                "attachment_id": "att-1",
            }
        ],
    }


def test_cache_creates_schema_and_persists_message(tmp_path):
    cache = GmailMailCache(tmp_path / "gmail.sqlite3")
    cache.upsert_message(
        account_id="gmail:user@example.com",
        email="user@example.com",
        message=_message(),
        synced_at=100.0,
    )

    row = cache.get_message(account_id="gmail:user@example.com", message_id="msg-1")

    assert row is not None
    assert row["subject"] == "Status update"
    assert row["text_body"] == "Plain body about AlphaOmega"
    assert row["attachments"][0]["filename"] == "report.pdf"
    assert row["cache_source"] == "local_provider_cache"


def test_list_filters_normalized_mailboxes_and_paginates(tmp_path):
    cache = GmailMailCache(tmp_path / "gmail.sqlite3")
    account_id = "gmail:user@example.com"
    cache.upsert_message(
        account_id=account_id,
        email="user@example.com",
        message=_message("msg-1", ["INBOX"]),
        synced_at=100.0,
    )
    cache.upsert_message(
        account_id=account_id,
        email="user@example.com",
        message=_message("msg-2", ["TRASH"]),
        synced_at=101.0,
    )
    cache.record_sync(
        account_id=account_id,
        email="user@example.com",
        label="inbox",
        query="in:inbox",
        limit=10,
        count=1,
        synced_at=101.0,
    )

    inbox, next_token = cache.list_messages(account_id=account_id, label="inbox", limit=1)
    trash, _ = cache.list_messages(account_id=account_id, label="trash", limit=10)

    assert [row["message_id"] for row in inbox] == ["msg-1"]
    assert next_token == ""
    assert [row["message_id"] for row in trash] == ["msg-2"]
    assert cache.has_sync_state(account_id, label="inbox", query="in:inbox") is True


def test_search_matches_headers_labels_and_body(tmp_path):
    cache = GmailMailCache(tmp_path / "gmail.sqlite3")
    account_id = "gmail:user@example.com"
    cache.upsert_message(
        account_id=account_id,
        email="user@example.com",
        message=_message("msg-1", ["INBOX"]),
    )

    by_body, _ = cache.search_messages(
        account_id=account_id,
        query="alphaomega",
        label="inbox",
    )
    by_sender, _ = cache.search_messages(
        account_id=account_id,
        query="sender@example.com",
        label="inbox",
    )

    assert by_body[0]["message_id"] == "msg-1"
    assert by_sender[0]["message_id"] == "msg-1"


def test_label_delta_updates_cached_views(tmp_path):
    cache = GmailMailCache(tmp_path / "gmail.sqlite3")
    account_id = "gmail:user@example.com"
    cache.upsert_message(
        account_id=account_id,
        email="user@example.com",
        message=_message("msg-1", ["INBOX", "UNREAD"]),
    )
    cache.apply_label_delta(
        account_id=account_id,
        message_id="msg-1",
        remove_labels=["INBOX", "UNREAD"],
    )

    inbox, _ = cache.list_messages(account_id=account_id, label="inbox")
    row = cache.get_message(account_id=account_id, message_id="msg-1")

    assert inbox == []
    assert row is not None
    assert row["is_read"] is True
    assert "INBOX" not in row["labels"]
