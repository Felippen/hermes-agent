import json

from tools import google_calendar_tools as calendar


def _configure(monkeypatch, tmp_path):
    token_path = tmp_path / "google_token.json"
    client_path = tmp_path / "google_client_secret.json"
    cache_path = tmp_path / "calendar.sqlite3"
    token_path.write_text(
        json.dumps({
            "token": "access-token",
            "refresh_token": "refresh-token",
            "scopes": ["https://www.googleapis.com/auth/calendar"],
        }),
        encoding="utf-8",
    )
    client_path.write_text(json.dumps({"installed": {"client_id": "client-id"}}), encoding="utf-8")
    monkeypatch.setenv("HERMES_GMAIL_TOKEN_PATH", str(token_path))
    monkeypatch.setenv("HERMES_GMAIL_CLIENT_SECRETS_PATH", str(client_path))
    monkeypatch.setenv("HERMES_GOOGLE_CALENDAR_CACHE_PATH", str(cache_path))
    monkeypatch.setenv("HERMES_GOOGLE_CALENDAR_ACCOUNT_EMAILS", "user@example.com")
    calendar._list_cache.clear()
    calendar._read_cache.clear()
    return cache_path


def _event(event_id="evt-1", summary="Planning"):
    return {
        "id": event_id,
        "etag": "etag-1",
        "status": "confirmed",
        "summary": summary,
        "start": {"dateTime": "2026-06-02T10:00:00Z", "timeZone": "Europe/Stockholm"},
        "end": {"dateTime": "2026-06-02T10:30:00Z", "timeZone": "Europe/Stockholm"},
        "attendees": [{"email": "user@example.com", "responseStatus": "needsAction"}],
    }


class FakeCall:
    def __init__(self, payload=None, hook=None):
        self.payload = payload or {}
        self.hook = hook

    def execute(self):
        if self.hook:
            self.hook()
        return self.payload


class FakeCalendarList:
    def list(self, **kwargs):
        return FakeCall({
            "items": [
                {
                    "id": "primary",
                    "summary": "Work",
                    "primary": True,
                    "accessRole": "owner",
                    "backgroundColor": "#111111",
                }
            ]
        })


class FakeEvents:
    def __init__(self, service):
        self.service = service

    def list(self, **kwargs):
        self.service.calls.append(("list", kwargs))
        page = len([call for call in self.service.calls if call[0] == "list"])
        if page == 1 and self.service.second_page:
            return FakeCall({"items": [self.service.events_payload[0]], "nextPageToken": "page-2"})
        if page == 2 and self.service.second_page:
            return FakeCall({"items": [self.service.events_payload[1]]})
        return FakeCall({"items": self.service.events_payload})

    def get(self, **kwargs):
        self.service.calls.append(("get", kwargs))
        event_id = kwargs.get("eventId")
        event = next((item for item in self.service.events_payload if item["id"] == event_id), _event(event_id))
        return FakeCall(event)

    def insert(self, **kwargs):
        self.service.calls.append(("insert", kwargs))
        body = kwargs["body"]
        return FakeCall({**_event("created-1", body["summary"]), **body, "id": "created-1"})

    def patch(self, **kwargs):
        self.service.calls.append(("patch", kwargs))
        body = kwargs["body"]
        event_id = kwargs["eventId"]
        current = _event(event_id)
        current.update(body)
        current["id"] = event_id
        return FakeCall(current)

    def delete(self, **kwargs):
        self.service.calls.append(("delete", kwargs))
        return FakeCall({})


class FakeCalendarService:
    def __init__(self, events_payload=None, *, second_page=False):
        self.events_payload = events_payload or [_event()]
        self.second_page = second_page
        self.calls = []
        self._events = FakeEvents(self)
        self._calendar_list = FakeCalendarList()

    def events(self):
        return self._events

    def calendarList(self):
        return self._calendar_list


def test_list_accounts_reports_calendar_oauth_status(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)

    result = calendar.list_calendar_accounts()

    assert result["configured"] is True
    assert result["oauth"]["connected"] is True
    assert result["data"][0]["provider"] == "google_calendar"


def test_list_accounts_disables_calendar_when_scope_missing(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    token_path = tmp_path / "google_token.json"
    token_path.write_text(
        json.dumps({
            "token": "access-token",
            "refresh_token": "refresh-token",
            "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
        }),
        encoding="utf-8",
    )

    result = calendar.list_calendar_accounts()

    assert result["configured"] is False
    assert result["oauth"]["status"] == "missing_scopes"
    assert result["data"][0]["enabled"] is False


def test_sync_calendar_populates_cache_and_list_uses_local(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    fake = FakeCalendarService([_event("evt-1", "Planning")])
    monkeypatch.setattr(calendar, "build_calendar_service", lambda _account: fake)

    sync = calendar.sync_calendar({
        "calendar_id": "primary",
        "start": "2026-06-01T00:00:00Z",
        "end": "2026-06-10T00:00:00Z",
    })
    fake.calls.clear()
    listed = calendar.list_calendar_events({
        "calendar_id": "primary",
        "start": "2026-06-01T00:00:00Z",
        "end": "2026-06-10T00:00:00Z",
    })

    assert sync["synced"] == 1
    assert listed["cache_source"] == "local_provider_cache"
    assert listed["data"][0]["event_id"] == "evt-1"
    assert fake.calls == []


def test_sync_calendar_follows_pages_until_limit(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    fake = FakeCalendarService([_event("evt-1"), _event("evt-2")], second_page=True)
    monkeypatch.setattr(calendar, "build_calendar_service", lambda _account: fake)

    sync = calendar.sync_calendar({
        "calendar_id": "primary",
        "start": "2026-06-01T00:00:00Z",
        "end": "2026-06-10T00:00:00Z",
        "limit": 2,
    })

    assert sync["synced"] == 2
    assert [call[0] for call in fake.calls].count("list") == 2


def test_create_requires_approval(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    fake = FakeCalendarService()
    monkeypatch.setattr(calendar, "build_calendar_service", lambda _account: fake)

    try:
        calendar.create_calendar_event({
            "calendar_id": "primary",
            "summary": "Planning",
            "start": "2026-06-02T10:00:00Z",
        })
    except calendar.CalendarError as exc:
        assert "requires explicit approval" in str(exc)
    else:
        raise AssertionError("expected approval error")

    assert fake.calls == []


def test_create_update_delete_and_respond_payloads(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    fake = FakeCalendarService([_event("evt-1")])
    monkeypatch.setattr(calendar, "build_calendar_service", lambda _account: fake)

    created = calendar.create_calendar_event({
        "calendar_id": "primary",
        "summary": "New meeting",
        "start": "2026-06-02T12:00:00Z",
        "end": "2026-06-02T12:30:00Z",
        "attendees": ["a@example.com"],
        "approved": True,
    })
    updated = calendar.update_calendar_event({
        "calendar_id": "primary",
        "event_id": "evt-1",
        "summary": "Updated",
        "approved": True,
    })
    responded = calendar.respond_to_calendar_event({
        "calendar_id": "primary",
        "event_id": "evt-1",
        "response_status": "accepted",
        "approved": True,
    })
    deleted = calendar.delete_calendar_event({
        "calendar_id": "primary",
        "event_id": "evt-1",
        "approved": True,
    })

    assert created["event_id"] == "created-1"
    assert updated["status"] == "updated"
    assert responded["response_status"] == "accepted"
    assert deleted["status"] == "deleted"
    assert [call[0] for call in fake.calls] == ["insert", "patch", "get", "patch", "delete"]
    assert fake.calls[0][1]["body"]["attendees"] == [{"email": "a@example.com"}]


def test_duplicate_create_returns_existing_event(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    fake = FakeCalendarService([_event("evt-1", "Planning")])
    monkeypatch.setattr(calendar, "build_calendar_service", lambda _account: fake)
    calendar.sync_calendar({
        "calendar_id": "primary",
        "start": "2026-06-01T00:00:00Z",
        "end": "2026-06-10T00:00:00Z",
    })
    fake.calls.clear()

    result = calendar.create_calendar_event({
        "calendar_id": "primary",
        "summary": "Planning",
        "start": "2026-06-02T10:00:00Z",
        "end": "2026-06-02T10:30:00Z",
        "approved": True,
    })

    assert result["status"] == "duplicate"
    assert result["event_id"] == "evt-1"
    assert fake.calls == []
