from tools.google_calendar_store import GoogleCalendarCache, normalize_google_event


def _event(event_id="evt-1", summary="Planning", start="2026-06-02T10:00:00Z"):
    return {
        "id": event_id,
        "etag": "etag-1",
        "status": "confirmed",
        "summary": summary,
        "description": "Plan launch",
        "location": "Office",
        "htmlLink": "https://calendar.google.com/event",
        "iCalUID": f"{event_id}@google.com",
        "start": {"dateTime": start, "timeZone": "Europe/Stockholm"},
        "end": {"dateTime": "2026-06-02T10:30:00Z", "timeZone": "Europe/Stockholm"},
        "attendees": [{"email": "user@example.com", "responseStatus": "accepted"}],
        "organizer": {"email": "organizer@example.com"},
        "conferenceData": {"entryPoints": [{"uri": "https://meet.google.com/abc"}]},
        "recurrence": ["RRULE:FREQ=WEEKLY"],
        "updated": "2026-06-01T10:00:00Z",
        "created": "2026-05-31T10:00:00Z",
    }


def test_cache_creates_schema_and_persists_calendar_and_event(tmp_path):
    cache = GoogleCalendarCache(tmp_path / "calendar.sqlite3")
    cache.upsert_calendar(
        account_id="google_calendar:user@example.com",
        email="user@example.com",
        calendar={"id": "primary", "summary": "Work", "primary": True},
        synced_at=100.0,
    )
    normalized = normalize_google_event(
        account_id="google_calendar:user@example.com",
        email="user@example.com",
        calendar_id="primary",
        event=_event(),
        synced_at=101.0,
    )
    cache.upsert_event(normalized)

    calendars = cache.list_calendars(account_id="google_calendar:user@example.com")
    row = cache.get_event(
        account_id="google_calendar:user@example.com",
        calendar_id="primary",
        event_id="evt-1",
    )

    assert calendars[0]["calendar_id"] == "primary"
    assert calendars[0]["primary"] is True
    assert row is not None
    assert row["summary"] == "Planning"
    assert row["attendees"][0]["email"] == "user@example.com"
    assert row["recurrence"] == ["RRULE:FREQ=WEEKLY"]
    assert row["cache_source"] == "local_provider_cache"


def test_range_coverage_list_search_and_pagination(tmp_path):
    cache = GoogleCalendarCache(tmp_path / "calendar.sqlite3")
    account_id = "google_calendar:user@example.com"
    cache.upsert_event(
        normalize_google_event(
            account_id=account_id,
            email="user@example.com",
            calendar_id="primary",
            event=_event("evt-1", "Alpha planning", "2026-06-02T10:00:00Z"),
            synced_at=100.0,
        )
    )
    cache.upsert_event(
        normalize_google_event(
            account_id=account_id,
            email="user@example.com",
            calendar_id="primary",
            event=_event("evt-2", "Beta review", "2026-06-03T10:00:00Z"),
            synced_at=101.0,
        )
    )
    cache.record_sync_range(
        account_id=account_id,
        email="user@example.com",
        calendar_id="primary",
        start="2026-06-01T00:00:00Z",
        end="2026-06-10T00:00:00Z",
        count=2,
        synced_at=102.0,
    )

    first_page, next_token = cache.list_events(
        account_id=account_id,
        calendar_id="primary",
        start="2026-06-01T00:00:00Z",
        end="2026-06-10T00:00:00Z",
        limit=1,
    )
    search, _ = cache.search_events(
        account_id=account_id,
        calendar_id="primary",
        query="beta",
        start="2026-06-01T00:00:00Z",
        end="2026-06-10T00:00:00Z",
    )

    assert [event["event_id"] for event in first_page] == ["evt-1"]
    assert next_token == "1"
    assert [event["event_id"] for event in search] == ["evt-2"]
    assert cache.range_is_covered(
        account_id=account_id,
        calendar_id="primary",
        start="2026-06-02T00:00:00Z",
        end="2026-06-04T00:00:00Z",
    ) is True


def test_all_day_and_recurring_occurrence_identity(tmp_path):
    cache = GoogleCalendarCache(tmp_path / "calendar.sqlite3")
    account_id = "google_calendar:user@example.com"
    event = {
        "id": "series-1_20260602",
        "recurringEventId": "series-1",
        "originalStartTime": {"date": "2026-06-02"},
        "status": "confirmed",
        "summary": "Offsite",
        "start": {"date": "2026-06-02"},
        "end": {"date": "2026-06-03"},
    }
    cache.upsert_event(
        normalize_google_event(
            account_id=account_id,
            email="user@example.com",
            calendar_id="primary",
            event=event,
            synced_at=100.0,
        )
    )

    row = cache.get_event(
        account_id=account_id,
        calendar_id="primary",
        event_id="series-1_20260602",
    )

    assert row is not None
    assert row["all_day"] is True
    assert row["recurring_event_id"] == "series-1"
    assert "2026-06-02" in row["occurrence_id"]
