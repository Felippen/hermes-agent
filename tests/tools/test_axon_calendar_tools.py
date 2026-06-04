from __future__ import annotations

from contextlib import contextmanager

from tools import axon_calendar_tools as calendar


@contextmanager
def _spine_only(store):
    yield store


class _FakeProjection:
    def __init__(self, store) -> None:
        self.store = store

    def events(self, **kwargs):
        assert kwargs["calendar_id"] == "primary"
        assert kwargs["start"] == "2026-06-04T00:00:00Z"
        assert kwargs["end"] == "2026-06-05T00:00:00Z"
        return {
            "provider": "google_calendar",
            "calendar_id": "primary",
            "events": [{"event_id": "evt-1", "summary": "Axon planning"}],
            "cache_source": "axon_spine",
        }


def test_list_calendar_events_reads_from_axon_spine(monkeypatch):
    store = object()
    monkeypatch.setattr(calendar, "_spine_repository", lambda: _spine_only(store))
    monkeypatch.setattr(calendar, "_calendar_projection", lambda value: _FakeProjection(value))

    result = calendar.dispatch_calendar_tool(
        "list_calendar_events",
        {
            "calendar_id": "primary",
            "start": "2026-06-04T00:00:00Z",
            "end": "2026-06-05T00:00:00Z",
        },
    )

    assert result["cache_source"] == "axon_spine"
    assert result["events"][0]["event_id"] == "evt-1"
    assert result["events"][0]["summary"] == "Axon planning"


def test_create_calendar_event_requires_approval():
    try:
        calendar.dispatch_calendar_tool(
            "create_calendar_event",
            {
                "calendar_id": "primary",
                "summary": "Axon planning",
                "start": "2026-06-04T09:00:00Z",
            },
        )
    except calendar.CalendarError as exc:
        assert "requires explicit approval" in str(exc)
    else:
        raise AssertionError("create_calendar_event should require explicit approval")


def test_create_calendar_event_writes_spine_and_enqueues_outbox(monkeypatch):
    calls = []

    def fake_apply(*, op, entity_id, args, payload, status):
        calls.append({"op": op, "entity_id": entity_id, "args": args, "payload": payload, "status": status})
        return {
            "status": status,
            "provider": "google_calendar",
            "calendar_id": payload["calendar_id"],
            "event_id": payload["event_id"],
            "event": {"event_id": payload["event_id"], "summary": payload["summary"]},
            "cache_source": "axon_spine",
        }

    monkeypatch.setattr(calendar, "_apply_write", fake_apply)

    result = calendar.dispatch_calendar_tool(
        "create_calendar_event",
        {
            "approved": True,
            "calendar_id": "primary",
            "summary": "Axon planning",
            "start": "2026-06-04T09:00:00Z",
            "end": "2026-06-04T10:00:00Z",
            "timezone": "Europe/Stockholm",
        },
    )

    assert result["status"] == "created"
    assert result["cache_source"] == "axon_spine"
    assert result["event"]["summary"] == "Axon planning"
    assert calls[0]["op"] == "create"
    assert calls[0]["entity_id"] is None
    payload = calls[0]["payload"]
    assert payload["calendar_id"] == "primary"
    assert payload["event_id"].startswith("orynpending")
    assert payload["provider"] == "google"
    assert payload["etag"] == '"oryn-pending"'
    assert payload["attendees"] == []
    assert payload["raw"]["source"] == "oryn_workspace"
    assert payload["timezone"] == "Europe/Stockholm"


def test_axon_calendar_requested_uses_dsn_or_explicit_flag():
    assert calendar.axon_calendar_requested({}) is False
    assert calendar.axon_calendar_requested({calendar.ORYN_CALENDAR_AXON_BACKED: "1"}) is True
    assert calendar.axon_calendar_requested({calendar.ORYN_SPINE_POSTGRES_DSN: "postgresql://local/test"}) is True
