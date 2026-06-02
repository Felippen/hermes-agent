import json

from tools import google_calendar_oauth as oauth


def _configure_paths(monkeypatch, tmp_path):
    client_path = tmp_path / "google_client_secret.json"
    token_path = tmp_path / "google_token.json"
    pending_path = tmp_path / "google_gmail_oauth_pending.json"
    monkeypatch.setenv("HERMES_GMAIL_CLIENT_SECRETS_PATH", str(client_path))
    monkeypatch.setenv("HERMES_GMAIL_TOKEN_PATH", str(token_path))
    monkeypatch.setenv("HERMES_GMAIL_OAUTH_PENDING_PATH", str(pending_path))
    return client_path, token_path, pending_path


def test_calendar_status_reports_configuration_needed_without_client(monkeypatch, tmp_path):
    _configure_paths(monkeypatch, tmp_path)

    status = oauth.calendar_oauth_status()

    assert status["status"] == "configuration_needed"
    assert status["connected"] is False
    assert status["configured"] is False
    assert "access-token" not in json.dumps(status).lower()


def test_calendar_status_reports_missing_scope_without_breaking_google_token(monkeypatch, tmp_path):
    client_path, token_path, _pending_path = _configure_paths(monkeypatch, tmp_path)
    client_path.write_text(json.dumps({"installed": {"client_id": "client-id"}}), encoding="utf-8")
    token_path.write_text(
        json.dumps({
            "token": "access-token",
            "refresh_token": "refresh-token",
            "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
        }),
        encoding="utf-8",
    )

    status = oauth.calendar_oauth_status()

    assert status["status"] == "missing_scopes"
    assert status["connected"] is False
    assert status["configured"] is True
    assert status["missing_scopes"] == ["https://www.googleapis.com/auth/calendar"]


def test_calendar_status_connected_with_calendar_scope(monkeypatch, tmp_path):
    client_path, token_path, _pending_path = _configure_paths(monkeypatch, tmp_path)
    client_path.write_text(json.dumps({"installed": {"client_id": "client-id"}}), encoding="utf-8")
    token_path.write_text(
        json.dumps({
            "token": "access-token",
            "refresh_token": "refresh-token",
            "scopes": ["https://www.googleapis.com/auth/calendar"],
        }),
        encoding="utf-8",
    )

    status = oauth.calendar_oauth_status()

    assert status["status"] == "connected"
    assert status["connected"] is True
    assert status["missing_scopes"] == []


def test_calendar_start_requests_mail_and_calendar_scopes(monkeypatch, tmp_path):
    from tools import gmail_oauth

    client_path, _token_path, pending_path = _configure_paths(monkeypatch, tmp_path)
    client_path.write_text(json.dumps({"installed": {"client_id": "client-id"}}), encoding="utf-8")
    captured = {}

    class FakeFlow:
        code_verifier = "verifier-1"

        def authorization_url(self, **kwargs):
            assert kwargs["access_type"] == "offline"
            assert kwargs["prompt"] == "consent"
            return "https://accounts.google.com/o/oauth2/v2/auth?state=state-1", "state-1"

    def fake_flow_from_client_secret(**kwargs):
        captured.update(kwargs)
        return FakeFlow()

    monkeypatch.setattr(gmail_oauth, "_flow_from_client_secret", fake_flow_from_client_secret)

    result = oauth.start_calendar_oauth("http://127.0.0.1:8642/v1/mail/oauth/callback")

    assert result["object"] == "google_calendar.oauth_start"
    assert result["provider"] == "google_calendar"
    assert result["status"] == "pending"
    assert "https://www.googleapis.com/auth/calendar" in captured["scopes"]
    assert "https://www.googleapis.com/auth/gmail.readonly" in captured["scopes"]
    pending = json.loads(pending_path.read_text(encoding="utf-8"))
    assert "https://www.googleapis.com/auth/calendar" in pending["requested_scopes"]
