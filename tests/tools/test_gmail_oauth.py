import json

from tools import gmail_oauth as oauth


def _configure_paths(monkeypatch, tmp_path):
    client_path = tmp_path / "google_client_secret.json"
    token_path = tmp_path / "google_token.json"
    pending_path = tmp_path / "google_gmail_oauth_pending.json"
    monkeypatch.setenv("HERMES_GMAIL_CLIENT_SECRETS_PATH", str(client_path))
    monkeypatch.setenv("HERMES_GMAIL_TOKEN_PATH", str(token_path))
    monkeypatch.setenv("HERMES_GMAIL_OAUTH_PENDING_PATH", str(pending_path))
    return client_path, token_path, pending_path


def _write_client_secret(path):
    path.write_text(json.dumps({"installed": {"client_id": "client-id"}}), encoding="utf-8")


def test_status_reports_configuration_needed_without_client(monkeypatch, tmp_path):
    _configure_paths(monkeypatch, tmp_path)

    status = oauth.gmail_oauth_status()

    assert status["status"] == "configuration_needed"
    assert status["connected"] is False
    assert status["configured"] is False
    assert "access-token" not in json.dumps(status).lower()


def test_start_creates_authorization_url_and_pending_state(monkeypatch, tmp_path):
    client_path, _token_path, pending_path = _configure_paths(monkeypatch, tmp_path)
    _write_client_secret(client_path)

    class FakeFlow:
        code_verifier = "verifier-1"

        def authorization_url(self, **kwargs):
            assert kwargs["access_type"] == "offline"
            assert kwargs["prompt"] == "consent"
            return "https://accounts.google.com/o/oauth2/v2/auth?state=state-1", "state-1"

    monkeypatch.setattr(oauth, "_flow_from_client_secret", lambda **_kwargs: FakeFlow())

    result = oauth.start_gmail_oauth("http://127.0.0.1:8642/v1/mail/oauth/callback")

    assert result["status"] == "pending"
    assert result["authorization_url"].startswith("https://accounts.google.com/")
    pending = json.loads(pending_path.read_text(encoding="utf-8"))
    assert pending["state"] == "state-1"
    assert pending["code_verifier"] == "verifier-1"
    assert pending["redirect_uri"].endswith("/v1/mail/oauth/callback")


def test_callback_rejects_state_mismatch(monkeypatch, tmp_path):
    client_path, token_path, pending_path = _configure_paths(monkeypatch, tmp_path)
    _write_client_secret(client_path)
    pending_path.write_text(
        json.dumps({
            "state": "expected-state",
            "code_verifier": "verifier",
            "redirect_uri": "http://127.0.0.1:8642/v1/mail/oauth/callback",
        }),
        encoding="utf-8",
    )

    try:
        oauth.complete_gmail_oauth_callback(code="code-1", state="wrong-state")
    except oauth.GmailOAuthError as exc:
        assert exc.status == "failed"
        assert "state mismatch" in str(exc)
    else:
        raise AssertionError("expected state mismatch")

    assert not token_path.exists()


def test_callback_writes_token_and_clears_pending(monkeypatch, tmp_path):
    client_path, token_path, pending_path = _configure_paths(monkeypatch, tmp_path)
    _write_client_secret(client_path)
    pending_path.write_text(
        json.dumps({
            "state": "state-1",
            "code_verifier": "verifier",
            "redirect_uri": "http://127.0.0.1:8642/v1/mail/oauth/callback",
        }),
        encoding="utf-8",
    )

    class FakeCredentials:
        granted_scopes = list(oauth.GMAIL_SCOPES)

        def to_json(self):
            return json.dumps({
                "token": "access-token",
                "refresh_token": "refresh-token",
                "client_id": "client-id",
                "client_secret": "client-secret",
            })

    class FakeFlow:
        credentials = FakeCredentials()

        def fetch_token(self, *, code):
            assert code == "code-1"

    monkeypatch.setattr(oauth, "_flow_from_client_secret", lambda **_kwargs: FakeFlow())

    result = oauth.complete_gmail_oauth_callback(code="code-1", state="state-1")

    assert result["status"] == "connected"
    assert result["connected"] is True
    assert token_path.exists()
    assert not pending_path.exists()
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    assert payload["type"] == "authorized_user"
    assert payload["refresh_token"] == "refresh-token"
    assert payload["scopes"] == oauth.GMAIL_SCOPES
