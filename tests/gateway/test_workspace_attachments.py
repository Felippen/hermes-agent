from __future__ import annotations

import base64

import pytest

from gateway.platforms.workspace_attachments import (
    MAX_WORKSPACE_ATTACHMENT_BYTES,
    save_workspace_attachment,
)


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


def test_save_workspace_attachment_writes_under_document_cache(hermes_home):
    saved = save_workspace_attachment(
        session_id="2a26d5d6-8f8b-4c39-8a58-ef10b3fb1a0b",
        original_filename="report.pdf",
        data=b"%PDF-1.4 sample",
        mime_type="application/pdf",
    )

    path = hermes_home / "cache" / "documents" / "workspace" / "2a26d5d6-8f8b-4c39-8a58-ef10b3fb1a0b"
    assert path.is_dir()
    written = next(path.iterdir())
    assert written.read_bytes() == b"%PDF-1.4 sample"
    assert saved["filename"] == "report.pdf"
    assert saved["mime_type"] == "application/pdf"
    assert saved["path"] == str(written.resolve())


def test_save_workspace_attachment_rejects_empty_body(hermes_home):
    with pytest.raises(ValueError, match="empty"):
        save_workspace_attachment(
            session_id="session-1",
            original_filename="empty.txt",
            data=b"",
        )


def test_save_workspace_attachment_rejects_oversize(hermes_home):
    with pytest.raises(ValueError, match="exceeds"):
        save_workspace_attachment(
            session_id="session-1",
            original_filename="big.bin",
            data=b"x" * (MAX_WORKSPACE_ATTACHMENT_BYTES + 1),
        )


@pytest.fixture
def auth_adapter():
    from tests.gateway.test_api_server import _make_adapter

    return _make_adapter(api_key="sk-secret")


@pytest.mark.asyncio
async def test_upload_session_attachment_endpoint(auth_adapter, monkeypatch, tmp_path):
    from aiohttp.test_utils import TestClient, TestServer

    from tests.gateway.test_api_server import _create_app

    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    app = _create_app(auth_adapter)

    payload = {
        "filename": "notes.txt",
        "mime_type": "text/plain",
        "content_base64": base64.b64encode(b"hello attachment").decode("ascii"),
    }

    async with TestClient(TestServer(app)) as cli:
        response = await cli.post(
            "/v1/sessions/session-123/attachments",
            json=payload,
            headers={"Authorization": "Bearer sk-secret"},
        )

        assert response.status == 200
        body = await response.json()
        assert body["object"] == "hermes.session.attachment"
        assert body["filename"] == "notes.txt"
        assert body["path"].endswith(".txt")

    assert (home / "cache" / "documents" / "workspace" / "session-123").is_dir()
