"""Tests for the optional codex app-server runtime gate.

These are unit tests for the api_mode rewriter and the wire-level transport
module. They do NOT require the `codex` CLI to be installed — that's
covered by a separate live test gated on `codex --version`.
"""

from __future__ import annotations

import os

import pytest

from hermes_cli.runtime_provider import (
    _VALID_API_MODES,
    _maybe_apply_codex_app_server_runtime,
)


class TestApiModeRegistration:
    """The new api_mode must be registered or downstream parsing rejects it."""

    def test_codex_app_server_is_a_valid_api_mode(self) -> None:
        assert "codex_app_server" in _VALID_API_MODES

    def test_existing_api_modes_still_present(self) -> None:
        # Regression guard: don't accidentally delete other api_modes when
        # touching this set.
        for mode in (
            "chat_completions",
            "codex_responses",
            "anthropic_messages",
            "bedrock_converse",
        ):
            assert mode in _VALID_API_MODES


class TestMaybeApplyCodexAppServerRuntime:
    """The opt-in helper that rewrites api_mode → codex_app_server."""

    @pytest.mark.parametrize(
        "model_cfg",
        [
            None,
            {},
            {"openai_runtime": ""},
            {"openai_runtime": "auto"},
            {"openai_runtime": "AUTO"},
            {"other_key": "codex_app_server"},  # wrong key
        ],
    )
    def test_default_off_for_openai(self, model_cfg) -> None:
        """Default behavior is preserved when the flag is unset/auto."""
        got = _maybe_apply_codex_app_server_runtime(
            provider="openai", api_mode="chat_completions", model_cfg=model_cfg
        )
        assert got == "chat_completions"

    def test_opt_in_rewrites_openai(self) -> None:
        got = _maybe_apply_codex_app_server_runtime(
            provider="openai",
            api_mode="chat_completions",
            model_cfg={"openai_runtime": "codex_app_server"},
        )
        assert got == "codex_app_server"

    def test_opt_in_rewrites_openai_codex(self) -> None:
        got = _maybe_apply_codex_app_server_runtime(
            provider="openai-codex",
            api_mode="codex_responses",
            model_cfg={"openai_runtime": "codex_app_server"},
        )
        assert got == "codex_app_server"

    def test_case_insensitive(self) -> None:
        got = _maybe_apply_codex_app_server_runtime(
            provider="openai",
            api_mode="chat_completions",
            model_cfg={"openai_runtime": "Codex_App_Server"},
        )
        assert got == "codex_app_server"

    @pytest.mark.parametrize(
        "provider",
        [
            "anthropic",
            "openrouter",
            "xai",
            "qwen-oauth",
            "google-gemini-cli",
            "opencode-zen",
            "bedrock",
            "",
        ],
    )
    def test_other_providers_never_rerouted(self, provider) -> None:
        """Non-OpenAI providers MUST NOT be rerouted even with the flag set —
        codex's app-server can only run OpenAI/Codex auth flows."""
        got = _maybe_apply_codex_app_server_runtime(
            provider=provider,
            api_mode="anthropic_messages",
            model_cfg={"openai_runtime": "codex_app_server"},
        )
        assert got == "anthropic_messages", (
            f"provider={provider!r} should not be rerouted to codex_app_server"
        )


class TestCodexAppServerModule:
    """Module-surface tests for the JSON-RPC speaker. Don't require codex CLI."""

    def test_module_imports(self) -> None:
        from agent.transports import codex_app_server

        assert codex_app_server.MIN_CODEX_VERSION >= (0, 1, 0)
        assert callable(codex_app_server.parse_codex_version)
        assert callable(codex_app_server.check_codex_binary)

    def test_parse_codex_version_valid(self) -> None:
        from agent.transports.codex_app_server import parse_codex_version

        assert parse_codex_version("codex-cli 0.130.0") == (0, 130, 0)
        assert parse_codex_version("codex-cli 1.2.3 (extra metadata)") == (1, 2, 3)
        assert parse_codex_version("codex 99.0.1\n") == (99, 0, 1)

    def test_parse_codex_version_invalid(self) -> None:
        from agent.transports.codex_app_server import parse_codex_version

        assert parse_codex_version("nope") is None
        assert parse_codex_version("") is None
        assert parse_codex_version(None) is None  # type: ignore[arg-type]

    def test_check_binary_handles_missing_executable(self) -> None:
        from agent.transports.codex_app_server import check_codex_binary

        ok, msg = check_codex_binary(codex_bin="/nonexistent/codex/binary/path")
        assert ok is False
        assert "not found" in msg.lower() or "no such" in msg.lower()

    def test_codex_error_class_is_runtimeerror(self) -> None:
        from agent.transports.codex_app_server import CodexAppServerError

        err = CodexAppServerError(code=-32600, message="boom")
        assert isinstance(err, RuntimeError)
        assert "boom" in str(err)
        assert "-32600" in str(err)


class TestSpawnEnvIsolation:
    """The codex spawn must NOT rewrite HOME — codex's shell tool spawns
    subprocesses (gh, git, npm, aws, gcloud, ...) that need to find their
    config in the real user $HOME. CODEX_HOME isolates codex's own state,
    HOME stays unchanged.

    OpenClaw hit this footgun (openclaw/openclaw#81562) — they were
    rewriting HOME to a synthetic per-agent dir alongside CODEX_HOME,
    and then `gh auth status` / git config / etc. all broke inside codex
    shell calls. We avoid the same bug by only overlaying CODEX_HOME and
    RUST_LOG on top of os.environ.copy().
    """

    def test_spawn_env_preserves_HOME(self, monkeypatch):
        """The spawn env must contain the parent process's HOME unchanged.
        Verifies via a subprocess-monkey-patch."""
        import subprocess
        from agent.transports import codex_app_server as cas

        captured = {}

        class FakePopen:
            def __init__(self, cmd, *args, **kwargs):
                captured["env"] = kwargs.get("env", {}).copy()
                # Provide minimal Popen surface so __init__ doesn't crash
                # on attribute access during construction.
                self.stdin = None
                self.stdout = None
                self.stderr = None
                self.pid = 1
                self.returncode = None

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

            def kill(self):
                pass

        monkeypatch.setattr(subprocess, "Popen", FakePopen)
        monkeypatch.setenv("HOME", "/users/alice")

        client = cas.CodexAppServerClient(codex_bin="codex")
        client._closed = True  # so close() is a no-op

        # The spawn env must have HOME=/users/alice unchanged
        assert captured["env"].get("HOME") == "/users/alice", (
            f"HOME got rewritten in codex spawn env: "
            f"{captured['env'].get('HOME')!r}. Codex's shell tool's "
            "subprocesses (gh, git, aws, npm) need the user's real HOME."
        )

    def test_spawn_env_sets_CODEX_HOME_when_provided(self, monkeypatch):
        """CODEX_HOME isolation must still work — that's the whole point
        of the codex_home arg."""
        import subprocess
        from agent.transports import codex_app_server as cas

        captured = {}

        class FakePopen:
            def __init__(self, cmd, *args, **kwargs):
                captured["env"] = kwargs.get("env", {}).copy()
                self.stdin = None
                self.stdout = None
                self.stderr = None
                self.pid = 1
                self.returncode = None

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

            def kill(self):
                pass

        monkeypatch.setattr(subprocess, "Popen", FakePopen)
        monkeypatch.setenv("HOME", "/users/alice")

        client = cas.CodexAppServerClient(
            codex_bin="codex", codex_home="/tmp/profile/codex"
        )
        client._closed = True

        assert captured["env"].get("CODEX_HOME") == "/tmp/profile/codex"
        # And HOME still passes through unchanged
        assert captured["env"].get("HOME") == "/users/alice"

    def test_kanban_worker_adds_only_kanban_writable_root(self, monkeypatch):
        """With no pinned workspace, a Kanban worker still gets exactly the
        board DB directory as its extra writable root, and never falls back to
        danger-full-access. (Workspace git roots are covered separately when
        HERMES_KANBAN_WORKSPACE is set.)
        """
        import subprocess
        from agent.transports import codex_app_server as cas

        captured = {}

        class FakePopen:
            def __init__(self, cmd, *args, **kwargs):
                captured["cmd"] = list(cmd)
                captured["env"] = kwargs.get("env", {}).copy()
                self.stdin = None
                self.stdout = None
                self.stderr = None
                self.pid = 1
                self.returncode = None

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

            def kill(self):
                pass

        monkeypatch.setattr(subprocess, "Popen", FakePopen)
        monkeypatch.setenv("HOME", "/users/alice")
        monkeypatch.setenv("HERMES_HOME", "/users/alice/.hermes/profiles/backend-worker")
        monkeypatch.setenv("HERMES_KANBAN_TASK", "t_smoke")
        monkeypatch.setenv(
            "HERMES_KANBAN_DB",
            "/users/alice/.hermes/kanban/boards/smoke/kanban.db",
        )

        client = cas.CodexAppServerClient(codex_bin="codex")
        client._closed = True

        cmd = captured["cmd"]
        assert cmd[:2] == ["codex", "app-server"]
        assert 'sandbox_mode="workspace-write"' in cmd
        assert (
            'sandbox_workspace_write.writable_roots=["/users/alice/.hermes/kanban/boards/smoke"]'
            in cmd
        )
        assert "sandbox_workspace_write.network_access=false" in cmd
        assert all("danger" not in part for part in cmd)


class TestGitWritableRoots:
    """`_git_writable_roots` resolves the metadata dirs a commit/push needs."""

    def test_standalone_repo_grants_dotgit(self, tmp_path):
        from agent.transports.codex_app_server import _git_writable_roots

        repo = tmp_path / "repo"
        (repo / ".git").mkdir(parents=True)

        roots = _git_writable_roots(str(repo))

        assert roots == [os.path.realpath(str(repo / ".git"))]

    def test_linked_worktree_grants_gitdir_and_common_dir(self, tmp_path):
        """A worktree's index lives under .git/worktrees/<name> while objects
        live in the common dir; BOTH must be writable to commit."""
        from agent.transports.codex_app_server import _git_writable_roots

        git_dir = tmp_path / "repo" / ".git"
        worktree_gitdir = git_dir / "worktrees" / "wt"
        worktree_gitdir.mkdir(parents=True)
        # commondir is relative to the worktree gitdir: ../.. -> the repo .git
        (worktree_gitdir / "commondir").write_text("../..\n", encoding="utf-8")

        workspace = tmp_path / "ws"
        workspace.mkdir()
        (workspace / ".git").write_text(
            f"gitdir: {worktree_gitdir}\n", encoding="utf-8"
        )

        roots = _git_writable_roots(str(workspace))

        assert os.path.realpath(str(worktree_gitdir)) in roots
        assert os.path.realpath(str(git_dir)) in roots

    def test_worktree_falls_back_to_conventional_common_dir(self, tmp_path):
        """If no commondir file exists, fall back to the conventional
        .git/worktrees/<name> -> .git layout."""
        from agent.transports.codex_app_server import _git_writable_roots

        git_dir = tmp_path / "repo" / ".git"
        worktree_gitdir = git_dir / "worktrees" / "wt"
        worktree_gitdir.mkdir(parents=True)

        workspace = tmp_path / "ws"
        workspace.mkdir()
        (workspace / ".git").write_text(
            f"gitdir: {worktree_gitdir}\n", encoding="utf-8"
        )

        roots = _git_writable_roots(str(workspace))

        assert os.path.realpath(str(git_dir)) in roots

    def test_non_repo_returns_empty(self, tmp_path):
        from agent.transports.codex_app_server import _git_writable_roots

        assert _git_writable_roots(str(tmp_path / "not-a-repo")) == []


class TestKanbanWritableRoots:
    """`_kanban_writable_roots` is ordered, de-duplicated, and DB-dir-first."""

    def test_db_dir_is_first_then_workspace_git_roots(self, tmp_path):
        from agent.transports.codex_app_server import _kanban_writable_roots

        git_dir = tmp_path / "repo" / ".git"
        worktree_gitdir = git_dir / "worktrees" / "wt"
        worktree_gitdir.mkdir(parents=True)
        (worktree_gitdir / "commondir").write_text("../..\n", encoding="utf-8")
        workspace = tmp_path / "ws"
        workspace.mkdir()
        (workspace / ".git").write_text(
            f"gitdir: {worktree_gitdir}\n", encoding="utf-8"
        )

        env = {
            "HERMES_KANBAN_DB": "/k/boards/smoke/kanban.db",
            "HERMES_KANBAN_WORKSPACE": str(workspace),
        }
        roots = _kanban_writable_roots(env)

        assert roots[0] == "/k/boards/smoke"
        assert str(workspace) in roots
        assert os.path.realpath(str(worktree_gitdir)) in roots
        assert os.path.realpath(str(git_dir)) in roots
        # No duplicates, and order is preserved.
        assert len(roots) == len(set(roots))

    def test_no_workspace_yields_only_db_dir(self):
        from agent.transports.codex_app_server import _kanban_writable_roots

        roots = _kanban_writable_roots(
            {"HERMES_KANBAN_DB": "/k/boards/smoke/kanban.db"}
        )
        assert roots == ["/k/boards/smoke"]

    def test_falls_back_to_kanban_root_without_db(self):
        from agent.transports.codex_app_server import _kanban_writable_roots

        roots = _kanban_writable_roots({"HERMES_KANBAN_ROOT": "/k/root"})
        assert roots == ["/k/root"]


class TestKanbanWorkerGrantsWorktreeGitRoot:
    """End-to-end: a worker pinned to a worktree workspace gets its git
    metadata dirs in the spawned app-server writable_roots."""

    def test_worktree_git_roots_in_spawn_cmd(self, tmp_path, monkeypatch):
        import json
        import subprocess
        from agent.transports import codex_app_server as cas

        git_dir = tmp_path / "repo" / ".git"
        worktree_gitdir = git_dir / "worktrees" / "wt"
        worktree_gitdir.mkdir(parents=True)
        (worktree_gitdir / "commondir").write_text("../..\n", encoding="utf-8")
        workspace = tmp_path / "ws"
        workspace.mkdir()
        (workspace / ".git").write_text(
            f"gitdir: {worktree_gitdir}\n", encoding="utf-8"
        )

        captured = {}

        class FakePopen:
            def __init__(self, cmd, *args, **kwargs):
                captured["cmd"] = list(cmd)
                self.stdin = self.stdout = self.stderr = None
                self.pid = 1
                self.returncode = None

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

            def kill(self):
                pass

        monkeypatch.setattr(subprocess, "Popen", FakePopen)
        monkeypatch.setenv("HERMES_KANBAN_TASK", "t_smoke")
        monkeypatch.setenv("HERMES_KANBAN_DB", "/k/boards/smoke/kanban.db")
        monkeypatch.setenv("HERMES_KANBAN_WORKSPACE", str(workspace))

        client = cas.CodexAppServerClient(codex_bin="codex")
        client._closed = True

        cmd = captured["cmd"]
        roots_arg = next(
            p
            for p in cmd
            if p.startswith("sandbox_workspace_write.writable_roots=")
        )
        roots = json.loads(roots_arg.split("=", 1)[1])
        assert "/k/boards/smoke" in roots
        assert os.path.realpath(str(worktree_gitdir)) in roots
        assert os.path.realpath(str(git_dir)) in roots
        assert "sandbox_workspace_write.network_access=false" in cmd
        assert all("danger" not in part for part in cmd)
