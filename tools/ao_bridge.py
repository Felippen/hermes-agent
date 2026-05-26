"""Small Python wrapper around the local Agent Orchestrator Node bridge."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_AO_CONFIG_PATH = "/Users/felipelamartine/projects/Oryn/agent-orchestrator.yaml"
DEFAULT_AO_HOME = "/Users/felipelamartine"
DEFAULT_CODEX_BIN = "/opt/homebrew/bin/codex"
TERMINAL_STATUSES = {"done", "merged", "killed", "errored", "terminated"}
FAILED_STATUSES = {"killed", "errored", "terminated"}


@dataclass
class AOSession:
    id: str
    project_id: Optional[str] = None
    status: Optional[str] = None
    activity: Optional[str] = None
    branch: Optional[str] = None
    issue_id: Optional[str] = None
    workspace_path: Optional[str] = None
    tmux_name: Optional[str] = None
    agent: Optional[str] = None
    model: Optional[str] = None
    pr: Any = None
    summary: Optional[str] = None
    open_command: Optional[str] = None

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "AOSession":
        return cls(
            id=str(payload.get("id") or ""),
            project_id=payload.get("project_id"),
            status=payload.get("status"),
            activity=payload.get("activity"),
            branch=payload.get("branch"),
            issue_id=payload.get("issue_id"),
            workspace_path=payload.get("workspace_path"),
            tmux_name=payload.get("tmux_name"),
            agent=payload.get("agent"),
            model=payload.get("model"),
            pr=payload.get("pr"),
            summary=payload.get("summary"),
            open_command=payload.get("open_command"),
        )

    @property
    def is_terminal(self) -> bool:
        return (self.status or "").lower() in TERMINAL_STATUSES

    @property
    def display_status(self) -> str:
        status = (self.status or "").lower()
        if status in FAILED_STATUSES:
            return "failed"
        if status in {"done", "merged"}:
            return "completed"
        return "running"

    def event_fields(self) -> Dict[str, Any]:
        return {
            "runtime": "ao",
            "ao_session_id": self.id,
            "ao_project_id": self.project_id,
            "workspace_path": self.workspace_path,
            "branch": self.branch,
            "issue_id": self.issue_id,
            "tmux_name": self.tmux_name,
            "open_command": self.open_command,
            "model": self.model,
            "status": self.display_status,
        }


class AOBridgeError(RuntimeError):
    pass


class AOBridge:
    def __init__(
        self,
        config_path: str = DEFAULT_AO_CONFIG_PATH,
        home: str = DEFAULT_AO_HOME,
        node_bin: str = "node",
        bridge_script: Optional[Path] = None,
        codex_shim_dir: Optional[Path] = None,
        codex_real_bin: Optional[str] = None,
    ):
        self.config_path = config_path
        self.home = home
        self.node_bin = node_bin
        self.bridge_script = bridge_script or Path(__file__).with_name("ao_bridge.mjs")
        self.codex_shim_dir = codex_shim_dir or Path(__file__).with_name("ao_shims")
        self.codex_shim_path = self.codex_shim_dir / "codex"
        self.user_bin_dir = Path(self.home) / "bin"
        self.codex_real_bin = self._resolve_codex_real_bin(codex_real_bin)

    def spawn(
        self,
        *,
        project_id: str,
        prompt: str,
        issue_id: Optional[str] = None,
        branch: Optional[str] = None,
        agent: Optional[str] = None,
    ) -> AOSession:
        payload = self._call(
            "spawn",
            {
                "project_id": project_id,
                "prompt": prompt,
                "issue_id": issue_id,
                "branch": branch,
                "agent": agent,
            },
            timeout=180,
        )
        return AOSession.from_payload(payload["session"])

    def status(self, session_id: str) -> Optional[AOSession]:
        payload = self._call("status", {"session_id": session_id}, timeout=30)
        session = payload.get("session")
        return AOSession.from_payload(session) if session else None

    def kill(self, session_id: str) -> None:
        self._call("kill", {"session_id": session_id}, timeout=60)

    def capture_output(self, session: AOSession, lines: int = 40) -> str:
        if not session.tmux_name:
            return ""
        try:
            proc = subprocess.run(
                ["tmux", "capture-pane", "-p", "-t", session.tmux_name, "-S", f"-{int(lines)}"],
                text=True,
                capture_output=True,
                timeout=10,
                check=False,
            )
        except Exception:
            return ""
        if proc.returncode != 0:
            return ""
        return proc.stdout.strip()

    def open_session(self, session_id: str) -> Dict[str, Any]:
        session = self.status(session_id)
        if not session:
            raise AOBridgeError(f"AO session not found: {session_id}")
        opened = False
        if session.workspace_path:
            try:
                subprocess.Popen(["open", session.workspace_path])
                opened = True
            except Exception:
                opened = False
        return {
            "ok": True,
            "opened": opened,
            "session": session.event_fields(),
        }

    def _call(self, command: str, payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
        request = {
            "config_path": self.config_path,
            **{k: v for k, v in payload.items() if v is not None},
        }
        env = self._bridge_env()
        if command == "spawn":
            self._ensure_codex_shim_on_user_path()
            self._prepare_tmux_environment(env)
        proc = subprocess.run(
            [self.node_bin, str(self.bridge_script), command],
            input=json.dumps(request),
            text=True,
            capture_output=True,
            timeout=timeout,
            env=env,
            check=False,
        )
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        try:
            data = json.loads(self._last_json_line(stdout or stderr))
        except json.JSONDecodeError as exc:
            raise AOBridgeError(f"AO bridge returned invalid JSON: {stdout or stderr}") from exc
        if proc.returncode != 0 or data.get("ok") is False:
            raise AOBridgeError(str(data.get("error") or stderr or "AO bridge failed"))
        return data

    def _bridge_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env["HOME"] = self.home
        env["AO_CONFIG_PATH"] = self.config_path
        env["CODEX_REAL_BIN"] = self.codex_real_bin
        shim_path = str(self.codex_shim_dir)
        current_path = env.get("PATH", "")
        if shim_path not in current_path.split(os.pathsep):
            env["PATH"] = f"{shim_path}{os.pathsep}{current_path}" if current_path else shim_path
        return env

    def _resolve_codex_real_bin(self, explicit: Optional[str]) -> str:
        candidate = explicit or os.environ.get("CODEX_REAL_BIN") or shutil.which("codex")
        if candidate:
            try:
                resolved_candidate = Path(candidate).resolve()
                resolved_shim = self.codex_shim_path.resolve()
                user_shim = (self.user_bin_dir / "codex").resolve()
                if resolved_candidate not in {resolved_shim, user_shim}:
                    return str(candidate)
            except Exception:
                return str(candidate)
        return DEFAULT_CODEX_BIN

    def _prepare_tmux_environment(self, env: Dict[str, str]) -> None:
        """Make AO-created tmux sessions resolve the Codex compatibility shim."""
        for key in ("PATH", "CODEX_REAL_BIN"):
            value = env.get(key)
            if not value:
                continue
            try:
                subprocess.run(
                    ["tmux", "set-environment", "-g", key, value],
                    text=True,
                    capture_output=True,
                    timeout=5,
                    check=False,
                )
            except Exception:
                pass

    def _ensure_codex_shim_on_user_path(self) -> None:
        """Install a non-destructive ~/bin/codex shim for AO tmux shells."""
        target = self.user_bin_dir / "codex"
        try:
            if target.exists() or target.is_symlink():
                return
            self.user_bin_dir.mkdir(parents=True, exist_ok=True)
            target.symlink_to(self.codex_shim_path)
        except Exception:
            pass

    @staticmethod
    def _last_json_line(output: str) -> str:
        for line in reversed((output or "").splitlines()):
            candidate = line.strip()
            if candidate.startswith("{") and candidate.endswith("}"):
                return candidate
        return output or "{}"
