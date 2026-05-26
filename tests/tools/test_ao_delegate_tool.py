import json

from tools.ao_bridge import AOSession
from tools.ao_delegate_tool import (
    _output_indicates_codex_complete,
    _summary_from_completed_output,
    ao_delegate_task,
)


class FakeBridge:
    def __init__(self):
        self.spawned = AOSession(
            id="oryn-workspace-1",
            project_id="OrynWorkspace",
            status="working",
            activity="active",
            branch="feat/test",
            workspace_path="/tmp/worktree",
            tmux_name="abc-oryn-workspace-1",
            open_command="tmux attach -t abc-oryn-workspace-1",
        )
        self.done = AOSession(
            id="oryn-workspace-1",
            project_id="OrynWorkspace",
            status="done",
            activity="exited",
            branch="feat/test",
            workspace_path="/tmp/worktree",
            tmux_name="abc-oryn-workspace-1",
            open_command="tmux attach -t abc-oryn-workspace-1",
        )
        self.status_calls = 0

    def spawn(self, **kwargs):
        self.spawn_kwargs = kwargs
        return self.spawned

    def status(self, session_id):
        self.status_calls += 1
        return self.done

    def capture_output(self, session, lines=40):
        return "worker finished\npytest passed"


class FakeBridgeWithCompletedTUI(FakeBridge):
    def __init__(self):
        super().__init__()
        self.done.status = "spawning"
        self.done.activity = None

    def capture_output(self, session, lines=40):
        return """
› You are an AI coding agent managed by the Agent Orchestrator (ao).

────────────────────────────────────────────────────────────────────────────────

• FOUND_PANEL
  pwd: /tmp/worktree

────────────────────────────────────────────────────────────────────────────────

› Improve documentation in @filename

  gpt-5.5 medium · /tmp/worktree
"""


class FakeBridgeWithIntermediateTUI(FakeBridge):
    def __init__(self):
        super().__init__()
        self.done.status = "spawning"
        self.done.activity = None
        self.outputs = [
            """
────────────────────────────────────────────────────────────────────────────────

• The relevant logic is small. I’m grabbing exact line numbers now.

────────────────────────────────────────────────────────────────────────────────

› Find and fix a bug in @filename
""",
            """
────────────────────────────────────────────────────────────────────────────────

• AO_PANEL_DONE
  Open and Stop are present.

────────────────────────────────────────────────────────────────────────────────

› Find and fix a bug in @filename
""",
            """
────────────────────────────────────────────────────────────────────────────────

• AO_PANEL_DONE
  Open and Stop are present.

────────────────────────────────────────────────────────────────────────────────

› Find and fix a bug in @filename
""",
        ]

    def capture_output(self, session, lines=40):
        if self.outputs:
            return self.outputs.pop(0)
        return """
────────────────────────────────────────────────────────────────────────────────

• AO_PANEL_DONE
  Open and Stop are present.

────────────────────────────────────────────────────────────────────────────────

› Find and fix a bug in @filename
"""


class ParentAgent:
    def __init__(self):
        self.events = []

    def tool_progress_callback(self, event_type, tool_name=None, preview=None, **kwargs):
        self.events.append((event_type, tool_name, preview, kwargs))


def test_ao_delegate_task_emits_subagent_events_and_returns_session():
    parent = ParentAgent()
    bridge = FakeBridge()
    result = ao_delegate_task(
        prompt="Run the test task",
        goal="Test AO worker",
        project_id="OrynWorkspace",
        branch="codex/test-branch",
        max_wait_seconds=5,
        parent_agent=parent,
        bridge=bridge,
    )

    payload = json.loads(result)
    assert payload["ok"] is True
    assert payload["runtime"] == "ao"
    assert payload["session"]["ao_session_id"] == "oryn-workspace-1"
    assert payload["session"]["workspace_path"] == "/tmp/worktree"
    assert bridge.spawn_kwargs["branch"] == "codex/test-branch"
    assert parent.events
    assert parent.events[0][3]["branch"] == "feat/test"

    event_names = [event[0] for event in parent.events]
    assert "subagent.start" in event_names
    assert "subagent.progress" in event_names
    assert "subagent.complete" in event_names

    complete = parent.events[-1]
    assert complete[3]["runtime"] == "ao"
    assert complete[3]["ao_project_id"] == "OrynWorkspace"
    assert complete[3]["branch"] == "feat/test"
    assert complete[3]["status"] == "completed"


def test_ao_delegate_task_completes_when_codex_tui_returns_to_prompt(monkeypatch):
    monkeypatch.setattr("tools.ao_delegate_tool.time.sleep", lambda _: None)
    parent = ParentAgent()
    result = ao_delegate_task(
        prompt="Run the test task",
        goal="Test AO worker",
        project_id="OrynWorkspace",
        max_wait_seconds=5,
        parent_agent=parent,
        bridge=FakeBridgeWithCompletedTUI(),
    )

    payload = json.loads(result)

    assert payload["timed_out"] is False
    assert payload["status"] == "completed"
    assert "FOUND_PANEL" in payload["summary"]
    assert "Improve documentation" not in payload["summary"]
    assert parent.events[-1][0] == "subagent.complete"
    assert parent.events[-1][3]["status"] == "completed"


def test_ao_delegate_task_waits_for_stable_completed_tui(monkeypatch):
    monkeypatch.setattr("tools.ao_delegate_tool.time.sleep", lambda _: None)
    parent = ParentAgent()
    result = ao_delegate_task(
        prompt="Run the test task",
        goal="Test AO worker",
        project_id="OrynWorkspace",
        max_wait_seconds=5,
        parent_agent=parent,
        bridge=FakeBridgeWithIntermediateTUI(),
    )

    payload = json.loads(result)

    assert payload["timed_out"] is False
    assert payload["status"] == "completed"
    assert "AO_PANEL_DONE" in payload["summary"]
    assert "grabbing exact line numbers" not in payload["summary"]


def test_codex_completion_detection_requires_final_prompt_after_separator():
    incomplete = """
› You are an AI coding agent managed by the Agent Orchestrator (ao).

• Working (10s • esc to interrupt)
"""
    active_after_separator = """
────────────────────────────────────────────────────────────────────────────────

• The active project is set. I’m reading the requested file.

• Working (12s • esc to interrupt)

› Find and fix a bug in @filename
"""
    complete = """
────────────────────────────────────────────────────────────────────────────────

• Final answer

────────────────────────────────────────────────────────────────────────────────

› Next prompt
"""

    assert _output_indicates_codex_complete(incomplete) is False
    assert _output_indicates_codex_complete(active_after_separator) is False
    assert _output_indicates_codex_complete(complete) is True
    assert _summary_from_completed_output(complete) == "Final answer"
