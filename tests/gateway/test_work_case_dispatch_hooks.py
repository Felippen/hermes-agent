import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from gateway.dev_control.work_case_hooks import (
    create_work_case_for_dispatch,
    maybe_close_work_case_for_task,
)

_RUNTIME_FIXTURE = r'''
import json
import time
from pathlib import Path


class WorkCaseRuntime:
    def __init__(self, cases_root=None):
        self.cases_root = Path(cases_root)
        self.cases_root.mkdir(parents=True, exist_ok=True)

    def case_path(self, case_id):
        return self.cases_root / case_id

    def read_json(self, path):
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def _write_json(self, path, payload):
        Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def create_case(self, *, title, summary, dispatch):
        case_id = "wc-test-1"
        root = self.case_path(case_id)
        root.mkdir(parents=True, exist_ok=True)
        self._write_json(root / "case.json", {
            "case_id": case_id,
            "title": title,
            "summary": summary,
            "status": "open",
            "dispatch": dispatch,
        })
        self._write_json(root / "carry_forward.json", {"verification_state": "unknown"})
        (root / "events.jsonl").write_text("", encoding="utf-8")
        return case_id

    def record_event(self, case_id, *, event_type, message):
        with (self.case_path(case_id) / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"type": event_type, "message": message, "created_at": time.time()}) + "\n")

    def update_carry_forward(self, case_id, updates):
        path = self.case_path(case_id) / "carry_forward.json"
        current = self.read_json(path)
        current.update(updates)
        self._write_json(path, current)

    def close_case(self, case_id, *, learnings=None, require_verified=True):
        path = self.case_path(case_id) / "case.json"
        metadata = self.read_json(path)
        metadata["status"] = "closed_verified" if require_verified else "closed_unverified"
        if learnings:
            metadata["learnings"] = learnings
        self._write_json(path, metadata)
'''


def _install_runtime_fixture(tmp: str) -> tuple[Path, Path, Path]:
    cases_root = Path(tmp) / "cases"
    vault_root = Path(tmp) / "vault"
    oryn_root = Path(tmp) / "Oryn"
    package = oryn_root / "tools" / "dev_reliability"
    package.mkdir(parents=True)
    (package / "work_case_runtime.py").write_text(_RUNTIME_FIXTURE, encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package.parent / "__init__.py").write_text("", encoding="utf-8")
    return oryn_root, cases_root, vault_root


def test_create_work_case_for_dispatch_persists_dispatch_metadata(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        oryn_root, cases_root, _vault_root = _install_runtime_fixture(tmp)
        monkeypatch.setenv("ORYN_ROOT", str(oryn_root))
        monkeypatch.setenv("ORYN_WORK_CASE_HOME", str(cases_root))
        monkeypatch.setenv("HERMES_DEV_WORK_CASE_AUTO", "1")

        case_id = create_work_case_for_dispatch(
            plan_id="devplan-1",
            task={"task_id": "task-1", "goal": "Fix CI", "prompt": "Repair adapter wiring"},
            ao_session_id="ao-1",
            runtime="ao",
            project_id="OrynWorkspace",
        )
        assert case_id
        metadata = json.loads((cases_root / case_id / "case.json").read_text(encoding="utf-8"))
        assert metadata["dispatch"]["plan_id"] == "devplan-1"
        assert metadata["dispatch"]["task_id"] == "task-1"


def test_maybe_close_work_case_for_task_closes_terminal_task(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        oryn_root, cases_root, vault_root = _install_runtime_fixture(tmp)
        monkeypatch.setenv("ORYN_ROOT", str(oryn_root))
        monkeypatch.setenv("ORYN_WORK_CASE_HOME", str(cases_root))
        monkeypatch.setenv("HERMES_VAULT_ROOT", str(vault_root))
        monkeypatch.setenv("HERMES_DEV_WORK_CASE_AUTO", "1")

        case_id = create_work_case_for_dispatch(
            plan_id="devplan-1",
            task={"task_id": "task-1", "goal": "Fix CI", "prompt": "Repair adapter wiring", "plan_id": "devplan-1"},
            ao_session_id="ao-1",
            runtime="ao",
            project_id="OrynWorkspace",
        )
        store = MagicMock()
        derived = {
            "derived_status": "completed",
            "status": "completed",
            "summary": "CI now runs adapter tiers.",
            "verification_evidence": ["python3 scripts/dev-reliability.py verify-adapters"],
        }
        maybe_close_work_case_for_task(
            task={"task_id": "task-1", "plan_id": "devplan-1", "payload": {"work_case_id": case_id}},
            derived=derived,
            store=store,
        )
        metadata = json.loads((cases_root / case_id / "case.json").read_text(encoding="utf-8"))
        assert metadata["status"] == "closed_unverified"
        assert derived["work_case_closed"] is True
        carry = json.loads((cases_root / case_id / "carry_forward.json").read_text(encoding="utf-8"))
        assert carry["verification_state"] == "unknown"
        events = [
            json.loads(line)
            for line in (cases_root / case_id / "events.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        event_types = {event["type"] for event in events}
        assert "worker_reported_evidence" in event_types
        assert "verification" not in event_types
        store.patch_task_payload.assert_called_once()
