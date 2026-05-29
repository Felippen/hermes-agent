#!/usr/bin/env python3.11
"""Run bounded Hermes Lab observe-loop passes."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway.dev_control.dogfood_backlog import discover_todo_candidates  # noqa: E402
from gateway.dev_control.lab_environment import lab_paths_from_env  # noqa: E402
from gateway.dev_control.lab_loop import (  # noqa: E402
    DevLabLoopStore,
    enqueue_approved_proposals,
    enqueue_candidates,
    loop_health,
    run_lab_loop,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Hermes Lab observe-loop dogfood passes.")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--stable-db-path", default=str(Path("~/.hermes/profiles/dev/state.db").expanduser()))
    parser.add_argument("--max-passes", type=int, default=1)
    parser.add_argument("--sources", default=os.getenv("HERMES_DEV_SIGNAL_DIGEST_SOURCES", "deterministic,product,reliability"))
    parser.add_argument("--enqueue-todos", action="store_true", help="Discover TODO/FIXME dogfood tasks before running.")
    parser.add_argument("--skip-approved-proposals", action="store_true", help="Do not append approved proposals to the dogfood backlog before running.")
    parser.add_argument("--todo-root", action="append", default=[], help="Repo root to scan for TODO/FIXME tasks.")
    parser.add_argument("--auto-approve", action="store_true", help="Auto-approve discovered low-risk allowed candidates.")
    parser.add_argument("--resume", action="store_true", help="Clear a halted loop before running.")
    parser.add_argument("--health", action="store_true", help="Print loop health and exit.")
    parser.add_argument(
        "--ao-config-path",
        default=os.getenv("ORYN_LAB_AO_CONFIG_PATH"),
        help="Lab Agent Orchestrator config path. Defaults to a colocated lab Oryn checkout when present.",
    )
    parser.add_argument("--max-consecutive-failures", type=int, default=int(os.getenv("HERMES_DEV_LAB_MAX_CONSECUTIVE_FAILURES", "2")))
    parser.add_argument("--max-consecutive-out-of-scope", type=int, default=int(os.getenv("HERMES_DEV_LAB_MAX_CONSECUTIVE_OUT_OF_SCOPE", "3")))
    parser.add_argument("--max-seconds", type=float, default=float(os.getenv("HERMES_DEV_LAB_PASS_MAX_SECONDS", "1800")))
    parser.add_argument("--max-cost-usd", type=float, default=float(os.getenv("HERMES_DEV_LAB_PASS_MAX_COST_USD", "0") or 0))
    parser.add_argument("--regression-threshold", type=float, default=float(os.getenv("HERMES_DEV_LAB_REGRESSION_THRESHOLD", "0.20")))
    parser.add_argument("--isolation-pid", action="append", default=[], help="Additional lab process pid to include in the open-file isolation audit.")
    parser.add_argument("--extra-isolation-pids", default="", help="Comma-separated lab process pids to include in the open-file isolation audit.")
    args = parser.parse_args()

    paths = lab_paths_from_env()
    db_path = Path(args.db_path or paths["db_path"]).expanduser()
    store = DevLabLoopStore(db_path)
    if args.resume:
        store.resume()
    if args.health:
        print(json.dumps(loop_health(db_path=db_path), ensure_ascii=False, sort_keys=True))
        return 0
    if args.enqueue_todos:
        roots = args.todo_root or [Path(paths["repos_dir"]) / "hermes-agent", Path(paths["repos_dir"]) / "Oryn"]
        candidates = discover_todo_candidates(repo_roots=roots, limit=20)
        enqueue_candidates(store, candidates, auto_approve=args.auto_approve)
    if not args.skip_approved_proposals:
        enqueue_approved_proposals(db_path=db_path, store=store)
    sources = [source.strip() for source in str(args.sources or "").split(",") if source.strip()]
    bridge = _lab_ao_bridge(args.ao_config_path, paths)
    isolation_pids = [
        *args.isolation_pid,
        *[pid.strip() for pid in str(args.extra_isolation_pids or "").split(",") if pid.strip()],
    ]
    result = run_lab_loop(
        db_path=db_path,
        stable_db_path=Path(args.stable_db_path).expanduser(),
        max_passes=args.max_passes,
        bridge=bridge,
        sources=sources,
        max_consecutive_failures=args.max_consecutive_failures,
        max_consecutive_out_of_scope=args.max_consecutive_out_of_scope,
        max_seconds=args.max_seconds,
        max_cost_usd=args.max_cost_usd if args.max_cost_usd > 0 else None,
        regression_threshold=args.regression_threshold,
        isolation_pids=isolation_pids,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok") else 1


def _lab_ao_bridge(config_path: str | None, paths: dict[str, str]):
    from gateway.dev_worker_runtimes import WorkerRuntimeRouter
    from tools.ao_bridge import AOBridge

    resolved = Path(config_path).expanduser() if config_path else _default_lab_ao_config(paths)
    if not resolved or not resolved.exists():
        raise RuntimeError(
            "Lab AO config is required for live lab loop execution. "
            "Pass --ao-config-path or set ORYN_LAB_AO_CONFIG_PATH."
        )
    return WorkerRuntimeRouter(
        ao_bridge=AOBridge(config_path=str(resolved), home=str(Path(paths["lab_home"]).expanduser()))
    )


def _default_lab_ao_config(paths: dict[str, str]) -> Path | None:
    candidates = [
        Path(paths["lab_home"]).expanduser() / "agent-orchestrator.lab.yaml",
        Path(paths["repos_dir"]).expanduser() / "Oryn" / "agent-orchestrator.lab.yaml",
        Path(__file__).resolve().parents[2] / "agent-orchestrator.lab.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


if __name__ == "__main__":
    raise SystemExit(main())
