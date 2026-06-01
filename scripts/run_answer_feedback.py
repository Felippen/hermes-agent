#!/usr/bin/env python3
"""Inspect, judge, and export Oryn answer-feedback records."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from gateway.dev_control.answer_feedback import DevAnswerFeedbackStore  # noqa: E402
from gateway.dev_control.laminar_exporter import export_answer_judge_event  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, help="Hermes state.db path.")
    sub = parser.add_subparsers(dest="command", required=True)

    list_cmd = sub.add_parser("list", help="List answer feedback.")
    list_cmd.add_argument("--profile")
    list_cmd.add_argument("--rating", default="down")
    list_cmd.add_argument("--reason")
    list_cmd.add_argument("--limit", type=int, default=25)

    judge_cmd = sub.add_parser("judge", help="Judge one feedback event.")
    judge_cmd.add_argument("event_id")

    export_cmd = sub.add_parser("export-ovyon-fixture", help="Export one negative Ovyon feedback event as a benchmark fixture.")
    export_cmd.add_argument("event_id")
    export_cmd.add_argument("--output-dir", type=Path, default=Path("tests/ovyon_situation_awareness/fixtures/answer_feedback"))

    args = parser.parse_args()
    store = DevAnswerFeedbackStore(args.db)
    try:
        if args.command == "list":
            result = {
                "ok": True,
                "data": store.list_events(
                    profile=args.profile,
                    rating=args.rating,
                    reason=args.reason,
                    limit=args.limit,
                ),
            }
        elif args.command == "judge":
            result = store.judge_event(args.event_id, export=export_answer_judge_event)
        elif args.command == "export-ovyon-fixture":
            result = store.export_ovyon_fixture(args.event_id, args.output_dir)
        else:
            parser.error(f"unknown command: {args.command}")
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
