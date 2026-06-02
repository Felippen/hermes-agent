"""Dogfood smoke coverage for the lab single-Python-file PR path."""

from pathlib import Path


def test_lab_dogfood_smoke_metadata_is_stable():
    metadata = {
        "surface": "gateway",
        "workflow": "single-python-pr-publish",
        "date": "2026-05-30",
        "target": Path(__file__).name,
    }

    assert metadata == {
        "surface": "gateway",
        "workflow": "single-python-pr-publish",
        "date": "2026-05-30",
        "target": "test_lab_dogfood_single_python_pr_publish_20260530.py",
    }


def test_lab_dogfood_smoke_lives_in_gateway_tests():
    path = Path(__file__)

    assert path.parent.name == "gateway"
    assert path.name.startswith("test_lab_dogfood_")
