# type: ignore
"""Tests of google_voice_history."""
import pathlib
import sys

import pytest

import google_voice_history

TESTS_PATH = pathlib.Path(__file__).parent.resolve()


@pytest.fixture
def patch_argv(monkeypatch):
    """Set command line arguments."""

    def patch(*args):
        name = google_voice_history.__file__
        args = [str(x) for x in args]
        monkeypatch.setattr(sys, "argv", [name, *args])

    return patch


def test_example(patch_argv, capsys):
    """Generate example CSV from example takeout."""
    patch_argv(TESTS_PATH / "takeout.zip")

    google_voice_history.main()

    with open(TESTS_PATH / "history.csv") as f:
        example_csv = f.read()

    captured = capsys.readouterr()
    assert captured.out == example_csv
