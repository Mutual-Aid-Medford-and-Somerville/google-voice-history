# type: ignore
"""Tests of running `python google_voice_history.py`."""
import pathlib
import re
import subprocess

TESTS_PATH = pathlib.Path(__file__).parent.resolve()
SRC_PATH = (TESTS_PATH / ".." / "src").resolve()


def run_script(*args):
    """Run the script and return the result with captured output."""
    return subprocess.run(
        ["python", "google_voice_history.py", *[str(x) for x in args]],
        cwd=SRC_PATH,
        capture_output=True,
        text=True,
    )


def test_required_path():
    """Show usage when missing file path."""
    process = run_script()

    assert process.returncode != 0
    assert "PATH" in process.stderr


def test_help_message():
    """Show a help message."""
    process = run_script("-h")

    assert process.returncode == 0
    assert re.match(
        r"^usage.+^Generate.+^  PATH.+^CSV columns",
        process.stdout,
        re.DOTALL | re.MULTILINE,
    )


def test_example():
    """Generate example CSV from example takeout."""
    process = run_script(TESTS_PATH / "takeout.zip")

    with open(TESTS_PATH / "history.csv") as f:
        example_csv = f.read()

    assert process.returncode == 0
    assert process.stdout == example_csv
