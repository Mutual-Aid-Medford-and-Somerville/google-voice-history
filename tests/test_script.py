# type: ignore
"""Tests for google_voice_history.py."""
import pathlib
import subprocess

TESTS_PATH = pathlib.Path(__file__).parent.resolve()
SRC_PATH = (TESTS_PATH / ".." / "src").resolve()


def run_script(*args):
    """Run the script and return the result with captured output."""
    return subprocess.run(
        ["python", "-m", "google_voice_history", *args],
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
    assert "Google Voice Takeout" in process.stdout
    assert "PATH" in process.stdout


def test_example():
    """Generate example CSV from example takeout."""
    process = run_script(TESTS_PATH / "takeout.zip")

    with open(TESTS_PATH / "takeout.csv") as f:
        example_csv = f.read()

    assert process.returncode == 0
    assert process.stdout == example_csv
