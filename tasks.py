# type: ignore
import os
import sys
from pathlib import Path

from invoke import task

# Allow invoke to run without active venv (e.g venv/bin/invoke),
# by prefixing tools with venv path
VENV_BIN = Path(sys.prefix) / "bin"

FILES = "src tests *.py"

os.environ["INVOKE_RUN_ECHO"] = "1"


@task
def format(c):
    """Format Python files with isort and black."""
    c.run(f"{VENV_BIN}/isort {FILES}")
    c.run(f"{VENV_BIN}/black {FILES}")


@task
def check(c):
    """Check Python files for code style and type errors."""
    c.run(f"{VENV_BIN}/isort --check {FILES}")
    c.run(f"{VENV_BIN}/black --check {FILES}")
    c.run(f"{VENV_BIN}/flake8 {FILES}")
    c.run(f"{VENV_BIN}/mypy {FILES}")
