import subprocess
from pathlib import Path


def test_black(test_folder: Path):
    completed_process = subprocess.run(
        ["poetry", "run", "black", "--check", str(test_folder.parent)],
        check=False,
    )
    assert completed_process.returncode == 0


def test_ruff(test_folder: Path):
    completed_process = subprocess.run(
        ["poetry", "run", "ruff", "check", str(test_folder.parent)],
        check=False,
    )
    assert completed_process.returncode == 0
