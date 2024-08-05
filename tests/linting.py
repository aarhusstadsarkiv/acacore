import subprocess
from pathlib import Path

from acacore import __file__ as acacore_init_file


def test_ruff(test_folder: Path):
    completed_process = subprocess.run(
        [
            "poetry",
            "run",
            "ruff",
            "check",
            "--config",
            str(test_folder.parent / "pyproject.toml"),
            str(Path(acacore_init_file).parent),
        ],
        check=False,
    )
    assert completed_process.returncode == 0
