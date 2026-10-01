import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cde" / "jobs"))


def load_job(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "cde" / "jobs" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def landed(tmp_path_factory):
    """Five business days of all sources, generated once for the whole test session."""
    land = load_job("land_sources")
    root = tmp_path_factory.mktemp("landing")
    assert land.main(["--all-dates", "--landing", str(root)]) == 0
    return root
