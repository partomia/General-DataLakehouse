import subprocess
import sys

import pytest
from conftest import ROOT

JOBS = sorted(p.stem for p in (ROOT / "cde" / "jobs").glob("*.py"))


@pytest.mark.parametrize("job", JOBS)
def test_job_imports_without_a_spark_session(job):
    """spark-submit imports the job before any session exists: no Column may be built at import time."""
    done = subprocess.run([sys.executable, "-c", f"import {job}"], cwd=ROOT / "cde" / "jobs",
                          capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
