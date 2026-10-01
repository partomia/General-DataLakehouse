import re
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


@pytest.mark.parametrize("job", [j for j in JOBS if j != "gdl_common"])
def test_tables_are_created_through_the_iceberg_helper(job):
    """CDE's spark_catalog makes a Hive table of a bare writeTo().create*(), which cannot be replaced."""
    text = (ROOT / "cde" / "jobs" / f"{job}.py").read_text()
    bare = [m.group(0) for m in re.finditer(r"writeTo\([^)]*\)\s*\.\s*create\w*\(", text)]
    assert not bare, f"create tables through the gdl_common helpers: {bare}"
