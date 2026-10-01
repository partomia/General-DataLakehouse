import pytest
from conftest import load_job

bronze = load_job("ingest_bronze")


def args(*argv):
    return bronze.apply_mode(bronze.C.parse(bronze.parser(), ["--business-date", "2026-09-23", *argv]))


def test_modes_map_to_the_drill_flags():
    a = args("--mode", "normal")
    assert (a.resume, a.fail_during, a.fail_after) == (False, None, None)
    assert args("--mode", "resume").resume is True
    assert args("--mode", "fail-during:lms_loan").fail_during == "lms_loan"
    assert args("--mode", "fail-after:cbs_account").fail_after == "cbs_account"
    assert args("--resume").resume is True


def test_unknown_mode_is_refused():
    with pytest.raises(SystemExit):
        args("--mode", "fail-during")
