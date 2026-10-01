import hashlib
import json

import gdl_common as C

CFG = C.load_json("config/pipeline.json")


def manifest(root, source, d):
    p = root / source / d / "_manifest.json"
    return json.loads(p.read_text()) if p.exists() else None


def test_every_day_has_manifests_matching_the_files(landed):
    for d in CFG["business_dates"]:
        for source in C.SOURCES:
            m = manifest(landed, source, d)
            assert m is not None, (source, d)
            assert m["batch_id"] == C.batch_id(C.parse_date(d))
            for f in m["files"]:
                data = (landed / source / d / f["file"]).read_bytes()
                assert len(data) == f["bytes"] and hashlib.sha256(data).hexdigest() == f["sha256"], f["file"]


def test_day_one_is_a_full_dump_and_later_days_are_cdc(landed):
    first, later = CFG["business_dates"][0], CFG["business_dates"][1:]
    assert list((landed / "cbs" / first).glob("cbs_dump_*.sql"))
    for d in later:
        assert not list((landed / "cbs" / d).glob("cbs_dump_*.sql")), d
        assert list((landed / "cbs" / d).glob("cbs_cdc_*.jsonl")), d


def test_generator_is_deterministic(landed, tmp_path):
    from conftest import load_job

    d = CFG["business_dates"][2]
    load_job("land_sources").main(["--business-date", d, "--landing", str(tmp_path)])
    for source in ("cbs", "lms", "payments", "compliance"):
        a, b = manifest(landed, source, d), manifest(tmp_path, source, d)
        assert [f["sha256"] for f in a["files"]] == [f["sha256"] for f in b["files"]], source


def test_planted_faults_are_present(landed):
    dates = CFG["business_dates"]
    rep = landed / "lms" / dates[2] / f"lms_repayment_{dates[2].replace('-', '')}.csv"
    lines = [x for x in rep.read_text().splitlines() if x]
    assert int(lines[-1].split("|")[1]) == len(lines) - 2 + 1, "D3 repayment trailer should overstate by one"

    pays = [json.loads(x) for x in (landed / "payments" / dates[3]).glob("*.jsonl").__next__().read_text().splitlines()]
    assert any("device" in p for p in pays), "the device field should appear from D4"
    assert not any("device" in json.loads(x) for x in
                   next((landed / "payments" / dates[2]).glob("*.jsonl")).read_text().splitlines())

    loan = next((landed / "lms" / dates[1]).glob("lms_loan_*.csv")).read_text()
    assert "|abc|" in loan, "D2 should carry a non-numeric dpd"


def test_truth_lists_every_person(landed):
    truth = json.loads((landed / "_truth" / "persons.json").read_text())
    assert len(truth["persons"]) >= CFG["customers"]


def test_aml_patterns_and_screening_hits_are_planted(landed):
    truth = json.loads((landed / "_truth" / "persons.json").read_text())
    dates = CFG["business_dates"]
    structuring = {p["acct_no"] for p in truth["aml_patterns"] if p["rule"] == "AML-STR-01"}
    deposits = {}
    for d in dates[1:4]:
        for line in next((landed / "payments" / d).glob("*.jsonl")).read_text().splitlines():
            p = json.loads(line)
            if p["channel"] == "CASH_DEPOSIT" and 40000 <= float(p["amount"].get("value") or 0) < 50000:
                acct = p["creditor"]["account"]["number"]
                deposits[acct] = deposits.get(acct, 0) + 1
    assert all(deposits.get(a, 0) >= 3 for a in structuring), "structuring is on the same accounts every date"

    wl = truth["aml_watchlist"]
    first = next((landed / "compliance" / dates[0]).glob("*.csv")).read_text()
    fourth = next((landed / "compliance" / dates[3]).glob("*.csv")).read_text()
    late = [h["entry_id"] for h in wl["hits"] if h["from"] == dates[3]]
    assert late and all(e not in first and e in fourth for e in late), "a listing arrives on the fourth date"
    assert wl["namesake_not_a_hit"]["entry_id"] in first
