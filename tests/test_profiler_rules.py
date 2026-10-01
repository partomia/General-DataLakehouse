import json
import re
import sys

import gdl_common as C
from conftest import ROOT

sys.path.insert(0, str(ROOT / "scripts"))
import governance as G  # noqa: E402
import profiler_rules as P  # noqa: E402

CFG = P.load()
GOV, _, _ = G.load_config()
D1 = C.load_json("config/pipeline.json")["business_dates"][0]


def contract_cols(name):
    return [c["name"] for c in json.loads((ROOT / "contracts" / f"{name}.json").read_text())["columns"]]


def landing_columns(landed):
    """Bronze-shaped columns (contract names, string values) of the first business date's files."""
    out = []

    def add(table, cols, rows):
        for i, c in enumerate(cols):
            out.append({"table": table, "name": c, "type": "string", "values": [r[i] for r in rows]})

    dump = next((landed / "cbs" / D1).glob("cbs_dump_*.sql")).read_text()
    for t in ("customer", "account", "branch", "product"):
        cols = contract_cols(f"cbs_{t}")
        add(f"cbs_{t}", cols, [r[2:] for r in C.parse_dump("x", dump, t, cols)])
    for src, ent in (("lms", "lms_borrower"), ("lms", "lms_loan"), ("compliance", "aml_watchlist")):
        cols = contract_cols(ent)
        text = next((landed / src / D1).glob(f"{ent}_*.csv")).read_text()
        add(ent, cols, [r[2:] for r in C.parse_psv("x", text, cols)])
    pays = [json.loads(x) for x in next((landed / "payments" / D1).glob("*.jsonl")).read_text().splitlines()]
    add("pay_transaction", ["msg_id", "debtor_name", "debtor_account", "creditor_account"],
        [(p["msg_id"], p["debtor"]["name"], p["debtor"]["account"]["number"], p["creditor"]["account"]["number"])
         for p in pays])
    return out


def test_rules_tag_only_own_classifications_and_compile():
    assert CFG["rules"] and len({r["name"] for r in CFG["rules"]}) == len(CFG["rules"])
    for r in CFG["rules"]:
        assert r["tag"] in GOV["classifications"], r["name"]
        assert 0 <= r["value_weight"] <= 100 and r["values"] and r["names"]
        for p in r["values"] + r["names"]:
            re.compile(p)


def test_rendered_rule_files_are_up_to_date():
    for r in CFG["rules"]:
        f = P.OUT / f"{P.slug(r['name'])}.csv"
        assert f.read_text() == P.rule_file(CFG, r), f"{f.name}: run scripts/profiler_rules.py render"


def test_scoring_weights_name_and_values():
    pan = next(r for r in CFG["rules"] if r["name"] == "GDL PAN")
    assert P.score(pan, "pan", ["ABCDE1234F", "abcde1234f"]) == 100
    assert P.score(pan, "tax_id", ["ABCDE1234F", None, ""]) == 85
    assert P.score(pan, "pan", ["n/a"]) == 15
    acct = next(r for r in CFG["rules"] if r["name"] == "GDL Account number")
    assert not P.predict(CFG, "debtor_account", ["00100001000001"] * 3)
    assert P.predict(CFG, "acct_no", ["00100001000001"])[0][0] == "GDL_PII_LAST_4"
    assert acct["value_weight"] < CFG["threshold"]


def test_rules_agree_with_the_name_rules_on_the_landing_files(landed):
    rows = P.compare(CFG, GOV, landing_columns(landed))
    tagged = {r["column"] for r in rows if r["verdict"] == "agree"}
    assert {"cbs_customer.pan", "cbs_customer.aadhaar", "cbs_customer.email", "cbs_customer.addr_line1",
            "lms_borrower.mobile", "cbs_account.acct_no", "aml_watchlist.full_name"} <= tagged
    bad = [r for r in rows if r["verdict"] not in ("agree", "left to governance.py", "empty")]
    assert not bad, bad


def test_profiler_mode_keeps_profiler_tags_on_tables_only():
    gov = {**GOV, "profiler_tags_on_tables": True}
    cols = [{"guid": "1", "qualifiedName": "d.t.notes@cm", "name": "notes", "type": "string",
             "tags": ["GDL_PII_REDACT"], "entity": "iceberg_column"},
            {"guid": "2", "qualifiedName": "d.v.notes@cm", "name": "notes", "type": "string",
             "tags": ["GDL_PII_REDACT"], "entity": "hive_column"}]
    assert G.classification_changes(gov, cols) == ([], [("2", "d.v.notes@cm", "GDL_PII_REDACT")])
    assert len(G.classification_changes(GOV, cols)[1]) == 2
