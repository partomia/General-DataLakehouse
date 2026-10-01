"""
Auto-classification: Data Compliance profiler tag rules (Cloudera Data Catalog, compute-cluster
profilers) that apply the GDL_PII_* classifications to the tables of the rsingh_gdl_* databases,
from config/profiler_tag_rules.json.

  render     governance/profiler/: one regular-expression file per tag rule (the upload format,
             "regex,matchType,threshold") and test_data.csv for the rules' Test Tag Rule step,
             a sample of synthetic generator records with the personal columns and look-alikes
  evaluate   score every column of the tables on Impala the way the rules score them and compare
             with the classifications governance.py applies by column name; exit 1 on a
             difference config/profiler_tag_rules.json does not explain

A column's score for a rule: value_weight x the share of its non-empty sampled values that
match any of the rule's value regexes, plus (100 - value_weight) if its name matches any of the
rule's name regexes. The rule's tag is applied at the threshold or above (70, as the profiler's
system rules).

  python scripts/profiler_rules.py render
  python scripts/profiler_rules.py evaluate        # GDL_IMPALA_USER / GDL_IMPALA_PASSWORD

Creating the rules, the dry run and enabling them are Data Catalog UI steps (docs/GOVERNANCE.md);
the CDP CLI only launches and deletes profilers.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "governance" / "profiler"
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "cde" / "jobs"))
import governance as G  # noqa: E402


def load() -> dict:
    return json.loads((ROOT / "config" / "profiler_tag_rules.json").read_text())


# ---------------------------------------------------------------- scoring (pure)


def score(rule: dict, name: str, values: list) -> float:
    vals = [str(v) for v in values if v is not None and str(v).strip() != ""]
    share = (sum(1 for v in vals if any(re.search(p, v) for p in rule["values"])) / len(vals)) if vals else 0.0
    by_name = any(re.search(p, name) for p in rule["names"])
    return round(rule["value_weight"] * share + (100 - rule["value_weight"]) * by_name, 1)


def predict(cfg: dict, name: str, values: list) -> list[tuple[str, str, float]]:
    """(tag, rule name, score) of every rule at or above the threshold, best first."""
    hits = [(r["tag"], r["name"], score(r, name, values)) for r in cfg["rules"]]
    return sorted([h for h in hits if h[2] >= cfg["threshold"]], key=lambda h: -h[2])


def compare(cfg: dict, gov: dict, columns: list[dict]) -> list[dict]:
    """columns: {table, name, type, values} -> one row per column either side tags:
    verdict agree | profiler only | names only | left to governance.py | empty | conflict
    (empty: no values to profile yet; governance.py tags it by name)."""
    out = []
    for c in columns:
        want = G.column_tag(gov, c["name"], c["type"])
        hits = predict(cfg, c["name"], c["values"])
        tags = sorted({h[0] for h in hits})
        if not want and not tags:
            continue
        if c["name"].lower() in cfg["left_to_governance_py"]:
            verdict = "left to governance.py"
        elif not tags and not any(v is not None and str(v).strip() for v in c["values"]):
            verdict = "empty"
        elif len(tags) > 1:
            verdict = "conflict"
        elif tags == [want]:
            verdict = "agree"
        elif not want:
            verdict = "profiler only"
        elif not tags:
            verdict = "names only"
        else:
            verdict = "conflict"
        out.append({"column": f"{c['table']}.{c['name']}", "type": c["type"], "names": want,
                    "profiler": ",".join(tags) or None, "rule": hits[0][1] if hits else None,
                    "score": hits[0][2] if hits else None, "verdict": verdict})
    return out


# ---------------------------------------------------------------- render


def rule_file(cfg: dict, rule: dict) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["regex", "matchType", "threshold"])
    for p in rule["values"]:
        w.writerow([p, "columnValue", cfg["threshold"]])
    for p in rule["names"]:
        w.writerow([p, "columnName", cfg["threshold"]])
    return buf.getvalue()


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


TEST_COLUMNS = ("pan", "aadhaar", "mobile", "acct_no", "email", "first_name", "last_name", "full_name",
                "addr_line1", "address", "branch_name", "product_name", "debtor_account", "loan_id", "pincode")


def sample_records(n: int = 25) -> list[dict]:
    """Synthetic records from the generator's first business date: customers, accounts, borrowers,
    branches and products, joined loosely into rows of TEST_COLUMNS."""
    import gdl_common as C
    import land_sources

    d = C.parse_date(C.load_json("config/pipeline.json")["business_dates"][0])
    with tempfile.TemporaryDirectory() as tmp:
        assert land_sources.main(["--business-date", d.isoformat(), "--landing", tmp]) == 0
        root = Path(tmp)
        dump = next((root / "cbs" / d.isoformat()).glob("cbs_dump_*.sql")).read_text()
        def table(name, cols):
            return [dict(zip(cols, r[2:])) for r in C.parse_dump("x", dump, name, cols)]
        cust = table("customer", ["first_name", "last_name", "pan", "aadhaar", "mobile", "email", "addr_line1",
                                  "pincode"])
        acct = table("account", ["acct_no"])
        branch = table("branch", ["branch_name"])
        prod = table("product", ["product_name"])
        lms = next((root / "lms" / d.isoformat()).glob("lms_borrower_*.csv")).read_text()
        borr = [dict(zip(["full_name", "address"], r[2:])) for r in C.parse_psv("x", lms, ["full_name", "address"])]
        loans = next((root / "lms" / d.isoformat()).glob("lms_loan_*.csv")).read_text()
        loan = [r[2] for r in C.parse_psv("x", loans, ["loan_id"])]
        pays = [json.loads(x) for x in next((root / "payments" / d.isoformat()).glob("*.jsonl")).read_text().splitlines()]
    rows = []
    for i in range(n):
        c, b = cust[i % len(cust)], borr[i % len(borr)]
        rows.append({**c, "acct_no": acct[i % len(acct)]["acct_no"], "full_name": b["full_name"],
                     "address": b["address"], "branch_name": branch[i % len(branch)]["branch_name"],
                     "product_name": prod[i % len(prod)]["product_name"],
                     "debtor_account": pays[i % len(pays)]["debtor"]["account"]["number"],
                     "loan_id": loan[i % len(loan)]})
    return rows


def render(cfg: dict) -> list[Path]:
    OUT.mkdir(parents=True, exist_ok=True)
    written = []
    for r in cfg["rules"]:
        p = OUT / f"{slug(r['name'])}.csv"
        p.write_text(rule_file(cfg, r))
        written.append(p)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(TEST_COLUMNS), extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    w.writerows(sample_records())
    (OUT / "test_data.csv").write_text(buf.getvalue())
    return written + [OUT / "test_data.csv"]


# ---------------------------------------------------------------- evaluate on Impala


def impala_columns(engine, cfg: dict) -> list[dict]:
    out = []
    for db in cfg["databases"]:
        for (t, *_) in engine.query(f"SHOW TABLES IN {db}")[1]:
            cols = [(r[0], r[1]) for r in engine.query(f"DESCRIBE {db}.{t}")[1]
                    if r[0] and not re.match(r"(array|map|struct)<", r[1] or "")]
            if not cols:
                continue
            sel = ", ".join(f"CAST(`{c}` AS STRING)" for c, _ in cols)
            rows = engine.query(f"SELECT {sel} FROM {db}.{t} LIMIT {cfg['sample_rows']}")[1]
            for i, (c, typ) in enumerate(cols):
                out.append({"table": f"{db}.{t}", "name": c, "type": typ, "values": [r[i] for r in rows]})
    return out


def evaluate(cfg: dict) -> int:
    from run_semantic import ImpalaEngine

    gov, _, _ = G.load_config()
    engine = ImpalaEngine(json.loads((ROOT / "config" / "pipeline.json").read_text())["impala"])
    cols = impala_columns(engine, cfg)
    rows = compare(cfg, gov, cols)
    counts = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print(f"{len(cols)} columns in {len(cfg['databases'])} databases; " +
          ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    for r in sorted(rows, key=lambda r: (r["verdict"] == "agree", r["verdict"], r["column"])):
        if r["verdict"] != "agree":
            print(f"  {r['verdict']:<22} {r['column']} ({r['type']}): names {r['names']}, "
                  f"profiler {r['profiler']} ({r['rule']} {r['score']})")
    bad = [r for r in rows if r["verdict"] not in ("agree", "left to governance.py", "empty")]
    print("evaluate: " + ("OK" if not bad else f"{len(bad)} difference(s)"))
    return 1 if bad else 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=("render", "evaluate"))
    args = p.parse_args(argv)
    cfg = load()
    if args.command == "render":
        for f in render(cfg):
            print(f"wrote {f.relative_to(ROOT)}")
        return 0
    return evaluate(cfg)


if __name__ == "__main__":
    sys.exit(main())
