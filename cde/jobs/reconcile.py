"""
Reconciliation: one business date, every layer built so far, against the source's own
control figures (the batch manifests), never against what Spark happened to read.

  bronze   landed_records   manifest record count = bronze accepted + quarantined
           control_total    manifest control total = sum of accepted + quarantined values
           file_trailer     CSV trailer count = data lines in the file (source-side defect)
           load_status      every manifest entity COMMITTED by a COMPLETED bronze run
  silver   silver_records   bronze accepted = silver rows, or EXPLAINED by duplicates removed
           silver_total     bronze accepted total = silver total, or EXPLAINED the same way
  gold     gold_records     silver rows = fact rows, or EXPLAINED by rows the fact excludes by design
           gold_total       silver total = fact total (INR), explained the same way
           dimension_link   every fact row found its dimension version valid on the date
           party_resolved   every fact row resolved to a golden party (not UNKNOWN)
           truth_*          fact_aml_alert against the generator's _truth file, when there is one:
                            planted patterns and screening hits alerted, nothing else alerted

Each check is MATCHED, EXPLAINED (the difference is fully accounted for, and the detail
says by what) or MISMATCH. Results replace the date's rows in ref.recon_results; the
mismatches are also printed and written as a CSV report under <reports>/recon/<date>/.
--fail-on-mismatch makes a MISMATCH fail the job (for a gate in the DAG).

Usage:
  spark-submit reconcile.py --business-date 2026-09-23 [--landing URI] [--db-prefix P]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gdl_common as C  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

RESULT_SCHEMA = ("run_id string, batch_id string, business_date date, layer string, entity string, "
                 "check_name string, expected double, actual double, difference double, status string, "
                 "detail string, checked_at timestamp_ntz")   # Impala writes it too: see gdl_common
SILVER_DAILY = {   # bronze entity -> (silver table, key, control column)
    "cbs_eod_balance": ("cbs_eod_balance", ["acct_no", "bal_date"], "ledger_balance"),
    "lms_loan": ("lms_loan_daily", ["loan_id"], "principal_outstanding"),
    "lms_repayment": ("lms_repayment", ["txn_id"], "amount"),
    "pay_transaction": ("pay_transaction", ["msg_id"], "amount"),
    "doc_document": ("doc_extract", ["file_name"], None),
}
GOLD_FACTS = {   # fact -> (silver table, silver total, fact total, dimension surrogate, rows the fact excludes)
    "fact_deposit_balance_daily": ("cbs_eod_balance", "ledger_balance_inr", "balance_inr", "account_sk", None),
    "fact_loan_position_daily": ("lms_loan_daily", "principal_outstanding", "principal_outstanding", "loan_sk", None),
    "fact_payment": ("pay_transaction", "amount_inr", "amount_inr", "account_sk", "account_key IS NULL"),
}
TOLERANCE = 0.005


def parser():
    p = C.base_parser(__doc__)
    p.add_argument("--reports", default=None, help="reports folder (default: next to --landing)")
    p.add_argument("--fail-on-mismatch", action="store_true")
    return p


class Recon:
    def __init__(self, spark, names, business_date):
        self.spark, self.names, self.d = spark, names, business_date
        self.bid = C.batch_id(business_date)
        self.rows: list[tuple] = []
        self.run_id = None

    def add(self, layer, entity, check, expected, actual, status=None, detail="", explained_by=None):
        expected = None if expected is None else float(expected)
        actual = None if actual is None else float(actual)
        diff = None if expected is None or actual is None else round(actual - expected, 2)
        if status is None:
            if diff is not None and abs(diff) <= TOLERANCE:
                status = "MATCHED"
            elif explained_by is not None and diff is not None and abs(diff - explained_by) <= TOLERANCE:
                status = "EXPLAINED"
            else:
                status = "MISMATCH"
        self.rows.append((self.run_id, self.bid, self.d, layer, entity, check, expected, actual, diff, status,
                          detail, C._now()))

    def table(self, layer, name):
        t = self.names.t(layer, name)
        return self.spark.table(t) if C.table_exists(self.spark, t) else None

    def on_date(self, df, col="_business_date"):
        return df.where(F.col(col) == F.lit(self.d.isoformat()).cast("date"))


def _sum(df, col):
    v = df.agg(F.sum(F.col(col).cast("double"))).collect()[0][0] if df is not None else None
    return round(v or 0.0, 2)


# ---------------------------------------------------------------- bronze


def latest_run(r: Recon) -> tuple[str, str, dict]:
    """Status and message of the batch's latest bronze run, and entity -> COMMITTED/SKIPPED in it."""
    audit = r.table("ref", "load_audit")
    if audit is None:
        return "NOT RUN", "", {}
    b = audit.where((F.col("batch_id") == r.bid) & (F.col("stage") == "bronze"))
    start = b.where("entity = '*' AND status = 'STARTED'").orderBy(F.desc("started_at")).limit(1).collect()
    if not start:
        return "NOT RUN", "", {}
    rows = b.where(F.col("run_id") == start[0]["run_id"]).collect()
    end = [x for x in rows if x["entity"] == "*" and x["status"] in ("COMPLETED", "FAILED")]
    status, message = (end[0]["status"], end[0]["message"] if end[0]["status"] == "FAILED" else "") if end \
        else ("RUNNING", "")
    return status, message, {x["entity"]: x["status"] for x in rows if x["status"] in ("COMMITTED", "SKIPPED")}


def bronze_checks(r: Recon, manifests: dict) -> None:
    contracts = C.contracts()
    run_status, run_message, by_entity = latest_run(r)
    r.add("bronze", "*", "batch_status", 1, 1 if run_status == "COMPLETED" else 0,
          "MATCHED" if run_status == "COMPLETED" else "MISMATCH",
          f"last bronze run {run_status}" + (f": {run_message}" if run_message else ""))
    quarantine = r.table("bronze", "quarantine")
    for entity, contract in contracts.items():
        m = manifests.get(contract["source"])
        listed = next((e for e in (m or {}).get("entities", []) if e["entity"] == entity), None)
        if listed is None:
            continue
        t = r.table("bronze", entity)
        accepted = r.on_date(t) if t is not None else None
        q = r.on_date(quarantine).where(F.col("_entity") == entity) if quarantine is not None else None
        n_acc = accepted.count() if accepted is not None else 0
        n_q = q.count() if q is not None else 0
        state = by_entity.get(entity)
        if state == "COMMITTED":
            r.add("bronze", entity, "load_status", 1, 1, "MATCHED", f"committed by the last run ({run_status})")
        elif state == "SKIPPED":
            r.add("bronze", entity, "load_status", 1, 1, "MATCHED", "committed by an earlier attempt (--resume)")
        else:
            held = (f"the table still holds {n_acc} rows for the date from an earlier run" if n_acc
                    else "no rows for the date")
            r.add("bronze", entity, "load_status", 1, 0, "MISMATCH", f"not loaded by the last run ({run_status}); {held}")
        r.add("bronze", entity, "landed_records", listed["records"], n_acc + n_q,
              detail=f"{n_acc} accepted + {n_q} quarantined vs {listed['records']} in the manifest")
        ctl = listed.get("control_total")
        if ctl is not None and contract.get("control_field"):
            field = contract["control_field"]
            col = next(c for c in contract["columns"] if c["name"] == field)
            path = col.get("path", field)
            acc_sum = _sum(accepted, field)
            q_sum = round((q.agg(F.sum(F.get_json_object("_record", f"$.{path}").cast("double"))).collect()[0][0]
                           or 0.0) if q is not None else 0.0, 2)
            unreadable = q.where(F.get_json_object("_record", f"$.{path}").cast("double").isNull()).count() \
                if q is not None else 0
            detail = f"accepted {acc_sum:,.2f} + quarantined {q_sum:,.2f} vs manifest {ctl:,.2f} ({field})"
            if unreadable:
                detail += f"; {unreadable} quarantined value(s) not numeric"
            r.add("bronze", entity, "control_total", ctl, acc_sum + q_sum, detail=detail)
    fc = r.table("ref", "file_control")
    if fc is not None:
        for row in fc.where(F.col("batch_id") == r.bid).collect():
            if row["trailer_records"] is None:
                continue
            r.add("bronze", row["entity"], "file_trailer", row["trailer_records"], row["data_records"],
                  detail=f"{row['source_file']}: trailer says {row['trailer_records']}, "
                         f"file has {row['data_records']} data lines")


# ---------------------------------------------------------------- silver


def silver_checks(r: Recon) -> None:
    for entity, (table, keys, ctl) in SILVER_DAILY.items():
        b, s = r.table("bronze", entity), r.table("silver", table)
        if b is None or s is None:
            continue
        b, s = r.on_date(b), r.on_date(s, "business_date")
        n_b, n_s = b.count(), s.count()
        if n_b == 0 and n_s == 0:
            continue
        distinct = b.dropDuplicates(keys)
        dupes = n_b - distinct.count()
        r.add("silver", table, "silver_records", n_b, n_s, explained_by=-dupes,
              detail=f"bronze {n_b} accepted, {dupes} duplicate(s) by {'+'.join(keys)}, silver {n_s}")
        if ctl:
            b_sum, s_sum = _sum(b, ctl), _sum(s, ctl)
            first = b.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy("_source_row")))
            dup_sum = _sum(first.where("_rn > 1"), ctl)
            r.add("silver", table, "silver_total", b_sum, s_sum, explained_by=-dup_sum,
                  detail=f"bronze {b_sum:,.2f}, duplicates {dup_sum:,.2f}, silver {s_sum:,.2f} ({ctl})")


# ---------------------------------------------------------------- gold


def gold_checks(r: Recon) -> None:
    for fact, (table, s_col, g_col, sk, excluded) in GOLD_FACTS.items():
        s, g = r.table("silver", table), r.table("gold", fact)
        if s is None or g is None:
            continue
        s, g = r.on_date(s, "business_date"), r.on_date(g, "business_date")
        n_s, n_g = s.count(), g.count()
        if n_g == 0:
            continue
        out = s.where(excluded) if excluded else None
        n_out = out.count() if out is not None else 0
        why = f", {n_out} excluded ({excluded})" if excluded else ""
        r.add("gold", fact, "gold_records", n_s, n_g, explained_by=-n_out if n_out else None,
              detail=f"silver {table} {n_s}{why}, gold {n_g}")
        s_sum, g_sum, out_sum = _sum(s, s_col), _sum(g, g_col), _sum(out, s_col) if out is not None else 0.0
        r.add("gold", fact, "gold_total", s_sum, g_sum, explained_by=-out_sum if out_sum else None,
              detail=f"silver {s_sum:,.2f} ({s_col}){f', excluded {out_sum:,.2f}' if excluded else ''}, "
                     f"gold {g_sum:,.2f} ({g_col})")
        unlinked = g.where(F.col(sk).isNull()).count()
        r.add("gold", fact, "dimension_link", 0, unlinked,
              detail=f"{unlinked} row(s) without a {sk} valid on {r.d.isoformat()}")
        unknown = g.where((F.col("party_id") == "UNKNOWN") | F.col("party_sk").isNull())
        n_unknown = unknown.count()
        absent = absent_owner_accounts(r) if "account_key" in g.columns else None
        if absent is None:
            r.add("gold", fact, "party_resolved", 0, n_unknown, detail=f"{n_unknown} row(s) on the UNKNOWN party")
            continue
        why = {x["reason"]: x["n"] for x in unknown.join(absent, "account_key").groupBy("reason")
               .agg(F.count("*").alias("n")).collect()}
        n_why = sum(why.values())
        r.add("gold", fact, "party_resolved", 0, n_unknown, explained_by=n_why if n_why else None,
              detail=f"{n_unknown} row(s) on the UNKNOWN party; owner customer not in silver as of the batch: "
                     f"{why.get('QUARANTINED', 0)} quarantined at bronze, {why.get('NOT_YET_RECEIVED', 0)} not yet "
                     f"received; {n_unknown - n_why} with the owner in silver but no golden party")


def aml_checks(r: Recon, landing: str) -> None:
    """The AML alerts against the generator's _truth file (synthetic data only): every planted
    payment pattern alerted by its rule once complete, every screening-list hit alerted while
    listed, and nothing else alerted on the date (the namesake with another date of birth included)."""
    g, fs, uri = r.table("gold", "fact_aml_alert"), C.HadoopFS(r.spark), f"{landing}/_truth/persons.json"
    if g is None or not fs.exists(uri):
        return
    truth = json.loads(fs.read_text(uri))
    if "aml_watchlist" not in truth:
        return
    day = r.d.isoformat()
    upto = g.where(F.col("business_date") <= F.lit(day).cast("date")).select("rule_code", "account_key").distinct()
    alerted = {(x["rule_code"], x["account_key"]) for x in upto.collect()}
    due = [p for p in truth["aml_patterns"] if p["complete_on"] <= day]
    missed = [p for p in due if (p["rule"], f"ACC:CBS:{p['acct_no']}") not in alerted]
    r.add("gold", "fact_aml_alert", "truth_planted_patterns", len(due), len(due) - len(missed),
          detail=f"{len(due)} planted pattern(s) complete by {day}, alerted by their rule: "
                 f"{len(due) - len(missed)}" + (f"; missed {', '.join(p['acct_no'] for p in missed)}" if missed else ""))

    wl = truth["aml_watchlist"]
    xref = C.read_as_of_batch(r.spark, r.names, "mdm", "party_xref", r.d).where("src_system = 'cbs'")
    party = {x["src_key"]: x["party_id"] for x in xref.select("src_key", "party_id").collect()}
    rule = {"PAN": "AML-WL-01", "NAME_DOB": "AML-WL-02"}
    today = [x.asDict() for x in r.on_date(g, "business_date")
             .select("rule_code", "account_key", "party_id", "entry_id").collect()]
    found = {(x["rule_code"], x["party_id"], x["entry_id"]) for x in today}
    hits = [h for h in wl["hits"] if h["from"] <= day]
    lost = [h for h in hits if (rule[h["basis"]], party.get(str(h["cbs_cust_id"])), h["entry_id"]) not in found]
    r.add("gold", "fact_aml_alert", "truth_watchlist_hits", len(hits), len(hits) - len(lost),
          detail=f"{len(hits)} listed customer(s) on {day}, alerted on the right basis: {len(hits) - len(lost)}"
                 + (f"; missed {', '.join(h['entry_id'] for h in lost)}" if lost else ""))

    expected = {(p["rule"], f"ACC:CBS:{p['acct_no']}") for p in truth["aml_patterns"]}
    expected_wl = {(rule[h["basis"]], party.get(str(h["cbs_cust_id"])), h["entry_id"]) for h in hits}
    other = [x for x in today if (x["rule_code"], x["account_key"]) not in expected
             and (x["rule_code"], x["party_id"], x["entry_id"]) not in expected_wl]
    trap = wl["namesake_not_a_hit"]
    r.add("gold", "fact_aml_alert", "truth_unexpected_alerts", 0, len(other),
          detail=f"{len(other)} alert(s) on {day} not planted"
                 + (": " + ", ".join(f"{x['rule_code']} {x['account_key'] or x['party_id']}" for x in other[:10])
                    if other else f" (namesake {trap['entry_id']}, another date of birth, not alerted)"))


def absent_owner_accounts(r: Recon):
    """account_key -> reason, for accounts whose owning CBS customer is not in silver as of the batch."""
    acc = r.table("silver", "cbs_account")
    if acc is None or r.table("silver", "cbs_customer") is None:
        return None
    cust = C.read_as_of_batch(r.spark, r.names, "silver", "cbs_customer", r.d).where("NOT _is_deleted")
    q = r.table("bronze", "quarantine")
    held = (q.where((F.col("_entity") == "cbs_customer") & (F.col("_business_date") <= F.lit(r.d.isoformat()).cast("date")))
            .select(F.get_json_object("_record", "$.cust_id").alias("cust_src_key")).distinct()
            .withColumn("held", F.lit(True))) if q is not None else None
    absent = acc.select("account_key", "cust_src_key").join(
        cust.select(F.col("cust_id").cast("string").alias("cust_src_key")), "cust_src_key", "left_anti")
    if held is not None:
        absent = absent.join(held, "cust_src_key", "left")
    else:
        absent = absent.withColumn("held", F.lit(None))
    return absent.select("account_key", F.when(F.col("held"), "QUARANTINED").otherwise("NOT_YET_RECEIVED")
                         .alias("reason")).distinct()


# ---------------------------------------------------------------- output


def write(r: Recon, reports: str) -> list:
    spark, target = r.spark, r.names.t("ref", "recon_results")
    C.ensure_table(spark, target, RESULT_SCHEMA, ["business_date"])
    layers = sorted({row[3] for row in r.rows})
    if layers:
        in_list = ", ".join(f"'{x}'" for x in layers)
        spark.sql(f"DELETE FROM {target} WHERE batch_id = '{r.bid}' AND layer IN ({in_list})")
        spark.createDataFrame(r.rows, RESULT_SCHEMA).writeTo(target).append()
    bad = [row for row in r.rows if row[9] == "MISMATCH"]
    out = f"{reports}/recon/{r.d.isoformat()}"
    fs = C.filesystem(spark, out)
    lines = ["layer,entity,check,expected,actual,difference,detail"]
    lines += [",".join(str(x) if x is not None else "" for x in row[3:9]) + ',"' + row[10].replace('"', "'") + '"'
              for row in bad]
    fs.write_bytes(f"{out}/mismatch_report.csv", ("\n".join(lines) + "\n").encode())
    return bad


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    landing = C.as_uri(args.landing)
    reports = C.as_uri(args.reports) if args.reports else C.reports_for(landing)
    audit = C.Audit(spark, names, "reconcile", args.business_date, args.pipeline_run)
    r = Recon(spark, names, args.business_date)
    r.run_id = audit.run_id
    audit.load("reconcile", "*", "STARTED")
    try:
        bronze_checks(r, C.read_manifests(spark, landing, args.business_date))
        silver_checks(r)
        gold_checks(r)
        aml_checks(r, landing)
        bad = write(r, reports)
        counts = {s: sum(1 for row in r.rows if row[9] == s) for s in ("MATCHED", "EXPLAINED", "MISMATCH")}
        print(f"reconcile {r.bid}: " + ", ".join(f"{k} {v}" for k, v in counts.items()), flush=True)
        for row in bad:
            print(f"  MISMATCH {row[3]}.{row[4]} {row[5]}: {row[10]}", flush=True)
        for row in r.rows:
            if row[9] == "EXPLAINED":
                print(f"  EXPLAINED {row[3]}.{row[4]} {row[5]}: {row[10]}", flush=True)
        audit.transform("reconcile", "validate", "ref.load_audit,bronze.*,silver.*,gold.*", names.t("ref", "recon_results"),
                        len(r.rows), len(bad), ", ".join(f"{k} {v}" for k, v in counts.items()))
        status = "COMPLETED"
        audit.load("reconcile", "*", status, rows_in=len(r.rows), rows_out=counts["MATCHED"] + counts["EXPLAINED"],
                   rows_rejected=counts["MISMATCH"], message=f"report {reports}/recon/{r.d.isoformat()}/")
        if bad and args.fail_on_mismatch:
            raise RuntimeError(f"{len(bad)} reconciliation mismatch(es) for {r.bid}")
    finally:
        audit.flush()
    return counts


def main() -> int:
    spark = C.get_spark("gdl-reconcile")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
