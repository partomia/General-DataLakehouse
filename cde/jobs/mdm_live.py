"""
Live MDM: a golden record from records given on the command line, with the pipeline's own code.

The records (config/mdm_live_pair.json, or --records-json) go through what build_mdm.py does
to the silver customers: silver's standardisation, match_pairs (blocking, rule, score,
decision), connected components and party ids, survivorship. The results go to their own
tables, so the pipeline's MDM tables are not touched:

  mdm.live_candidate         the records, standardised
  mdm.live_match_pair        every candidate pair with its rule, score and decision
  mdm.live_party_xref        source record -> party_id
  mdm.live_golden_party      the golden record
  mdm.live_golden_attribute  which record each golden value came from, and the rule
  ref.transform_log          one row per step (job mdm_live)

Each run replaces the live tables; Iceberg keeps the earlier runs as snapshots.

Usage:
  spark-submit mdm_live.py --business-date 2026-09-25 [--records-json '{"records": [...]}']
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_mdm as M  # noqa: E402
import build_silver as S  # noqa: E402
import gdl_common as C  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

FIELDS = ("src_system", "src_key", "full_name", "dob", "pan", "gender", "mobile", "email", "address", "city",
          "pincode", "kyc_status", "segment", "home_branch", "updated_at", "address_updated_on", "cbs_ref")
RECORD_SCHEMA = ", ".join(f"{f} string" for f in FIELDS)


def parser():
    p = C.base_parser(__doc__)
    p.add_argument("--records-json", default=None, help='{"records": [...]}; default config/mdm_live_pair.json')
    return p


def candidates(spark, records: list) -> DataFrame:
    for r in records:
        if r.get("src_system") not in M.NAME_TRUST:
            raise SystemExit(f"src_system must be one of {sorted(M.NAME_TRUST)}: {r}")
    rows = [tuple(None if r.get(f) is None else str(r[f]) for f in FIELDS) for r in records]
    df = S.person_std(spark.createDataFrame(rows, RECORD_SCHEMA), F.col("full_name"), F.col("pan"), F.col("mobile"),
                      F.col("email"), F.col("address"), F.col("pincode"))
    ts = F.to_timestamp("updated_at")
    return df.select(
        "src_system", "src_key", "name_std", "name_key", F.to_date("dob").alias("dob"), "pan_std", "mobile_e164",
        "email_std", "address_std", "pincode_std", "gender", "city",
        F.concat_ws(", ", "address", "city").alias("address_display"),
        ((F.col("src_system") == "cbs") & (F.col("kyc_status") == "VERIFIED")).alias("kyc_verified"),
        F.when(F.col("src_system") == "cbs", F.col("kyc_status")).alias("kyc_status"),
        F.when(F.col("src_system") == "cbs", F.col("segment")).alias("segment"),
        F.when(F.col("src_system") == "cbs", F.col("home_branch")).alias("home_branch"),
        F.when(F.col("src_system") == "lms", F.col("cbs_ref")).alias("cbs_ref"),
        ts.alias("record_ts"),
        F.coalesce(F.to_timestamp("address_updated_on"), ts).alias("address_ts"),
        F.lit(True).alias("is_active"),
        F.concat_ws(":", "src_system", "src_key").alias("candidate_id"))


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    d, bid = args.business_date, C.batch_id(args.business_date)
    records = json.loads(args.records_json)["records"] if args.records_json else \
        C.load_json("config/mdm_live_pair.json")["records"]
    C.ensure_databases(spark, names)
    audit = C.Audit(spark, names, "mdm_live", d, args.pipeline_run)
    t = lambda name: names.t("mdm", f"live_{name}")  # noqa: E731
    try:
        cand = candidates(spark, records).localCheckpoint()
        n = cand.count()
        C.replace_table(cand.withColumn("run_id", F.lit(audit.run_id)), t("candidate"))
        audit.transform("candidates", "standardise", "--records-json", t("candidate"), n, n, S.PERSON_STD)

        pairs = M.match_pairs(cand).localCheckpoint()
        labels = M.components(spark, cand, pairs.where("decision = 'MERGE'"))
        xref = M.assign_parties(spark, names, cand, labels, pairs, bid).localCheckpoint()
        party_of = xref.select(F.concat_ws(":", "src_system", "src_key").alias("cid"), "party_id")
        pairs_out = (pairs.join(party_of.withColumnRenamed("cid", "id_a").withColumnRenamed("party_id", "party_a"), "id_a")
                     .join(party_of.withColumnRenamed("cid", "id_b").withColumnRenamed("party_id", "party_b"), "id_b")
                     .withColumn("run_id", F.lit(audit.run_id)))
        C.replace_table(pairs_out, t("match_pair"))
        found = pairs.collect()
        audit.transform("match", "match", t("candidate"), t("match_pair"), n, len(found), "; ".join(
            f"{p['id_a']} ~ {p['id_b']}: {p['rule']} {p['decision']} score {p['score']} "
            f"(name similarity {p['name_similarity']}, same PAN {p['same_pan']}, same mobile {p['same_mobile']}, "
            f"same DOB {p['same_dob']}, same pincode {p['same_pincode']})" for p in found) or "no candidate pair")
        C.replace_table(xref.withColumn("run_id", F.lit(audit.run_id)), t("party_xref"))
        n_party = xref.select("party_id").distinct().count()
        audit.transform("cluster", "deduplicate", t("match_pair"), t("party_xref"), n, n_party,
                        "connected components over MERGE pairs")

        golden, golden_attr = M.survive(M.attribute_rows(cand, xref, None))
        members = xref.groupBy("party_id").agg(F.array_sort(F.collect_set("src_system")).alias("source_systems"),
                                               F.count(F.lit(1)).alias("member_records"))
        golden = (golden.join(members, "party_id").withColumn("dob", F.col("dob").cast("date"))
                  .withColumn("run_id", F.lit(audit.run_id)).withColumn("batch_id", F.lit(bid)))
        C.replace_table(golden, t("golden_party"))
        C.replace_table(golden_attr.withColumn("run_id", F.lit(audit.run_id)), t("golden_attribute"))
        chosen = golden_attr.orderBy("party_id", "attribute").collect()
        audit.transform("survive", "survive", t("party_xref"), t("golden_party"), n, golden.count(), "; ".join(
            f"{a['party_id']}.{a['attribute']} = {a['src_system']}:{a['src_key']} "
            f"({a['distinct_values']} candidate value(s); {a['rule']})" for a in chosen))
    finally:
        audit.flush()

    print(f"run_id {audit.run_id}: {n} records -> {n_party} part{'y' if n_party == 1 else 'ies'}", flush=True)
    for p in found:
        print(f"  {p['id_a']} ~ {p['id_b']}: {p['rule']} -> {p['decision']} (score {p['score']}, "
              f"name similarity {p['name_similarity']})", flush=True)
    for a in chosen:
        print(f"  {a['party_id']} {a['attribute']:<12} {a['value']!s:<45} from {a['src_system']}:{a['src_key']}",
              flush=True)
    return {"run_id": audit.run_id, "records": n, "parties": n_party, "pairs": [p["rule"] for p in found]}


def main() -> int:
    spark = C.get_spark("gdl-mdm-live")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
