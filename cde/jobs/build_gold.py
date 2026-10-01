"""
Stage 4 - Gold: the banking data model (Customer, Deposits, Lending, Payments).

For one business date, from silver and MDM as the batch committed them:

  conformed dimensions (shared by every domain)
    dim_date        calendar with the Indian fiscal year (April to March)
    dim_branch      branches, region, IFSC
    dim_product     deposit and lending products, domain, deposit type
    dim_currency    currencies and the INR rate used
    dim_party       SCD2 over the MDM golden record (party_id is the canonical customer key)
  domain dimensions (SCD2)
    dim_account     deposit accounts: owner party, product, branch, status, rate
    dim_loan        loans: borrower party, product, branch, rate, restructuring, status
  bridge_party_account   party -> account / loan with a role (PRIMARY, JOINT, BORROWER)
  facts (the date's partition replaced)
    fact_deposit_balance_daily   end-of-day balance in INR, deposit type (SAVINGS / CURRENT / TERM)
    fact_loan_position_daily     outstanding, overdue, DPD, IRAC asset class computed from DPD
                                 (the source's own class is kept beside it), provision
    fact_payment                 own-account payments: channel, direction, INR amount, fees
  AML, the extension domain (config/aml_rules.json); joins only through party_id / account_key
    dim_aml_rule                 the alert rules and their parameters
    fact_aml_alert               cash structuring and pass-through on fact_payment, screening-list
                                 matches (PAN, or name + date of birth) of dim_party against
                                 silver.aml_watchlist; is_new when not alerted on an earlier date
  ref.kpi_definition / ref.kpi_parameter   from config/kpi.json

SCD2 rows carry version, effective_from, effective_to (9999-12-31 while open), is_current,
record_hash (of the tracked attributes), change_reason, changed_attributes, end_reason, and
the link to the source record (src_system, src_record_id, src_batch_id, src_record_hash);
dim_party links to every source record behind each golden value (src_record_ids). A re-run
of a date restates that date's versions in place; facts join the version valid on their
business date. Every gold column must be described in model/source_mapping.csv; the job
fails if one is not, and loads the mapping into ref.source_mapping.

Usage:
  spark-submit build_gold.py --business-date 2026-09-22 [--db-prefix P]
"""

from __future__ import annotations

import csv
import io
import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gdl_common as C  # noqa: E402
from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "gold"
OPEN_END = "9999-12-31"
SCD_META = ["version", "effective_from", "effective_to", "is_current", "change_reason", "changed_attributes",
            "end_reason", "created_batch_id"]
DEPOSIT_TYPE = {"SA": "SAVINGS", "CA": "CURRENT", "TD": "TERM"}
UNKNOWN_PARTY = "UNKNOWN"
KPI = C.load_json("config/kpi.json")
AML = C.load_json("config/aml_rules.json")
MAPPING_SCHEMA = ("target_table string, target_column string, source_system string, source_entity string, "
                  "source_field string, transform_type string, rule string")


class Ctx:
    def __init__(self, spark, names, audit, d):
        self.spark, self.names, self.audit, self.d = spark, names, audit, d
        self.bid = C.batch_id(d)
        self.dlit = F.lit(d.isoformat()).cast("date")
        self.written: list[str] = []

    def t(self, name: str) -> str:
        return self.names.t(STAGE, name)

    def as_of(self, layer: str, entity: str) -> DataFrame:
        df = C.read_as_of_batch(self.spark, self.names, layer, entity, self.d)
        if df is None:
            raise RuntimeError(f"{self.names.t(layer, entity)} does not exist")
        return df

    def on_date(self, layer: str, entity: str) -> DataFrame:
        return self.spark.table(self.names.t(layer, entity)).where(F.col("business_date") == self.dlit)

    def committed(self, name: str, rows_in, rows_out, before, message: str = "") -> None:
        self.audit.load(STAGE, name, "COMMITTED", rows_in=rows_in, rows_out=rows_out, snapshot_before=before,
                        snapshot_after=C.snapshot_id(self.spark, self.t(name)), message=message)
        self.written.append(name)
        print(f"{name}: {rows_out} rows{' (' + message + ')' if message else ''}", flush=True)


def valid_on(df: DataFrame, dlit) -> DataFrame:
    return df.where((F.col("effective_from") <= dlit) & (F.col("effective_to") >= dlit))


# ---------------------------------------------------------------- SCD2


def scd2(ctx: Ctx, name: str, cur: DataFrame, keys: list, tracked: list, sk: str,
         held: DataFrame | None = None) -> None:
    """Version table `name` from `cur` (one row per key): new keys open version 1, changed tracked
    attributes close the open version the day before and open the next, keys gone from `cur` are
    closed; a re-run of the same date restates that date's version instead of adding one.
    `held` lists keys whose source record was quarantined on the date: their open version stays
    open (the source did not remove them, validation held them back). A key that comes back after
    being closed opens the version after its last one (REAPPEARED), so surrogate keys never repeat."""
    spark, t, d = ctx.spark, ctx.t(name), ctx.dlit
    hashed = F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in tracked]), 256)
    cur = cur.withColumn("record_hash", hashed)
    data_cols = cur.columns
    end, prev_day = F.lit(OPEN_END).cast("date"), F.date_sub(d, 1)
    empty_changes = F.array().cast("array<string>")

    def finish(df: DataFrame) -> DataFrame:
        return df.withColumn(sk, F.xxhash64(*keys, "version")).select(sk, *data_cols, *SCD_META)

    before = C.snapshot_id(spark, t)
    rows_in = cur.count()
    if not C.table_exists(spark, t):
        out = finish(cur.withColumn("version", F.lit(1)).withColumn("effective_from", d)
                     .withColumn("effective_to", end).withColumn("is_current", F.lit(True))
                     .withColumn("change_reason", F.lit("NEW")).withColumn("changed_attributes", empty_changes)
                     .withColumn("end_reason", F.lit(None).cast("string")).withColumn("created_batch_id", F.lit(ctx.bid)))
        C._create(out, t).create()
        ctx.committed(name, rows_in, out.count(), before, f"{rows_in} new")
        return
    old = spark.table(t).localCheckpoint()
    latest = old.agg(F.max("effective_from")).collect()[0][0]
    if latest is not None and latest > ctx.d:
        raise RuntimeError(f"{t} already has versions from {latest}; SCD2 is built forward: "
                           f"rebuild gold from the first business date to restate {ctx.d}")
    hist = old.where(~F.col("is_current")).drop(sk)
    others = [c for c in old.columns if c not in keys and c != sk]
    o = old.where("is_current").select(*keys, *[F.col(c).alias(f"o_{c}") for c in others])
    last = (hist.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(F.desc("version"))))
            .where("_rn = 1").select(*keys, F.col("version").alias("h_version"),
                                     *[F.col(c).alias(f"h_{c}") for c in tracked]))
    j = cur.withColumn("_present", F.lit(True)).join(o, keys, "full_outer").join(last, keys, "left")
    if held is not None:
        j = j.join(held.select(*keys).distinct().withColumn("_held", F.lit(True)), keys, "left")
    else:
        j = j.withColumn("_held", F.lit(None).cast("boolean"))
    present, is_new = F.col("_present").isNotNull(), F.col("o_version").isNull()
    reappeared = is_new & F.col("h_version").isNotNull()
    same = present & ~is_new & (F.col("record_hash") == F.col("o_record_hash"))
    changed = present & ~is_new & (F.col("record_hash") != F.col("o_record_hash"))
    restate = changed & (F.col("o_effective_from") == d)
    roll = changed & (F.col("o_effective_from") < d)
    carried = ~present & F.col("_held").isNotNull()
    vanished = ~present & F.col("_held").isNull()

    def old_row(df):
        return df.select(*keys, *[F.col(f"o_{c}").alias(c) for c in others])

    kept = old_row(j.where(same | carried))
    closed = (old_row(j.where(roll)).withColumn("effective_to", prev_day).withColumn("is_current", F.lit(False))
              .withColumn("end_reason", F.lit("SUPERSEDED")))
    gone = (old_row(j.where(vanished)).withColumn("effective_to", prev_day).withColumn("is_current", F.lit(False))
            .withColumn("end_reason", F.lit("REMOVED_AT_SOURCE")))

    def diffs(prefix):
        return F.filter(F.array(*[F.when(~F.col(c).eqNullSafe(F.col(f"{prefix}{c}")), F.lit(c)) for c in tracked]),
                        lambda x: x.isNotNull())

    new = (j.where(is_new | changed)
           .withColumn("version", F.when(reappeared, F.col("h_version") + 1).when(is_new, F.lit(1))
                       .when(restate, F.col("o_version")).otherwise(F.col("o_version") + 1))
           .withColumn("effective_from", F.when(restate, F.col("o_effective_from")).otherwise(d))
           .withColumn("effective_to", end).withColumn("is_current", F.lit(True))
           .withColumn("change_reason", F.when(reappeared, F.lit("REAPPEARED")).when(is_new, F.lit("NEW"))
                       .when(restate, F.coalesce(F.col("o_change_reason"), F.lit("CHANGED")))
                       .otherwise(F.lit("CHANGED")))
           .withColumn("changed_attributes", F.when(reappeared, diffs("h_")).when(is_new, empty_changes)
                       .otherwise(diffs("o_")))
           .withColumn("end_reason", F.lit(None).cast("string")).withColumn("created_batch_id", F.lit(ctx.bid))
           .select(*data_cols, *SCD_META))
    out = finish(hist.unionByName(kept).unionByName(closed).unionByName(gone).unionByName(new)).localCheckpoint()
    out.writeTo(t).overwrite(F.lit(True))
    counts = j.select(*[F.sum(c.cast("int")) for c in (is_new & ~reappeared, roll, restate, vanished, carried,
                                                        reappeared)]).collect()[0]
    n = [x or 0 for x in counts]
    ctx.committed(name, rows_in, out.count(), before,
                  f"{n[0]} new, {n[1]} new versions, {n[2]} restated, {n[3]} closed, "
                  f"{n[4]} held open (record quarantined), {n[5]} reappeared")


# ---------------------------------------------------------------- conformed dimensions


def replace_dim(ctx: Ctx, name: str, df: DataFrame, message: str = "") -> None:
    before = C.snapshot_id(ctx.spark, ctx.t(name))
    df = df.localCheckpoint()
    if C.table_exists(ctx.spark, ctx.t(name)):
        df.writeTo(ctx.t(name)).overwrite(F.lit(True))
    else:
        C._create(df, ctx.t(name)).create()
    n = df.count()
    ctx.committed(name, n, n, before, message)


def dim_date(ctx: Ctx) -> None:
    start, end = date(2015, 1, 1), date(2027, 12, 31)
    days = [(start + timedelta(days=i),) for i in range((end - start).days + 1)]
    d = F.col("calendar_date")
    fy = F.when(F.month(d) >= 4, F.year(d)).otherwise(F.year(d) - 1)
    df = ctx.spark.createDataFrame(days, "calendar_date date").select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"), d, F.year(d).alias("year"),
        F.quarter(d).alias("quarter"), F.month(d).alias("month"), F.date_format(d, "MMMM").alias("month_name"),
        F.dayofmonth(d).alias("day_of_month"), F.date_format(d, "EEEE").alias("day_name"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"), (d == F.last_day(d)).alias("is_month_end"),
        F.concat(F.lit("FY"), fy.cast("string"), F.lit("-"), F.substring((fy + 1).cast("string"), 3, 2)).alias("fiscal_year"),
        (F.floor(((F.month(d) + 8) % 12) / 3) + 1).cast("int").alias("fiscal_quarter"))
    replace_dim(ctx, "dim_date", df, "2015-01-01 to 2027-12-31")


def dim_branch(ctx: Ctx) -> None:
    b = ctx.as_of("silver", "cbs_branch").where("NOT _is_deleted")
    replace_dim(ctx, "dim_branch", b.select(
        "branch_code", "ifsc", "branch_name", "city", "state", "region", "opened_on",
        F.lit("cbs").alias("src_system"), F.col("_src_batch_id").alias("src_batch_id")))


def dim_product(ctx: Ctx) -> None:
    p = ctx.as_of("silver", "cbs_product").where("NOT _is_deleted")
    dtype = F.create_map(*[x for k, v in DEPOSIT_TYPE.items() for x in (F.lit(k), F.lit(v))])
    replace_dim(ctx, "dim_product", p.select(
        "product_code", "product_name", "product_type", "domain",
        F.element_at(dtype, F.col("product_type")).alias("deposit_type"),
        F.col("product_type").isin("SA", "CA").alias("is_casa"), "base_rate",
        F.lit("cbs").alias("src_system"), F.col("_src_batch_id").alias("src_batch_id")))


def dim_currency(ctx: Ctx) -> None:
    rows = [(c["code"], c["name"], float(c["inr_rate"])) for c in C.load_json("config/pipeline.json")["currencies"]]
    replace_dim(ctx, "dim_currency", ctx.spark.createDataFrame(
        rows, "currency_code string, currency_name string, inr_rate double"))


def kpi_reference(ctx: Ctx) -> None:
    spark, ref = ctx.spark, lambda n: ctx.names.t("ref", n)  # noqa: E731
    defs = [(k["kpi_code"], k["name"], k["definition"], k["formula"], k["grain"], k["certified_view"], k["owner"],
             k["version"], k["certified_on"], k["glossary_term"]) for k in KPI["kpis"]]
    C.replace_table(spark.createDataFrame(
        defs, "kpi_code string, kpi_name string, definition string, formula string, grain string, "
              "certified_view string, owner string, version string, certified_on string, glossary_term string"),
        ref("kpi_definition"))
    params = [("NPA", "npa_dpd", float(KPI["irac"]["npa_dpd"]), None, "days past due above which a loan is NPA")]
    params += [("NPA", f"provision_rate.{k}", float(v), k, f"provision on {k} outstanding")
               for k, v in KPI["provision_rates"].items()]
    crv = KPI["crv"]
    params += [("CRV", "casa_spread", crv["casa_spread"], None, "annual spread earned on CASA balances"),
               ("CRV", "td_spread", crv["td_spread"], None, "annual spread earned on term deposits"),
               ("CRV", "fee_window_days", float(crv["fee_window_days"]), None, "days of fee income counted"),
               ("CRV", "annualisation_factor", float(crv["annualisation_factor"]), None, "fee income multiplier")]
    params += [("CRV", f"lending_margin.{k}", float(v), k, f"annual margin on {k} outstanding")
               for k, v in crv["lending_margin"].items()]
    C.replace_table(spark.createDataFrame(
        params, "kpi_code string, parameter string, value double, applies_to string, description string"),
        ref("kpi_parameter"))


# ---------------------------------------------------------------- party, account, loan


def xref(ctx: Ctx, system: str) -> DataFrame:
    return (ctx.as_of("mdm", "party_xref").where(F.col("src_system") == system)
            .select(F.col("src_key"), "party_id"))


def dim_party(ctx: Ctx) -> None:
    g = ctx.as_of("mdm", "golden_party")
    tracked = ["full_name", "dob", "gender", "pan", "mobile", "email", "address", "pincode", "address_city",
               "segment", "kyc_status", "home_branch"]
    cur = g.select(
        "party_id", *tracked,
        F.array_join("source_systems", ",").alias("source_systems"), F.col("member_records").cast("int").alias("member_records"),
        F.array_join("cbs_cust_ids", ",").alias("cbs_cust_ids"), F.array_join("lms_borrower_ids", ",").alias("lms_borrower_ids"),
        F.array_join("crm_ids", ",").alias("crm_ids"), F.coalesce("has_pan_conflict", F.lit(False)).alias("has_pan_conflict"),
        F.lit("mdm").alias("src_system"), F.to_json("attribute_sources").alias("src_record_ids"),
        F.col("batch_id").alias("src_batch_id"), F.col("record_hash").alias("src_record_hash"))
    unknown = ctx.spark.createDataFrame([(UNKNOWN_PARTY, "UNRESOLVED PARTY")], "party_id string, full_name string")
    for c, typ in cur.dtypes:
        if c not in unknown.columns:
            unknown = unknown.withColumn(c, F.lit(None).cast(typ))
    cur = cur.unionByName(unknown.select(*cur.columns))
    scd2(ctx, "dim_party", cur, ["party_id"], tracked, "party_sk")


def dim_account(ctx: Ctx) -> None:
    a = ctx.as_of("silver", "cbs_account").where("NOT _is_deleted")
    owner = xref(ctx, "cbs")
    cur = (a.join(owner.withColumnRenamed("src_key", "cust_src_key"), "cust_src_key", "left")
           .join(owner.withColumnRenamed("src_key", "joint_cust_src_key").withColumnRenamed("party_id", "joint_party_id"),
                 "joint_cust_src_key", "left")
           .select("account_key", "acct_no", F.coalesce("party_id", F.lit(UNKNOWN_PARTY)).alias("party_id"),
                   "joint_party_id", F.col("cust_id").cast("string").alias("cbs_cust_id"), "product_code",
                   "branch_code", "currency", "status", "open_date", "close_date", "interest_rate",
                   F.lit("cbs").alias("src_system"), F.col("acct_no").alias("src_record_id"),
                   F.col("_src_position").alias("src_position"), F.col("_src_batch_id").alias("src_batch_id"),
                   F.col("_src_record_hash").alias("src_record_hash")))
    scd2(ctx, "dim_account", cur, ["account_key"],
         ["party_id", "joint_party_id", "product_code", "branch_code", "currency", "status", "close_date",
          "interest_rate"], "account_sk")


def dim_loan(ctx: Ctx) -> None:
    loans = ctx.on_date("silver", "lms_loan_daily")
    borrower = xref(ctx, "lms").withColumnRenamed("src_key", "borrower_id")
    cur = (loans.join(borrower, "borrower_id", "left")
           .select("loan_key", "loan_id", F.coalesce("party_id", F.lit(UNKNOWN_PARTY)).alias("party_id"),
                   "borrower_id", "product_code", "branch_code", "sanction_date", "sanction_amount", "interest_rate",
                   "tenure_months", "emi_amount", "restructured_flag", "loan_status",
                   F.lit("lms").alias("src_system"), F.col("loan_id").alias("src_record_id"),
                   F.col("_src_position").alias("src_position"), F.col("_src_batch_id").alias("src_batch_id"),
                   F.col("_src_record_hash").alias("src_record_hash")))
    scd2(ctx, "dim_loan", cur, ["loan_key"],
         ["party_id", "product_code", "branch_code", "interest_rate", "tenure_months", "emi_amount",
          "restructured_flag", "loan_status"], "loan_sk",
         held=quarantined_keys(ctx, "lms_loan", "loan_id", "LN:LMS:", "loan_key"))


def quarantined_keys(ctx: Ctx, entity: str, field: str, key_prefix: str, key: str) -> DataFrame:
    """Keys of the entity's records quarantined at bronze on the date (a daily snapshot feed
    misses them, but the source still has them)."""
    q = ctx.spark.table(ctx.names.t("bronze", "quarantine"))
    return (q.where((F.col("_entity") == entity) & (F.col("_business_date") == ctx.dlit))
            .select(F.concat(F.lit(key_prefix), F.get_json_object("_record", f"$.{field}")).alias(key))
            .where(F.col(key).isNotNull()))


def bridge(ctx: Ctx) -> None:
    acc = ctx.spark.table(ctx.t("dim_account")).where("is_current")
    loan = ctx.spark.table(ctx.t("dim_loan")).where("is_current")
    df = (acc.select("party_id", "account_key", F.lit("PRIMARY").alias("role"), F.lit("DEPOSITS").alias("domain"))
          .unionByName(acc.where(F.col("joint_party_id").isNotNull())
                       .select(F.col("joint_party_id").alias("party_id"), "account_key", F.lit("JOINT").alias("role"),
                               F.lit("DEPOSITS").alias("domain")))
          .unionByName(loan.select("party_id", F.col("loan_key").alias("account_key"), F.lit("BORROWER").alias("role"),
                                   F.lit("LENDING").alias("domain")))
          .withColumn("as_of_date", ctx.dlit))
    replace_dim(ctx, "bridge_party_account", df)


# ---------------------------------------------------------------- facts


def replace_fact(ctx: Ctx, name: str, df: DataFrame, rows_in: int, message: str = "") -> None:
    t = ctx.t(name)
    before = C.snapshot_id(ctx.spark, t)
    df = df.withColumn("business_date", ctx.dlit)
    if not C.table_exists(ctx.spark, t):
        C._create(df.limit(0), t, ["business_date"]).create()
    if df.isEmpty():
        ctx.spark.sql(f"DELETE FROM {t} WHERE business_date = DATE '{ctx.d.isoformat()}'")
    else:
        C.write_partitions(df, t, ["business_date"])
    n = ctx.spark.table(t).where(F.col("business_date") == ctx.dlit).count()
    ctx.committed(name, rows_in, n, before, message)


def party_sk(ctx: Ctx) -> DataFrame:
    return valid_on(ctx.spark.table(ctx.t("dim_party")), ctx.dlit).select("party_id", "party_sk")


def date_key(col="business_date"):
    return F.date_format(col, "yyyyMMdd").cast("int").alias("date_key")


def fact_deposit_balance(ctx: Ctx) -> None:
    eod = ctx.on_date("silver", "cbs_eod_balance")
    acc = valid_on(ctx.spark.table(ctx.t("dim_account")), ctx.dlit).select(
        "account_key", "account_sk", "party_id", "product_code", "branch_code")
    prod = ctx.spark.table(ctx.t("dim_product")).select("product_code", "deposit_type", "is_casa")
    rows_in = eod.count()
    df = (eod.join(acc, "account_key", "left").join(prod, "product_code", "left").join(party_sk(ctx), "party_id", "left")
          .select(date_key(), "account_key", "account_sk", F.coalesce("party_id", F.lit(UNKNOWN_PARTY)).alias("party_id"),
                  "party_sk", "product_code", "branch_code", "currency", "deposit_type", "is_casa",
                  F.col("ledger_balance").alias("balance"), F.col("ledger_balance_inr").alias("balance_inr"),
                  "available_balance_inr", F.col("fx_rate_inr").alias("fx_rate"),
                  F.lit("cbs").alias("src_system"), F.col("_src_position").alias("src_position"),
                  F.col("_src_batch_id").alias("src_batch_id"), F.col("_src_record_hash").alias("src_record_hash")))
    replace_fact(ctx, "fact_deposit_balance_daily", df, rows_in)


def asset_class(dpd, loss_flag):
    """IRAC asset class from days past due (config/kpi.json); a flagged loss is LOSS."""
    irac = KPI["irac"]
    npa_days = dpd - irac["npa_dpd"]
    c = F.when(loss_flag == "Y", F.lit(irac["loss_flag_class"]))
    for band in irac["npa_classes"]:
        cond = (dpd > irac["npa_dpd"]) if band["npa_days_to"] is None else \
            (dpd > irac["npa_dpd"]) & (npa_days <= band["npa_days_to"])
        c = c.when(cond, F.lit(band["class"]))
    return c.otherwise(F.lit("STANDARD"))


def fact_loan_position(ctx: Ctx) -> None:
    loans = ctx.on_date("silver", "lms_loan_daily")
    dim = valid_on(ctx.spark.table(ctx.t("dim_loan")), ctx.dlit).select("loan_key", "loan_sk", "party_id")
    irac = KPI["irac"]
    sma = F.when(F.col("dpd") <= 0, F.lit("REGULAR"))
    for band in irac["sma"]:
        sma = sma.when(F.col("dpd").between(band["dpd_from"], band["dpd_to"]), F.lit(band["class"]))
    sma = sma.otherwise(F.lit("NPA"))
    rates = F.create_map(*[x for k, v in KPI["provision_rates"].items() for x in (F.lit(k), F.lit(float(v)))])
    rows_in = loans.count()
    cls = asset_class(F.col("dpd"), F.col("loss_flag"))
    df = (loans.join(dim, "loan_key", "left").join(party_sk(ctx), "party_id", "left")
          .withColumn("asset_class", F.when(F.col("loan_status") == "CLOSED", F.lit("CLOSED")).otherwise(cls))
          .withColumn("is_npa", F.col("asset_class").isin([b["class"] for b in irac["npa_classes"]]
                                                           + [irac["loss_flag_class"]]))
          .withColumn("provision_rate", F.coalesce(F.element_at(rates, F.col("asset_class")), F.lit(0.0)))
          .select(date_key(), "loan_key", "loan_sk", F.coalesce("party_id", F.lit(UNKNOWN_PARTY)).alias("party_id"),
                  "party_sk", "product_code", "branch_code", "principal_outstanding", "principal_overdue",
                  "interest_overdue", "dpd", sma.alias("sma_status"), "asset_class", "is_npa",
                  F.when(F.col("is_npa"), F.date_sub(F.col("as_of_date"), F.col("dpd") - irac["npa_dpd"]))
                  .alias("npa_since"),
                  "provision_rate",
                  F.round(F.col("principal_outstanding") * F.col("provision_rate"), 2).cast(C.DECIMAL)
                  .alias("provision_amount"),
                  F.col("src_asset_class").alias("source_asset_class"),
                  (F.regexp_replace("asset_class", r"_\d+$", "") != F.col("src_asset_class"))
                  .alias("class_differs_from_source"),
                  "loan_status", F.lit("lms").alias("src_system"), F.col("_src_position").alias("src_position"),
                  F.col("_src_batch_id").alias("src_batch_id"), F.col("_src_record_hash").alias("src_record_hash")))
    replace_fact(ctx, "fact_loan_position_daily", df, rows_in)


def fact_payment(ctx: Ctx) -> None:
    pay = ctx.on_date("silver", "pay_transaction")
    rows_in = pay.count()
    acc = valid_on(ctx.spark.table(ctx.t("dim_account")), ctx.dlit).select("account_key", "account_sk", "party_id")
    df = (pay.where(F.col("account_key").isNotNull())
          .join(acc, "account_key", "left").join(party_sk(ctx), "party_id", "left")
          .select(date_key(), "msg_id", "account_key", "account_sk",
                  F.coalesce("party_id", F.lit(UNKNOWN_PARTY)).alias("party_id"), "party_sk", "value_date",
                  "created_at", "channel", "direction", "status", "currency", "amount", "amount_inr",
                  "fee_amount", "gst_amount", "counterparty_bank", "is_on_us", "purpose_code", "device_id",
                  F.lit("payments").alias("src_system"), F.col("_src_position").alias("src_position"),
                  F.col("_src_batch_id").alias("src_batch_id"), F.col("_src_record_hash").alias("src_record_hash")))
    n = df.count()
    replace_fact(ctx, "fact_payment", df, rows_in, f"{rows_in - n} payments not on an own account")


# ---------------------------------------------------------------- AML (the extension domain)


def dim_aml_rule(ctx: Ctx) -> None:
    rows = [(r["rule_code"], r["name"], r["subject"], r["severity"], r["description"],
             json.dumps(r["params"], sort_keys=True)) for r in AML["rules"]]
    replace_dim(ctx, "dim_aml_rule", ctx.spark.createDataFrame(
        rows, "rule_code string, rule_name string, subject_type string, severity string, description string, "
              "parameters string"), "from config/aml_rules.json")


def account_alerts(ctx: Ctx, rule: dict, df: DataFrame) -> DataFrame:
    """Account-level alerts (account_key, txn_count, amount_inr, window_from, window_to, evidence) with
    the account and party versions valid on the date."""
    acc = valid_on(ctx.spark.table(ctx.t("dim_account")), ctx.dlit).select(
        "account_key", "account_sk", "party_id", "branch_code")
    return (df.join(acc, "account_key", "left").join(party_sk(ctx), "party_id", "left")
            .select(F.lit(rule["rule_code"]).alias("rule_code"), F.col("account_key").alias("subject_key"),
                    F.coalesce("party_id", F.lit(UNKNOWN_PARTY)).alias("party_id"), "party_sk", "account_key",
                    "account_sk", "branch_code", "window_from", "window_to", "txn_count", "amount_inr",
                    F.lit(None).cast("string").alias("entry_id"), F.lit(None).cast("string").alias("list_name"),
                    F.lit(None).cast("string").alias("match_basis"), "evidence",
                    F.lit("payments").alias("src_system")))


def evidence(col):
    return F.array_join(F.slice(F.array_sort(F.collect_list(col)), 1, 20), ",").alias("evidence")


def structuring(ctx: Ctx, rule: dict) -> DataFrame:
    p, prm = ctx.spark.table(ctx.t("fact_payment")), rule["params"]
    dates = [r[0] for r in p.where(F.col("business_date") <= ctx.dlit).select("business_date").distinct()
             .orderBy(F.desc("business_date")).limit(prm["window_business_dates"]).collect()]
    cash = p.where(F.col("business_date").isin(dates) & (F.col("channel") == "CASH_DEPOSIT")
                   & (F.col("direction") == "CR") & (F.col("status") == "SETTLED")
                   & F.col("amount_inr").between(prm["band_from"], prm["band_to"]))
    hits = (cash.groupBy("account_key")
            .agg(F.count("*").alias("txn_count"), F.sum("amount_inr").cast(C.DECIMAL).alias("amount_inr"),
                 F.min("value_date").alias("window_from"), F.max("value_date").alias("window_to"), evidence("msg_id"))
            .where(F.col("txn_count") >= prm["min_count"]))
    return account_alerts(ctx, rule, hits)


def pass_through(ctx: Ctx, rule: dict) -> DataFrame:
    prm = rule["params"]
    p = ctx.on_date("gold", "fact_payment").where(F.col("status") == "SETTLED")
    cr, dr = F.col("direction") == "CR", F.col("direction") == "DR"
    hits = (p.groupBy("account_key")
            .agg(F.sum(cr.cast("int")).alias("credits"), F.sum(F.when(cr, F.col("amount_inr"))).alias("credit_inr"),
                 F.sum(F.when(dr, F.col("amount_inr"))).alias("debit_inr"), F.count("*").alias("txn_count"),
                 F.min("value_date").alias("window_from"), F.max("value_date").alias("window_to"), evidence("msg_id"))
            .where((F.col("credits") >= prm["min_credits"]) & (F.col("credit_inr") >= prm["min_credit_total"])
                   & (F.col("debit_inr") >= F.col("credit_inr") * prm["min_out_ratio"]))
            .withColumn("amount_inr", F.col("credit_inr").cast(C.DECIMAL)))
    return account_alerts(ctx, rule, hits)


def watchlist_alerts(ctx: Ctx, rules: dict) -> DataFrame | None:
    """Golden parties valid on the date against the screening-list entries active on it (silver as the
    batch committed it): PAN first; name and date of birth only where the PAN did not match."""
    wl = C.read_as_of_batch(ctx.spark, ctx.names, "silver", "aml_watchlist", ctx.d)
    if wl is None:
        return None
    d = ctx.dlit
    wl = wl.where(~F.col("_is_deleted") & (F.col("listed_on") <= d)
                  & (F.col("delisted_on").isNull() | (F.col("delisted_on") > d))).select(
        "entry_id", "list_name", F.col("pan_std").alias("w_pan"), F.col("name_key").alias("w_name"),
        F.col("dob").alias("w_dob"))
    party = (valid_on(ctx.spark.table(ctx.t("dim_party")), d).where(F.col("party_id") != UNKNOWN_PARTY)
             .select("party_id", "party_sk", F.col("home_branch").alias("branch_code"), "dob",
                     C.std_pan(F.col("pan")).alias("pan_std"), C.name_key(C.std_name(F.col("full_name"))).alias("name_key")))
    by_pan = party.join(wl, party["pan_std"] == wl["w_pan"]).withColumn("match_basis", F.lit("PAN"))
    by_name = (party.join(wl, (party["name_key"] == wl["w_name"]) & (party["dob"] == wl["w_dob"]))
               .withColumn("match_basis", F.lit("NAME_DOB")))
    both = by_pan.unionByName(by_name)
    first = Window.partitionBy("party_id", "entry_id").orderBy(F.when(F.col("match_basis") == "PAN", 0).otherwise(1))
    code = F.create_map(*[x for r in rules.values() for x in (F.lit(r["params"]["basis"]), F.lit(r["rule_code"]))])
    return (both.withColumn("_rn", F.row_number().over(first)).where("_rn = 1")
            .select(F.element_at(code, F.col("match_basis")).alias("rule_code"),
                    F.concat_ws("|", "party_id", "entry_id").alias("subject_key"), "party_id", "party_sk",
                    F.lit(None).cast("string").alias("account_key"), F.lit(None).cast("bigint").alias("account_sk"),
                    "branch_code", d.alias("window_from"), d.alias("window_to"), F.lit(1).cast("bigint").alias("txn_count"),
                    F.lit(None).cast(C.DECIMAL).alias("amount_inr"), "entry_id", "list_name", "match_basis",
                    F.concat(F.lit("watchlist "), F.col("entry_id")).alias("evidence"),
                    F.lit("compliance").alias("src_system")))


def fact_aml_alert(ctx: Ctx) -> None:
    """One row per rule and subject (account, or party and list entry) that alerts on the date. is_new:
    the same rule did not alert on the same subject on an earlier business date."""
    rules = {r["rule_code"]: r for r in AML["rules"]}
    parts = [structuring(ctx, rules["AML-STR-01"]), pass_through(ctx, rules["AML-PTH-01"])]
    wl = watchlist_alerts(ctx, {k: v for k, v in rules.items() if v["subject"] == "PARTY"})
    if wl is not None:
        parts.append(wl)
    df = parts[0]
    for x in parts[1:]:
        df = df.unionByName(x)
    sev = F.create_map(*[x for r in rules.values() for x in (F.lit(r["rule_code"]), F.lit(r["severity"]))])
    t = ctx.t("fact_aml_alert")
    if C.table_exists(ctx.spark, t):
        seen = (ctx.spark.table(t).where(F.col("business_date") < ctx.dlit).select("rule_code", "subject_key")
                .distinct().withColumn("_seen", F.lit(True)))
        df = df.join(seen, ["rule_code", "subject_key"], "left")
    else:
        df = df.withColumn("_seen", F.lit(None).cast("boolean"))
    df = df.select(
        date_key(F.lit(ctx.d.isoformat()).cast("date")),
        F.substring(F.sha2(F.concat_ws("|", "rule_code", "subject_key", F.lit(ctx.d.isoformat())), 256), 1, 16)
        .alias("alert_id"),
        "rule_code", F.element_at(sev, F.col("rule_code")).alias("severity"),
        F.when(F.col("account_key").isNotNull(), "ACCOUNT").otherwise("PARTY").alias("subject_type"),
        "subject_key", "party_id", "party_sk", "account_key", "account_sk", "branch_code", "window_from", "window_to",
        "txn_count", "amount_inr", "entry_id", "list_name", "match_basis", "evidence",
        F.col("_seen").isNull().alias("is_new"), "src_system", F.lit(ctx.bid).alias("src_batch_id")).localCheckpoint()
    n = df.count()
    by_rule = {r["rule_code"]: r["count"] for r in df.groupBy("rule_code").count().collect()}
    replace_fact(ctx, "fact_aml_alert", df, n, ", ".join(f"{k} {by_rule.get(k, 0)}" for k in rules))


# ---------------------------------------------------------------- source mapping


def source_mapping(ctx: Ctx) -> None:
    text = (C.repo_root() / "model" / "source_mapping.csv").read_text()
    rows = [tuple(r[k] or None for k in ("target_table", "target_column", "source_system", "source_entity",
                                         "source_field", "transform_type", "rule"))
            for r in csv.DictReader(io.StringIO(text))]
    mapped = {(r[0], r[1]) for r in rows}
    missing = []
    for name in sorted(set(ctx.written)):
        for col in ctx.spark.table(ctx.t(name)).columns:
            if (name, col) not in mapped:
                missing.append(f"{name}.{col}")
    if missing:
        raise RuntimeError(f"gold columns without a row in model/source_mapping.csv: {', '.join(missing)}")
    C.replace_table(ctx.spark.createDataFrame(rows, MAPPING_SCHEMA), ctx.names.t("ref", "source_mapping"))


# ---------------------------------------------------------------- run


def run(spark, argv=None) -> dict:
    args = C.parse(C.base_parser(__doc__), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    C.require_completed(spark, names, "mdm", args.business_date)
    audit = C.Audit(spark, names, "build_gold", args.business_date, args.pipeline_run)
    ctx = Ctx(spark, names, audit, args.business_date)
    s, m = lambda n: names.t("silver", n), lambda n: names.t("mdm", n)  # noqa: E731
    audit.load(STAGE, "*", "STARTED")
    try:
        dim_date(ctx)
        dim_branch(ctx)
        dim_product(ctx)
        dim_currency(ctx)
        kpi_reference(ctx)
        dim_party(ctx)
        audit.transform("dim_party", "historise", m("golden_party"), ctx.t("dim_party"), None, None,
                        "SCD2 on the golden record's survived attributes")
        dim_account(ctx)
        audit.transform("dim_account", "historise", f"{s('cbs_account')},{m('party_xref')}", ctx.t("dim_account"),
                        None, None, "SCD2 on owner party, product, branch, status, rate")
        dim_loan(ctx)
        audit.transform("dim_loan", "historise", f"{s('lms_loan_daily')},{m('party_xref')}", ctx.t("dim_loan"),
                        None, None, "SCD2 on borrower party, product, rate, restructuring, status")
        bridge(ctx)
        fact_deposit_balance(ctx)
        audit.transform("fact_deposit_balance_daily", "enrich", f"{s('cbs_eod_balance')},{ctx.t('dim_account')}",
                        ctx.t("fact_deposit_balance_daily"), None, None,
                        "account and party versions valid on the date; deposit type from product")
        fact_loan_position(ctx)
        audit.transform("fact_loan_position_daily", "enrich", f"{s('lms_loan_daily')},{ctx.t('dim_loan')}",
                        ctx.t("fact_loan_position_daily"), None, None,
                        "IRAC asset class and provision from DPD (config/kpi.json)")
        fact_payment(ctx)
        audit.transform("fact_payment", "enrich", f"{s('pay_transaction')},{ctx.t('dim_account')}",
                        ctx.t("fact_payment"), None, None, "own-account payments with account and party")
        dim_aml_rule(ctx)
        fact_aml_alert(ctx)
        audit.transform("fact_aml_alert", "enrich",
                        f"{ctx.t('fact_payment')},{s('aml_watchlist')},{ctx.t('dim_party')},{ctx.t('dim_account')}",
                        ctx.t("fact_aml_alert"), None, None, "rules in config/aml_rules.json")
        source_mapping(ctx)
        audit.load(STAGE, "*", "COMPLETED", rows_out=len(ctx.written))
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return {"tables": ctx.written}


def main() -> int:
    spark = C.get_spark("gdl-build-gold")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
