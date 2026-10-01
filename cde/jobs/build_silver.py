"""
Stage 2 - Silver: typed, cleansed, standardised; one current state per source record.

For one business date, from that date's bronze rows:

  state tables     (MERGE by source key, newest source timestamp wins, so a re-run changes nothing)
    cbs_customer     D1 dump rows, then CDC after-images; deletes are soft (_is_deleted)
    cbs_account      the same for accounts
    cbs_branch, cbs_product, lms_borrower, crm_customer
  daily tables     (the business date's partition replaced in one commit)
    cbs_eod_balance  duplicates removed, balances converted to INR
    lms_loan_daily, lms_repayment
    pay_transaction  nested JSON flattened, re-sends removed, fees and GST, INR, counterparty bank
    doc_extract      entities read from e-mails and KYC declarations (customer id, account, PAN,
                     mobile, address, intent), scan size from the PNG header
  cdc_exception    what CDC handling did not apply as-is: a re-sent event (DEDUPED), events out
                   of order in the file (APPLIED_BY_TS), an event older than the applied state
                   (IGNORED), a delete of a key never loaded (IGNORED), an update of a key not in
                   silver, e.g. a record quarantined at load and corrected at source (INSERTED)

CDC events are applied in source timestamp order (ts_ms), never in file order. Every row
keeps its link to the source record: _src_system, _src_key, _src_op, _src_ts,
_src_position (file:row or binlog file:pos), _src_batch_id, _src_record_hash.
Standardised columns sit next to the source values: name_std, name_key, pan_std,
mobile_e164, email_std, address_std, pincode_std.

Usage:
  spark-submit build_silver.py --business-date 2026-09-22 [--db-prefix P]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gdl_common as C  # noqa: E402
from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "silver"
EXCEPTION_SCHEMA = ("business_date date, batch_id string, entity string, src_key string, op string, "
                    "event_ts timestamp, position string, kind string, action string, detail string")
CFG = C.load_json("config/pipeline.json")


class Ctx:
    def __init__(self, spark, names, audit, business_date):
        self.spark, self.names, self.audit = spark, names, audit
        self.d, self.bid = business_date, C.batch_id(business_date)
        self.contracts = C.contracts()
        self.exceptions: list = []
        self.summary: dict = {}

    def bronze(self, entity: str) -> DataFrame | None:
        t = self.names.t("bronze", entity)
        if not C.table_exists(self.spark, t):
            return None
        return self.spark.table(t).where(F.col("_business_date") == F.lit(self.d.isoformat()).cast("date"))

    def committed(self, entity: str, rows_in: int, rows_out: int, before, message: str = "") -> None:
        target = self.names.t(STAGE, entity)
        self.audit.load(STAGE, entity, "COMMITTED", rows_in=rows_in, rows_out=rows_out, snapshot_before=before,
                        snapshot_after=C.snapshot_id(self.spark, target), message=message)
        self.summary[entity] = {"rows_in": rows_in, "rows_out": rows_out}
        print(f"{entity}: {rows_in} in, {rows_out} out{' (' + message + ')' if message else ''}", flush=True)


def nz(col):
    return F.when(col != "", col)


def lineage(system: str, key, op, ts, position, deleted=None) -> list:
    deleted = F.lit(False) if deleted is None else deleted
    return [F.lit(system).alias("_src_system"), key.cast("string").alias("_src_key"), op.alias("_src_op"),
            ts.alias("_src_ts"), position.alias("_src_position"), F.col("_batch_id").alias("_src_batch_id"),
            F.col("_record_hash").alias("_src_record_hash"), deleted.alias("_is_deleted")]


def file_position():
    return F.concat_ws(":", "_source_file", F.col("_source_row").cast("string"))


def latest_per_key(df: DataFrame, keys: list, *order) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def exception_rows(ctx: Ctx, df: DataFrame, entity: str, keys: list, kind: str, action: str, detail) -> None:
    """Collected now: they compare against the target as it is before the MERGE."""
    ctx.exceptions += df.select(
        F.lit(ctx.d.isoformat()).cast("date").alias("business_date"), F.lit(ctx.bid).alias("batch_id"),
        F.lit(entity).alias("entity"), F.concat_ws("|", *[F.col(k).cast("string") for k in keys]).alias("src_key"),
        F.col("_src_op").alias("op"), F.col("_src_ts").alias("event_ts"), F.col("_src_position").alias("position"),
        F.lit(kind).alias("kind"), F.lit(action).alias("action"), detail.alias("detail")).collect()


# ---------------------------------------------------------------- writers


def apply_state(ctx: Ctx, entity: str, src: DataFrame, keys: list, rows_in: int, message: str = "") -> None:
    """MERGE the newest version of each key into silver.<entity>; older-than-applied events are ignored."""
    spark, target = ctx.spark, ctx.names.t(STAGE, entity)
    src = src.withColumn("_first_batch_id", F.col("_src_batch_id")).cache()
    if not C.table_exists(spark, target):
        C._create(src.limit(0), target).create()
    before = C.snapshot_id(spark, target)
    current = spark.table(target).select(*keys, F.col("_src_ts").alias("_t_ts"))
    j = src.join(current, keys, "left")
    exception_rows(ctx, j.where(F.col("_t_ts").isNotNull() & (F.col("_src_ts") < F.col("_t_ts"))), entity, keys,
                   "STALE_EVENT", "IGNORED", F.concat(F.lit("older than the applied state of "),
                                                       F.col("_t_ts").cast("string")))
    exception_rows(ctx, j.where(F.col("_t_ts").isNull() & F.col("_is_deleted")), entity, keys,
                   "DELETE_UNKNOWN_KEY", "IGNORED", F.lit("delete of a key never loaded into silver"))
    exception_rows(ctx, j.where(F.col("_t_ts").isNull() & (F.col("_src_op") == "u")), entity, keys,
                   "UPSERT_UNKNOWN_KEY", "INSERTED",
                   F.lit("update of a key not in silver (quarantined at load, corrected at source): inserted"))
    view = f"_silver_src_{entity}"
    src.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    sets = ", ".join(f"t.`{c}` = s.`{c}`" for c in src.columns if c != "_first_batch_id")
    spark.sql(f"""
        MERGE INTO {target} t USING {view} s ON {on}
        WHEN MATCHED AND s._src_ts >= t._src_ts AND s._src_record_hash <> t._src_record_hash THEN UPDATE SET {sets}
        WHEN NOT MATCHED AND NOT s._is_deleted THEN INSERT *""")
    src.unpersist()
    live = spark.table(target).where("NOT _is_deleted").count()
    ctx.committed(entity, rows_in, live, before, message or f"{live} live records")


def replace_date(ctx: Ctx, entity: str, df: DataFrame, rows_in: int, message: str = "") -> None:
    spark, target = ctx.spark, ctx.names.t(STAGE, entity)
    df = df.withColumn("business_date", F.lit(ctx.d.isoformat()).cast("date"))
    before = C.snapshot_id(spark, target)
    if not C.table_exists(spark, target):
        C._create(df.limit(0), target, ["business_date"]).create()
    if df.isEmpty():
        spark.sql(f"DELETE FROM {target} WHERE business_date = DATE '{ctx.d.isoformat()}'")
        n = 0
    else:
        C.write_partitions(df, target, ["business_date"])
        n = spark.table(target).where(F.col("business_date") == F.lit(ctx.d.isoformat()).cast("date")).count()
    ctx.committed(entity, rows_in, n, before, message)


def log(ctx: Ctx, entity: str, transform_type: str, source: str, rows_in, rows_out, details: str) -> None:
    ctx.audit.transform(f"{transform_type} {entity}", transform_type, ctx.names.t("bronze", source),
                        ctx.names.t(STAGE, entity), rows_in, rows_out, details)


# ---------------------------------------------------------------- standardisers


def person_std(df: DataFrame, full_name, pan, mobile, email, address, pincode) -> DataFrame:
    return (df.withColumn("name_std", C.std_name(full_name))
            .withColumn("name_key", C.name_key(F.col("name_std")))
            .withColumn("pan_std", C.std_pan(pan))
            .withColumn("mobile_e164", C.std_mobile(mobile))
            .withColumn("email_std", C.std_email(email) if email is not None else F.lit(None).cast("string"))
            .withColumn("address_std", C.std_address(address))
            .withColumn("pincode_std", C.std_pincode(pincode)))


PERSON_STD = "name (case, punctuation, titles, variants, token order), PAN, mobile E.164, e-mail, address abbreviations, pincode"


# ---------------------------------------------------------------- CBS (dump + CDC)


def cdc_images(ctx: Ctx, table: str, image_entity: str) -> tuple[DataFrame | None, int]:
    """Typed images of one table's CDC events of the date (after for c/u, before for d), newest per key."""
    ev = ctx.bronze("cbs_cdc_event")
    if ev is None:
        return None, 0
    contract, keys = ctx.contracts[image_entity], ctx.contracts[image_entity]["key"]
    ev = ev.where(F.col("table") == table)
    image = lambda c: F.when(F.col("op") == "d", F.get_json_object("payload", f"$.before.{c['name']}")) \
        .otherwise(F.get_json_object("payload", f"$.after.{c['name']}"))  # noqa: E731
    df = ev.select("_batch_id", "_record_hash", "_source_row", "op", F.col("ts_ms").cast("bigint").alias("ts_ms"),
                   "binlog_file", F.col("binlog_pos").cast("bigint").alias("binlog_pos"),
                   *C.typed_columns(contract, image))
    df = df.select(*[c["name"] for c in contract["columns"]], "_source_row", "ts_ms", "binlog_pos",
                   *lineage("cbs", F.col(keys[0]), F.col("op"), F.expr("timestamp_millis(ts_ms)"),
                            F.concat_ws(":", "binlog_file", F.col("binlog_pos").cast("string")),
                            F.col("op") == "d")).cache()
    rows_in = df.count()
    if rows_in == 0:
        df.unpersist()
        return None, 0

    dup_w = Window.partitionBy("_src_position").orderBy("_source_row")
    df = df.withColumn("_dup", F.row_number().over(dup_w) > 1)
    exception_rows(ctx, df.where("_dup"), image_entity, keys, "DUPLICATE_EVENT", "DEDUPED",
                   F.lit("same binlog position received twice"))
    df = df.where("NOT _dup").drop("_dup")

    by_file = Window.partitionBy(*keys).orderBy("_source_row")
    by_ts = Window.partitionBy(*keys).orderBy("ts_ms", "binlog_pos")
    df = df.withColumn("_file_rank", F.row_number().over(by_file)).withColumn("_ts_rank", F.row_number().over(by_ts))
    exception_rows(ctx, df.where(F.col("_file_rank") != F.col("_ts_rank")), image_entity, keys, "OUT_OF_ORDER",
                   "APPLIED_BY_TS", F.concat(F.lit("file order "), F.col("_file_rank").cast("string"),
                                             F.lit(", timestamp order "), F.col("_ts_rank").cast("string")))
    latest = latest_per_key(df, keys, F.desc("ts_ms"), F.desc("binlog_pos"))
    return latest.drop("_source_row", "ts_ms", "binlog_pos", "_file_rank", "_ts_rank"), rows_in


def cbs_state(ctx: Ctx, entity: str, table: str, extra) -> None:
    contract, keys = ctx.contracts[entity], ctx.contracts[entity]["key"]
    parts, rows_in, ops = [], 0, []
    dump = ctx.bronze(entity)
    if dump is not None:
        snap = dump.select("*", *[c.alias(f"_t_{n}") for n, c in
                                  zip([c["name"] for c in contract["columns"]], C.typed_columns(contract))])
        snap = snap.select(*[F.col(f"_t_{c['name']}").alias(c["name"]) for c in contract["columns"]],
                           *lineage("cbs", F.col(f"_t_{keys[0]}"), F.lit("snapshot"),
                                    C.source_ts(F.col("_t_updated_at")), file_position()))
        n = snap.count()
        if n:
            parts.append(snap)
            rows_in += n
            ops.append(f"dump {n}")
    events, n = cdc_images(ctx, table, entity)
    if events is not None:
        parts.append(events)
        rows_in += n
        ops.append(f"cdc {n}")
    if not parts:
        return
    src = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    src = latest_per_key(src, keys, F.desc("_src_ts"))
    src = extra(src)
    apply_state(ctx, entity, src, keys, rows_in, ", ".join(ops))
    sources = "cbs_customer/cbs_account dump + cbs_cdc_event"
    log(ctx, entity, "cleanse", entity, rows_in, rows_in, f"typed per contract; blanks to NULL ({sources})")
    if events is not None:
        log(ctx, entity, "normalise", "cbs_cdc_event", n, None,
            f"CDC {table} images flattened; applied in ts_ms order by MERGE; deletes soft")
        log(ctx, entity, "deduplicate", "cbs_cdc_event", n, None, "re-sent binlog positions dropped")


def customer_extra(df: DataFrame) -> DataFrame:
    full = F.concat_ws(" ", "first_name", "middle_name", "last_name")
    return person_std(df.withColumn("full_name", full), F.col("full_name"), F.col("pan"), F.col("mobile"),
                      F.col("email"), F.concat_ws(" ", "addr_line1", "addr_line2"), F.col("pincode"))


def account_extra(df: DataFrame) -> DataFrame:
    return (df.withColumn("account_key", F.concat(F.lit("ACC:CBS:"), "acct_no"))
            .withColumn("cust_src_key", F.col("cust_id").cast("string"))
            .withColumn("joint_cust_src_key", F.col("joint_cust_id").cast("string")))


def simple_state(ctx: Ctx, entity: str, system: str, ts, extra=None) -> None:
    raw = ctx.bronze(entity)
    if raw is None:
        return
    contract, keys = ctx.contracts[entity], ctx.contracts[entity]["key"]
    typed = raw.select("*", *[c.alias(f"_t_{c_def['name']}") for c_def, c in
                              zip(contract["columns"], C.typed_columns(contract))])
    rows_in = typed.count()
    if rows_in == 0:
        return
    src = typed.select(*[F.col(f"_t_{c['name']}").alias(c["name"]) for c in contract["columns"]],
                       *lineage(system, F.concat_ws("|", *[F.col(f"_t_{k}") for k in keys]), F.lit("extract"),
                                ts, file_position()))
    src = latest_per_key(src, keys, F.desc("_src_ts"), F.desc("_src_position"))
    if extra is not None:
        src = extra(src)
    apply_state(ctx, entity, src, keys, rows_in)
    log(ctx, entity, "cleanse", entity, rows_in, rows_in, "typed per contract; blanks to NULL")


def borrower_extra(df: DataFrame) -> DataFrame:
    return (person_std(df, F.col("full_name"), F.col("pan"), F.col("mobile"), None, F.col("address"), F.col("pincode"))
            .withColumn("borrower_key", F.concat(F.lit("BRW:LMS:"), "borrower_id")))


# ---------------------------------------------------------------- CRM


def crm_state(ctx: Ctx) -> None:
    raw = ctx.bronze("crm_customer")
    if raw is None:
        return
    rows_in = raw.count()
    if rows_in == 0:
        return
    j = lambda path: F.get_json_object("payload", f"$.{path}")  # noqa: E731
    ids = F.from_json(j("identifiers"), "array<struct<type:string,value:string>>")
    ident = lambda kind: F.element_at(F.transform(F.filter(ids, lambda x: x["type"] == kind),  # noqa: E731
                                                  lambda x: x["value"]), 1)
    dob_def = next(c for c in ctx.contracts["crm_customer"]["columns"] if c["name"] == "date_of_birth")
    df = raw.select(
        F.col("crm_id"), j("source_channel").alias("source_channel"), F.col("full_name"),
        C.typed(F.col("date_of_birth"), dob_def).alias("dob"), F.upper(j("profile.gender")).substr(1, 1).alias("gender"),
        ident("PAN").alias("pan"), F.coalesce(ident("MOBILE"), j("contacts.mobile")).alias("mobile"),
        F.col("email"), j("addresses[0].line1").alias("addr_line1"), j("addresses[0].line2").alias("addr_line2"),
        j("addresses[0].city").alias("city"), j("addresses[0].pincode").alias("pincode"),
        F.to_date(j("addresses[0].updated_on")).alias("address_updated_on"),
        (j("consent.marketing") == "true").alias("consent_marketing"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        *lineage("crm", F.col("crm_id"), F.lit("extract"), F.col("updated_at").cast("timestamp"), file_position()))
    df = latest_per_key(df, ["crm_id"], F.desc("_src_ts"))
    df = person_std(df, F.col("full_name"), F.col("pan"), F.col("mobile"), F.col("email"),
                    F.concat_ws(" ", "addr_line1", "addr_line2"), F.col("pincode"))
    apply_state(ctx, "crm_customer", df, ["crm_id"], rows_in)
    log(ctx, "crm_customer", "normalise", "crm_customer", rows_in, rows_in,
        "nested JSON flattened: identifiers by type, first address, contacts, consent")
    log(ctx, "crm_customer", "standardise", "crm_customer", rows_in, rows_in, PERSON_STD)


# ---------------------------------------------------------------- daily tables


def src_cols() -> list:
    return [F.col("_batch_id").alias("_src_batch_id"), file_position().alias("_src_position"),
            F.col("_record_hash").alias("_src_record_hash")]


def eod_balance(ctx: Ctx) -> None:
    raw = ctx.bronze("cbs_eod_balance")
    if raw is None:
        return
    contract = ctx.contracts["cbs_eod_balance"]
    rows_in = raw.count()
    df = raw.select(*C.typed_columns(contract), "_source_row", *src_cols())
    df = latest_per_key(df, ["acct_no", "bal_date"], "_source_row").drop("_source_row")
    df = (df.join(F.broadcast(C.fx_rates(ctx.spark)), "currency", "left")
          .withColumn("account_key", F.concat(F.lit("ACC:CBS:"), "acct_no"))
          .withColumn("ledger_balance_inr", F.round(F.col("ledger_balance") * F.col("fx_rate_inr"), 2).cast(C.DECIMAL))
          .withColumn("available_balance_inr",
                      F.round(F.col("available_balance") * F.col("fx_rate_inr"), 2).cast(C.DECIMAL)))
    n = df.count()
    replace_date(ctx, "cbs_eod_balance", df, rows_in, f"{rows_in - n} duplicate rows removed")
    log(ctx, "cbs_eod_balance", "deduplicate", "cbs_eod_balance", rows_in, n, "one row per account and date")
    log(ctx, "cbs_eod_balance", "enrich", "cbs_eod_balance", n, n, "balances in INR at config/pipeline.json rates")


def loan_daily(ctx: Ctx) -> None:
    raw = ctx.bronze("lms_loan")
    if raw is None:
        return
    rows_in = raw.count()
    df = raw.select(*C.typed_columns(ctx.contracts["lms_loan"]), "_source_row", *src_cols())
    df = latest_per_key(df, ["loan_id"], "_source_row").drop("_source_row")
    df = (df.withColumn("loan_key", F.concat(F.lit("LN:LMS:"), "loan_id"))
          .withColumn("borrower_key", F.concat(F.lit("BRW:LMS:"), "borrower_id")))
    replace_date(ctx, "lms_loan_daily", df, rows_in)
    log(ctx, "lms_loan_daily", "cleanse", "lms_loan", rows_in, rows_in, "typed; dd/MM/yyyy dates parsed")


def repayments(ctx: Ctx) -> None:
    raw = ctx.bronze("lms_repayment")
    if raw is None:
        return
    rows_in = raw.count()
    df = raw.select(*C.typed_columns(ctx.contracts["lms_repayment"]), "_source_row", *src_cols())
    df = latest_per_key(df, ["txn_id"], "_source_row").drop("_source_row")
    df = df.withColumn("loan_key", F.concat(F.lit("LN:LMS:"), "loan_id"))
    replace_date(ctx, "lms_repayment", df, rows_in)
    log(ctx, "lms_repayment", "cleanse", "lms_repayment", rows_in, rows_in, "typed; dd/MM/yyyy dates parsed")


def payments(ctx: Ctx) -> None:
    raw = ctx.bronze("pay_transaction")
    if raw is None:
        return
    rows_in = raw.count()
    own = CFG["bank"]["ifsc_prefix"]
    j = lambda path: F.get_json_object("payload", f"$.{path}")  # noqa: E731
    charges = F.from_json(j("charges"), "array<struct<type:string,amount:string,ccy:string>>")
    charge = lambda kind: F.aggregate(F.filter(charges, lambda x: x["type"] == kind), F.lit(0.0),  # noqa: E731
                                      lambda acc, x: acc + x["amount"].cast("double"))
    dr = F.col("direction") == "DR"
    df = raw.select(
        "msg_id", j("end_to_end_id").alias("end_to_end_id"), F.col("created_at").cast("timestamp").alias("created_at"),
        F.to_date("value_date").alias("value_date"), "channel", "direction", "status",
        F.col("amount").cast(C.DECIMAL).alias("amount"), "currency",
        j("debtor.name").alias("debtor_name"), F.col("debtor_account"), j("debtor.account.ifsc").alias("debtor_ifsc"),
        j("creditor.name").alias("creditor_name"), F.col("creditor_account"),
        j("creditor.account.ifsc").alias("creditor_ifsc"),
        j("remittance.purpose").alias("purpose_code"), j("remittance.narrative").alias("narrative"),
        F.round(charge("TXN_FEE"), 2).cast(C.DECIMAL).alias("fee_amount"),
        F.round(charge("GST"), 2).cast(C.DECIMAL).alias("gst_amount"),
        j("device.id").alias("device_id"), j("device.os").alias("device_os"),
        "_source_row", *src_cols())
    df = latest_per_key(df, ["msg_id"], "_source_row").drop("_source_row")
    n = df.count()
    df = (df.join(F.broadcast(C.fx_rates(ctx.spark)), "currency", "left")
          .withColumn("amount_inr", F.round(F.col("amount") * F.col("fx_rate_inr"), 2).cast(C.DECIMAL))
          .withColumn("own_account", F.when(dr, F.col("debtor_account")).otherwise(F.col("creditor_account")))
          .withColumn("own_ifsc", F.when(dr, F.col("debtor_ifsc")).otherwise(F.col("creditor_ifsc")))
          .withColumn("counterparty_name", F.when(dr, F.col("creditor_name")).otherwise(F.col("debtor_name")))
          .withColumn("counterparty_account", F.when(dr, F.col("creditor_account")).otherwise(F.col("debtor_account")))
          .withColumn("counterparty_ifsc", F.when(dr, F.col("creditor_ifsc")).otherwise(F.col("debtor_ifsc")))
          .withColumn("counterparty_bank", F.substring("counterparty_ifsc", 1, 4))
          .withColumn("is_on_us", F.col("counterparty_bank") == own)
          .withColumn("account_key", F.when(F.substring("own_ifsc", 1, 4) == own,
                                            F.concat(F.lit("ACC:CBS:"), "own_account"))))
    replace_date(ctx, "pay_transaction", df, rows_in, f"{rows_in - n} re-sent messages removed")
    log(ctx, "pay_transaction", "normalise", "pay_transaction", rows_in, rows_in,
        "nested JSON flattened: debtor, creditor, amount, remittance, charges[], device (new from D4)")
    log(ctx, "pay_transaction", "deduplicate", "pay_transaction", rows_in, n, "one row per msg_id")
    log(ctx, "pay_transaction", "enrich", "pay_transaction", n, n,
        "amount in INR; own vs counterparty side by direction; counterparty bank from IFSC")


def documents(ctx: Ctx) -> None:
    raw = ctx.bronze("doc_document")
    if raw is None:
        return
    rows_in = raw.count()
    text = F.coalesce(F.col("text_content"), F.lit(""))
    rx = lambda pattern: nz(F.regexp_extract(text, pattern, 1))  # noqa: E731
    is_eml, is_kyc, is_scan = (F.col("doc_type") == t for t in ("EML", "KYC", "SCAN"))
    subject = rx(r"(?m)^Subject: (.*)$")
    sent = F.to_timestamp(rx(r"(?m)^Date: \w{3}, (.+)$"), "d MMM yyyy HH:mm:ss Z")
    intent = (F.when(is_scan, "KYC_SCAN").when(F.trim(text) == "", "UNREADABLE")
              .when(is_kyc, "KYC_DECLARATION")
              .when(F.lower(subject).contains("address"), "ADDRESS_CHANGE")
              .when(F.lower(subject).contains("complaint"), "COMPLAINT").otherwise("OTHER"))
    df = raw.select(
        "file_name", "doc_type", "mime_type", F.col("bytes").cast("bigint").alias("bytes"), "sha256",
        intent.alias("intent"), subject.alias("subject"),
        F.when(is_eml, rx(r'(?m)^From: "([^"]+)"')).when(is_kyc, rx(r"(?m)^Name: (.*)$")).alias("party_name"),
        F.when(is_eml, rx(r"(?m)^From: .*<([^>]+)>")).alias("sender_email"),
        F.when(is_eml, sent).when(is_kyc, F.to_timestamp(rx(r"Signed on (\d{4}-\d{2}-\d{2})"))).alias("document_ts"),
        rx(r"Customer ID: (\d+)").cast("int").alias("cust_id"),
        rx(r"account (\d{12,16})").alias("acct_no"),
        F.coalesce(rx(r"(?m)^PAN: (\S+)"), rx(r"PAN is ([A-Z]{5}[0-9]{4}[A-Z])")).alias("pan"),
        F.coalesce(rx(r"(?m)^Mobile: (\S+)"), rx(r"mobile is (\d{10})")).alias("mobile"),
        F.to_date(rx(r"Date of birth: (\d{2}/\d{2}/\d{4})"), "dd/MM/yyyy").alias("dob"),
        F.coalesce(rx(r"(?m)^Address: (.*)$"), rx(r"(?s)to:\n(.+?)\.\n")).alias("address_text"),
        F.when(is_scan, F.regexp_replace(F.regexp_replace("file_name", "^SCAN-", "KYC-"), r"\.png$", ".txt"))
        .alias("kyc_ref"),
        F.when(is_scan, F.conv(F.hex(F.substring("content", 17, 4)), 16, 10).cast("int")).alias("image_width"),
        F.when(is_scan, F.conv(F.hex(F.substring("content", 21, 4)), 16, 10).cast("int")).alias("image_height"),
        F.col("_batch_id").alias("_src_batch_id"), F.col("_source_file").alias("_src_position"),
        F.col("_record_hash").alias("_src_record_hash"))
    df = (df.withColumn("pan_std", C.std_pan("pan")).withColumn("mobile_e164", C.std_mobile("mobile"))
          .withColumn("name_std", C.std_name("party_name")).withColumn("address_std", C.std_address("address_text"))
          .withColumn("pincode_std", C.std_pincode("address_text"))
          .withColumn("account_key", F.when(F.col("acct_no").isNotNull(), F.concat(F.lit("ACC:CBS:"), "acct_no")))
          .withColumn("extraction_status",
                      F.when(F.col("intent") == "UNREADABLE", "EMPTY")
                      .when(F.col("intent") == "KYC_SCAN", F.when(F.col("image_width") > 0, "OK").otherwise("BAD_IMAGE"))
                      .when(F.coalesce("cust_id", "acct_no", "pan_std", "mobile_e164").isNull(), "NO_IDENTIFIER")
                      .otherwise("OK")))
    replace_date(ctx, "doc_extract", df, rows_in)
    log(ctx, "doc_extract", "enrich", "doc_document", rows_in, rows_in,
        "intent, customer id, account, PAN, mobile, DOB and address read from e-mails and KYC text; "
        "scan size from the PNG header")
    log(ctx, "doc_extract", "standardise", "doc_document", rows_in, rows_in, PERSON_STD)


def write_exceptions(ctx: Ctx) -> None:
    target = ctx.names.t(STAGE, "cdc_exception")
    C.ensure_table(ctx.spark, target, EXCEPTION_SCHEMA, ["business_date"])
    ctx.spark.sql(f"DELETE FROM {target} WHERE business_date = DATE '{ctx.d.isoformat()}'")
    if ctx.exceptions:
        ctx.spark.createDataFrame(ctx.exceptions, EXCEPTION_SCHEMA).writeTo(target).append()
        kinds: dict = {}
        for r in ctx.exceptions:
            kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
        print("cdc_exception: " + ", ".join(f"{k}={v}" for k, v in sorted(kinds.items())), flush=True)


# ---------------------------------------------------------------- run


def run(spark, argv=None) -> dict:
    args = C.parse(C.base_parser(__doc__), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    C.require_completed(spark, names, "bronze", args.business_date)
    audit = C.Audit(spark, names, "build_silver", args.business_date, args.pipeline_run)
    ctx = Ctx(spark, names, audit, args.business_date)
    audit.load(STAGE, "*", "STARTED")
    day_start = F.lit(f"{args.business_date.isoformat()} 00:00:00").cast("timestamp")
    try:
        simple_state(ctx, "cbs_branch", "cbs", day_start)
        simple_state(ctx, "cbs_product", "cbs", day_start)
        cbs_state(ctx, "cbs_customer", "customer", customer_extra)
        log(ctx, "cbs_customer", "standardise", "cbs_customer", None, None, PERSON_STD)
        cbs_state(ctx, "cbs_account", "account", account_extra)
        eod_balance(ctx)
        simple_state(ctx, "lms_borrower", "lms", F.col("_t_updated_on").cast("timestamp"), borrower_extra)
        log(ctx, "lms_borrower", "standardise", "lms_borrower", None, None, PERSON_STD)
        loan_daily(ctx)
        repayments(ctx)
        payments(ctx)
        crm_state(ctx)
        documents(ctx)
        write_exceptions(ctx)
        audit.load(STAGE, "*", "COMPLETED", rows_in=sum(s["rows_in"] for s in ctx.summary.values()),
                   rows_out=sum(s["rows_out"] for s in ctx.summary.values()))
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return ctx.summary


def main() -> int:
    spark = C.get_spark("gdl-build-silver")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
