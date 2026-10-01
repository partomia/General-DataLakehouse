"""
Stage 1 - Bronze: ingest one business date's landed files as received, with record validation.

For every entity of contracts/ (in config/pipeline.json order) that the date's
manifests list:

  parse      mysqldump INSERTs (column order from CREATE TABLE), pipe-delimited
             extracts (columns by header, trailer read), JSON lines, a JSON array
             of documents, or binary files (e-mails, KYC text, scans)
  validate   every record against its contract: mandatory, type (int, decimal,
             date in the source's own format, timestamp), pattern, allowed values,
             minimum; CDC events are also checked against the contract of the
             table they change (the after-image of c/u, the key of d)
  split      records with a `reject` finding -> bronze.quarantine with reason codes;
             the rest -> bronze.<entity> (all values as strings, plus `_warnings`)
  metadata   _batch_id, _business_date, _source_system, _source_file, _source_row,
             _ingested_at, _record_hash on every row
  drift      JSON fields, CSV header columns or dump columns the contract does not
             know -> ref.schema_drift; CSV trailers -> ref.file_control

Each entity is written in its own Iceberg commit that replaces this business
date's partition (overwritePartitions), so a re-run of a date never duplicates
rows. ref.load_audit gets STARTED, one COMMITTED per entity (rows in / accepted /
rejected, snapshot before and after) and COMPLETED, or FAILED with the error.

Failure drill (docs/FAILED_BATCH_DEMO.md):
  --fail-after ENTITY    commit ENTITY, then fail the batch: later entities are not loaded
  --fail-during ENTITY   one write task of ENTITY fails: Spark aborts, nothing of ENTITY commits
  --resume               skip entities already COMMITTED since the date's last COMPLETED run

Usage:
  spark-submit ingest_bronze.py --business-date 2026-09-21 [--landing URI] [--db-prefix P]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gdl_common as C  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "bronze"
DRIFT_SCHEMA = ("batch_id string, business_date date, entity string, source_file string, kind string, "
                "field string, records bigint, detected_at timestamp")
FILE_CONTROL_SCHEMA = ("batch_id string, business_date date, source string, entity string, source_file string, "
                       "header string, data_records bigint, trailer_records bigint, trailer_total double, "
                       "logged_at timestamp")
QUARANTINE_COLS = ["_entity", "_batch_id", "_business_date", "_source_system", "_source_file", "_source_row",
                   "_reject_reasons", "_warnings", "_record", "_record_hash", "_ingested_at"]


def parser():
    p = C.base_parser(__doc__)
    p.add_argument("--fail-after", default=None, help="entity: commit it, then fail the batch")
    p.add_argument("--fail-during", default=None, help="entity: fail one of its write tasks")
    p.add_argument("--resume", action="store_true", help="skip entities committed since the last COMPLETED run")
    p.add_argument("--mode", default="normal",
                   help="the same as one value (an Airflow template fills it): normal, resume, "
                        "fail-during:<entity> or fail-after:<entity>")
    return p


def apply_mode(args):
    mode, _, entity = args.mode.partition(":")
    if mode == "resume":
        args.resume = True
    elif mode in ("fail-during", "fail-after") and entity:
        setattr(args, mode.replace("-", "_"), entity)
    elif mode != "normal":
        raise SystemExit(f"--mode {args.mode}: use normal, resume, fail-during:<entity> or fail-after:<entity>")
    return args


# ---------------------------------------------------------------- parse


def _json_cols(df: DataFrame, contract: dict) -> DataFrame:
    for c in contract["columns"]:
        df = df.withColumn(c["name"], F.get_json_object("payload", f"$.{c['path']}"))
    return df


def unread_files(spark, df: DataFrame, manifest_files: list) -> DataFrame | None:
    """Manifest files the binary reader did not return (Spark skips zero-byte files), as rows."""
    read = {r[0] for r in df.select("_source_file").collect()}
    missing = [f for f in manifest_files if f["file"] not in read and not f["file"].startswith("_")]
    if not missing:
        return None
    rows = [(f["file"], 1, f["file"], f["file"].split("-")[0], str(f["bytes"]), f["sha256"], None, None, None)
            for f in missing]
    return spark.createDataFrame(rows, "_source_file string, _source_row int, file_name string, doc_type string, "
                                       "bytes string, sha256 string, mime_type string, text_content string, "
                                       "content binary")


def raw_frame(spark, contract: dict, folder: str, manifest_files=()) -> tuple[DataFrame, list]:
    """The entity's records as strings, with _source_file / _source_row; and file stats (CSV trailers)."""
    fmt, cols = contract["format"], [c["name"] for c in contract["columns"]]
    files = spark.sparkContext.wholeTextFiles(f"{folder}/{contract['file_glob']}") if fmt != "binary" else None
    col_schema = ", ".join(f"`{c}` string" for c in cols)
    stats = []
    if fmt == "mysqldump":
        table = contract["table"]
        rdd = files.flatMap(lambda kv, t=table, cs=cols: C.parse_dump(kv[0], kv[1], t, cs))
        df = spark.createDataFrame(rdd, f"_source_file string, _source_row int, {col_schema}")
        stats = files.map(lambda kv, t=table: (C._basename(kv[0]), "|".join(C.dump_columns(kv[1], t)))).collect()
    elif fmt == "psv":
        rdd = files.flatMap(lambda kv, cs=cols: C.parse_psv(kv[0], kv[1], cs))
        df = spark.createDataFrame(rdd, f"_source_file string, _source_row int, {col_schema}")
        stats = files.map(lambda kv: C.psv_stats(kv[0], kv[1])).collect()
    elif fmt in ("jsonl", "json_array"):
        parse = C.parse_jsonl if fmt == "jsonl" else C.parse_json_array
        rdd = files.flatMap(lambda kv, f=parse: f(kv[0], kv[1]))
        df = _json_cols(spark.createDataFrame(
            rdd, "_source_file string, _source_row int, payload string, _parse_error string"), contract)
    elif fmt == "binary":
        bin_df = spark.read.format("binaryFile").load(folder)
        name = F.element_at(F.split("path", "/"), -1)
        ext = F.lower(F.element_at(F.split(name, "[.]"), -1))
        df = (bin_df.withColumn("_source_file", name).where(~F.col("_source_file").startswith("_"))
              .select("_source_file", F.lit(1).alias("_source_row"),
                      F.col("_source_file").alias("file_name"),
                      F.element_at(F.split("_source_file", "-"), 1).alias("doc_type"),
                      F.col("length").cast("string").alias("bytes"),
                      F.sha2("content", 256).alias("sha256"),
                      F.when(ext == "eml", "message/rfc822").when(ext == "txt", "text/plain")
                      .when(ext == "png", "image/png").otherwise("application/octet-stream").alias("mime_type"),
                      F.when(ext.isin("eml", "txt"), F.decode("content", "UTF-8")).alias("text_content"),
                      "content"))
        extra = unread_files(spark, df, list(manifest_files))
        if extra is not None:
            df = df.unionByName(extra)
    else:
        raise ValueError(f"unknown format {fmt}")
    return df, stats


# ---------------------------------------------------------------- validate


def _checks(columns: list, value_of, prefix: str = "", guard=None) -> tuple[list, list]:
    rejects, warns = [], []
    for c in columns:
        v = F.trim(value_of(c))
        blank = v.isNull() | (v == "")
        present = ~blank
        name = prefix + c["name"]

        def add(cond, code, severity):
            cond = cond if guard is None else (guard & cond)
            (rejects if severity == "reject" else warns).append(F.when(cond, F.lit(f"{name}:{code}")))

        if c.get("required"):
            add(blank, "REQUIRED", "reject")
        kind = c["type"]
        if kind in ("int", "bigint"):
            add(present & v.cast("bigint").isNull(), "NOT_INTEGER", "reject")
        elif kind == "decimal":
            add(present & v.cast("decimal(20,4)").isNull(), "NOT_DECIMAL", "reject")
        elif kind == "date":
            parsed = F.coalesce(*[F.to_date(v, f) for f in (c.get("formats") or [c["format"]])])
            add(present & parsed.isNull(), "INVALID_DATE", "reject")
        elif kind == "timestamp":
            add(present & F.to_timestamp(v, c["format"]).isNull(), "INVALID_TIMESTAMP", "reject")
        severity = c.get("severity", "warn")
        if "pattern" in c:
            add(present & ~v.rlike(c["pattern"]), "PATTERN", severity)
        if "allowed" in c:
            add(present & ~v.isin(c["allowed"]), "NOT_ALLOWED", severity)
        if "min" in c:
            add(present & (v.cast("double") < c["min"]), "BELOW_MIN", c.get("severity", "reject"))
    return rejects, warns


def _compact(exprs: list):
    if not exprs:
        return F.array().cast("array<string>")
    return F.filter(F.array(*exprs), lambda x: x.isNotNull())


def validate(df: DataFrame, contract: dict, contracts: dict) -> DataFrame:
    rejects, warns = _checks(contract["columns"], lambda c: F.col(c["name"]))
    if "payload" in df.columns:
        rejects.append(F.when(F.col("_parse_error").isNotNull(), F.lit("payload:MALFORMED_JSON")))
    if "content" in df.columns:
        rejects.append(F.when(F.col("content").isNull() & (F.col("bytes").cast("bigint") > 0),
                              F.lit("content:NOT_READ")))
    for table, image in contract.get("image_contracts", {}).items():
        img = contracts[image]
        is_table = F.col("table") == table
        r, w = _checks(img["columns"], lambda c: F.get_json_object("payload", f"$.after.{c['name']}"),
                       prefix="after.", guard=is_table & F.col("op").isin("c", "u"))
        rejects += r
        warns += w
        for key in img["key"]:
            before_key = F.get_json_object("payload", f"$.before.{key}")
            rejects.append(F.when(is_table & (F.col("op") == "d") & before_key.isNull(), F.lit(f"before.{key}:REQUIRED")))
    from pyspark.sql import Window
    key_window = Window.partitionBy(*contract["key"])
    warns.append(F.when(F.count(F.lit(1)).over(key_window) > 1, F.lit("key:DUPLICATE_IN_BATCH")))
    return df.withColumn("_reject_reasons", _compact(rejects)).withColumn("_warnings", _compact(warns))


def record_hash(df: DataFrame, contract: dict):
    if "payload" in df.columns:
        return F.sha2(F.col("payload"), 256)
    if "content" in df.columns:
        return F.col("sha256")
    cols = [F.coalesce(F.col(c["name"]), F.lit("\u0000")) for c in contract["columns"]]
    return F.sha2(F.concat_ws("\u0001", *cols), 256)


def record_json(df: DataFrame, contract: dict):
    if "payload" in df.columns:
        return F.col("payload")
    return F.to_json(F.struct(*[F.col(c["name"]) for c in contract["columns"]]))


# ---------------------------------------------------------------- write


def replace_rows(spark, table: str, schema: str, where: str, rows: list) -> None:
    C.ensure_table(spark, table, schema)
    spark.sql(f"DELETE FROM {table} WHERE {where}")
    if rows:
        spark.createDataFrame(rows, schema).writeTo(table).append()


def write_batch(spark, df: DataFrame, table: str, business_date, partition_cols, fail: bool = False) -> None:
    """Replace this date's rows of `table` with df in one commit (or remove them when df is empty)."""
    if fail:
        boom = F.udf(lambda pid: C.fail_on_partition(pid), "int")
        C.write_partitions(df.repartition(4).where(boom(F.spark_partition_id()) >= 0), table, partition_cols)
        return
    if df.isEmpty():
        if C.table_exists(spark, table):
            spark.sql(f"DELETE FROM {table} WHERE _business_date = DATE '{business_date.isoformat()}'")
        return
    C.write_partitions(df, table, partition_cols)


def drift_rows(spark, df: DataFrame, contract: dict, stats: list, bid: str, business_date, now) -> list:
    rows = []
    known = set(contract.get("known_keys", []))
    if known and "payload" in df.columns:
        keys = (df.where(F.col("_parse_error").isNull())
                .select("_source_file", F.explode(F.expr("json_object_keys(payload)")).alias("k"))
                .where(~F.col("k").isin(*known)).groupBy("_source_file", "k").count().collect())
        rows += [(bid, business_date, contract["entity"], r["_source_file"], "NEW_FIELD", r["k"], r["count"], now)
                 for r in keys]
    expected = [c["name"] for c in contract["columns"]]
    for st in stats:
        header = st[1].split("|") if st[1] else []
        for col in header:
            if col not in expected:
                rows.append((bid, business_date, contract["entity"], st[0], "NEW_COLUMN", col, None, now))
        for col in expected:
            if header and col not in header:
                rows.append((bid, business_date, contract["entity"], st[0], "MISSING_COLUMN", col, None, now))
    return rows


def committed_since_complete(spark, names: C.Names, bid: str) -> set:
    t = names.t("ref", "load_audit")
    if not C.table_exists(spark, t):
        return set()
    scope = f"batch_id = '{bid}' AND stage = '{STAGE}'"
    return {r[0] for r in spark.sql(f"""
        SELECT DISTINCT entity FROM {t}
        WHERE {scope} AND status = 'COMMITTED'
          AND started_at > coalesce((SELECT max(started_at) FROM {t}
                                     WHERE {scope} AND entity = '*' AND status = 'COMPLETED'),
                                    TIMESTAMP '1970-01-01 00:00:00')""").collect()}


def ingest_entity(spark, names, audit, contract, contracts, folder, manifest_entity, args, manifest_files=()) -> dict:
    entity, d, bid = contract["entity"], args.business_date, C.batch_id(args.business_date)
    started = C._now()
    target, quarantine = names.t("bronze", entity), names.t("bronze", "quarantine")
    before = C.snapshot_id(spark, target)
    raw, stats = raw_frame(spark, contract, folder, manifest_files)
    meta = [F.lit(bid).alias("_batch_id"), F.lit(d).cast("date").alias("_business_date"),
            F.lit(contract["source"]).alias("_source_system"), "_source_file", "_source_row",
            F.current_timestamp().alias("_ingested_at")]
    checked = validate(raw, contract, contracts).withColumn("_record_hash", record_hash(raw, contract))
    checked = checked.withColumn("_record", record_json(raw, contract)).cache()
    data_cols = [c["name"] for c in contract["columns"]] + [c for c in ("payload", "mime_type", "text_content", "content")
                                                             if c in raw.columns]
    good = checked.where(F.size("_reject_reasons") == 0).select(*meta, *data_cols, "_warnings", "_record_hash")
    bad = checked.where(F.size("_reject_reasons") > 0).select(
        F.lit(entity).alias("_entity"), *meta, "_reject_reasons", "_warnings", "_record", "_record_hash")
    bad = bad.select(*QUARANTINE_COLS)
    rows_in = checked.count()
    n_bad = bad.count()
    n_good = rows_in - n_bad

    write_batch(spark, good, target, d, ["_business_date"], fail=args.fail_during == entity)
    if n_bad:
        C.write_partitions(bad, quarantine, ["_business_date", "_entity"])
    elif C.table_exists(spark, quarantine):
        spark.sql(f"DELETE FROM {quarantine} WHERE _business_date = DATE '{d.isoformat()}' AND _entity = '{entity}'")
    now = C._now()
    replace_rows(spark, names.t("ref", "schema_drift"), DRIFT_SCHEMA,
                 f"batch_id = '{bid}' AND entity = '{entity}'", drift_rows(spark, raw, contract, stats, bid, d, now))
    if contract["format"] == "psv":
        replace_rows(spark, names.t("ref", "file_control"), FILE_CONTROL_SCHEMA,
                     f"batch_id = '{bid}' AND entity = '{entity}'",
                     [(bid, d, contract["source"], entity, s[0], s[1], s[2], s[3], s[4], now) for s in stats])
    reasons = (checked.where(F.size("_reject_reasons") > 0).select(F.explode("_reject_reasons").alias("r"))
               .groupBy("r").count().orderBy(F.desc("count")).limit(5).collect())
    msg = ", ".join(f"{r['r']}={r['count']}" for r in reasons)
    checked.unpersist()
    after = C.snapshot_id(spark, target)
    audit.load(STAGE, entity, "COMMITTED", rows_in=rows_in, rows_out=n_good, rows_rejected=n_bad,
               snapshot_before=before, snapshot_after=after, started_at=started,
               message=f"manifest {manifest_entity['records']}; rejected: {msg or 'none'}")
    audit.transform(f"ingest {entity}", "ingest", f"landing:{contract['source']}/{d}/{contract['file_glob']}",
                    target, rows_in, n_good, f"format {contract['format']}")
    audit.transform(f"validate {entity}", "validate", target, quarantine, rows_in, n_bad, msg or "no rejects")
    print(f"{entity}: {rows_in} in, {n_good} accepted, {n_bad} quarantined ({msg or 'no rejects'})", flush=True)
    return {"rows_in": rows_in, "accepted": n_good, "rejected": n_bad}


def run(spark, argv=None) -> dict:
    args = apply_mode(C.parse(parser(), argv))
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    contracts = C.contracts()
    d, bid = args.business_date, C.batch_id(args.business_date)
    landing = C.as_uri(args.landing)
    audit = C.Audit(spark, names, "ingest_bronze", d, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED", message=f"landing {landing}")
    manifests = C.read_manifests(spark, landing, d)
    skip = committed_since_complete(spark, names, bid) if args.resume else set()
    summary = {}
    try:
        for source, m in manifests.items():
            if m is None:
                audit.load(STAGE, f"{source}:*", "MISSING_MANIFEST", message=f"no {source} manifest for {d}")
        for entity, contract in contracts.items():
            m = manifests.get(contract["source"])
            listed = next((e for e in (m or {}).get("entities", []) if e["entity"] == entity), None)
            if listed is None:
                continue
            if entity in skip:
                print(f"{entity}: already committed for {bid}, skipped (--resume)")
                audit.load(STAGE, entity, "SKIPPED", message="committed by an earlier attempt (--resume)")
                continue
            folder = f"{landing}/{contract['source']}/{d.isoformat()}"
            summary[entity] = ingest_entity(spark, names, audit, contract, contracts, folder, listed, args,
                                            m.get("files", []))
            if args.fail_after == entity:
                raise RuntimeError(f"simulated failure after committing {entity} (--fail-after)")
        audit.load(STAGE, "*", "COMPLETED", rows_in=sum(s["rows_in"] for s in summary.values()),
                   rows_out=sum(s["accepted"] for s in summary.values()),
                   rows_rejected=sum(s["rejected"] for s in summary.values()))
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return summary


def main() -> int:
    spark = C.get_spark("gdl-ingest-bronze")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
