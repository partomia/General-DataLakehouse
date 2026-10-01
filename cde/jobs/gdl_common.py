"""
Shared helpers for the CDE Spark jobs (driver side, plus the few functions
executors call). Standard library and PySpark only, so the jobs need no
python-env resource on CDE.

  names        database and table names: <prefix>_<layer>.<table>
  contracts    the per-entity record contracts in contracts/*.json
  spark        session settings every job relies on (ANSI off: a bad value
               casts to NULL and is reported, it never kills a load)
  iceberg      one atomic commit per write: overwritePartitions for a batch,
               createOrReplace for a rebuilt table, snapshot ids for lineage
  audit        ref.load_audit (batch x stage x entity status) and
               ref.transform_log (one row per transformation step)
  fs           write files to the landing zone through Hadoop (s3a:// or file://)

On CDE the repository is mounted at /app/mount, so contracts and config are
read relative to this file there and on a laptop alike.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

DEFAULT_PREFIX = "rsingh_gdl"
DEFAULT_LANDING = "s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing"
DEFAULT_REPORTS = "s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/reports"
LAYERS = ("bronze", "silver", "mdm", "gold", "semantic", "ref")
SOURCES = ("cbs", "lms", "payments", "crm", "documents")

LOAD_AUDIT_SCHEMA = (
    "run_id string, pipeline_run string, batch_id string, business_date date, stage string, "
    "entity string, status string, rows_in bigint, rows_out bigint, rows_rejected bigint, "
    "snapshot_before bigint, snapshot_after bigint, started_at timestamp, ended_at timestamp, "
    "message string")
TRANSFORM_LOG_SCHEMA = (
    "run_id string, pipeline_run string, batch_id string, business_date date, job string, "
    "step string, transform_type string, source_tables string, target_table string, "
    "rows_in bigint, rows_out bigint, snapshot_id bigint, logged_at timestamp, details string")
TRANSFORM_TYPES = ("ingest", "validate", "cleanse", "standardise", "deduplicate", "match", "survive",
                   "enrich", "normalise", "aggregate", "historise", "consume")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_json(rel: str):
    return json.loads((repo_root() / rel).read_text())


def contracts() -> dict:
    """entity -> contract, in the ingest order of config/pipeline.json."""
    order = load_json("config/pipeline.json")["entities"]
    found = {p.stem: json.loads(p.read_text()) for p in (repo_root() / "contracts").glob("*.json")}
    missing = [e for e in order if e not in found]
    if missing:
        raise RuntimeError(f"no contract for {missing}")
    return {e: found[e] for e in order}


def batch_id(business_date: date) -> str:
    return f"B{business_date:%Y%m%d}"


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def base_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--db-prefix", default=DEFAULT_PREFIX)
    p.add_argument("--business-date", type=parse_date, required=True)
    p.add_argument("--landing", default=DEFAULT_LANDING)
    p.add_argument("--pipeline-run", default=None, help="groups the stages of one DAG run (Airflow run_id)")
    return p


def parse(parser: argparse.ArgumentParser, argv=None) -> argparse.Namespace:
    args, _ = parser.parse_known_args(argv)
    return args


class Names:
    def __init__(self, prefix: str = DEFAULT_PREFIX):
        self.prefix = prefix

    def db(self, layer: str) -> str:
        assert layer in LAYERS, layer
        return f"{self.prefix}_{layer}"

    def t(self, layer: str, name: str) -> str:
        return f"{self.db(layer)}.{name}"


# ---------------------------------------------------------------- spark


def configure(spark) -> None:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")   # 1985-02-30 -> NULL, not an error
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        spark.sparkContext.addPyFile(str(Path(__file__).resolve()))
    except Exception:  # already shipped in this session
        pass


def get_spark(app: str):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(app).getOrCreate()
    configure(spark)
    return spark


def catalog(spark) -> str:
    return spark.conf.get("spark.sql.defaultCatalog", "spark_catalog")


def ensure_databases(spark, names: Names) -> None:
    for layer in LAYERS:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {names.db(layer)}")


# ---------------------------------------------------------------- iceberg


def table_exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


def snapshot_id(spark, table: str) -> int | None:
    if not table_exists(spark, table):
        return None
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(rows[0][0]) if rows else None


def _create(df, table: str, partition_cols=()):
    from pyspark.sql import functions as F

    w = df.writeTo(table).using("iceberg").tableProperty("format-version", "2")
    if partition_cols:
        w = w.partitionedBy(*[F.col(c) for c in partition_cols])
    return w


def write_partitions(df, table: str, partition_cols) -> None:
    """Replace exactly the partitions present in df, in one commit (a re-run of a batch)."""
    if table_exists(df.sparkSession, table):
        df.writeTo(table).overwritePartitions()
    else:
        _create(df, table, partition_cols).create()


def replace_table(df, table: str, partition_cols=()) -> None:
    _create(df, table, partition_cols).createOrReplace()


def append(df, table: str, partition_cols=()) -> None:
    if table_exists(df.sparkSession, table):
        df.writeTo(table).append()
    else:
        _create(df, table, partition_cols).create()


def ensure_table(spark, table: str, schema: str, partition_cols=()) -> None:
    if not table_exists(spark, table):
        _create(spark.createDataFrame([], schema), table, partition_cols).create()


# ---------------------------------------------------------------- audit


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Audit:
    """Writes ref.load_audit rows as they happen (a failed run keeps what it logged) and
    buffers ref.transform_log rows until flush()."""

    def __init__(self, spark, names: Names, job: str, business_date: date, pipeline_run: str | None):
        self.spark, self.names, self.job = spark, names, job
        self.business_date, self.batch = business_date, batch_id(business_date)
        self.run_id = f"{job}-{uuid.uuid4().hex[:12]}"
        self.pipeline_run = pipeline_run or os.environ.get("GDL_PIPELINE_RUN") or self.run_id
        self._steps: list[tuple] = []
        ensure_table(spark, names.t("ref", "load_audit"), LOAD_AUDIT_SCHEMA)
        ensure_table(spark, names.t("ref", "transform_log"), TRANSFORM_LOG_SCHEMA)

    def load(self, stage: str, entity: str, status: str, *, rows_in=None, rows_out=None, rows_rejected=None,
             snapshot_before=None, snapshot_after=None, started_at=None, message: str = "") -> None:
        row = (self.run_id, self.pipeline_run, self.batch, self.business_date, stage, entity, status,
               rows_in, rows_out, rows_rejected, snapshot_before, snapshot_after, started_at or _now(), _now(),
               message[:2000])
        self.spark.createDataFrame([row], LOAD_AUDIT_SCHEMA).writeTo(self.names.t("ref", "load_audit")).append()

    def transform(self, step: str, transform_type: str, sources, target: str, rows_in=None, rows_out=None,
                  details: str = "") -> None:
        assert transform_type in TRANSFORM_TYPES, transform_type
        sources = sources if isinstance(sources, str) else ",".join(sources)
        snap = snapshot_id(self.spark, target) if "." in target and table_exists(self.spark, target) else None
        self._steps.append((self.run_id, self.pipeline_run, self.batch, self.business_date, self.job, step,
                            transform_type, sources, target, rows_in, rows_out, snap, _now(), details[:2000]))

    def flush(self) -> None:
        if self._steps:
            df = self.spark.createDataFrame(self._steps, TRANSFORM_LOG_SCHEMA)
            df.writeTo(self.names.t("ref", "transform_log")).append()
            self._steps = []


# ---------------------------------------------------------------- fs


def read_manifests(spark, landing: str, business_date: date) -> dict:
    """source -> the batch's _manifest.json (None when the source sent nothing for the date)."""
    fs, out = HadoopFS(spark), {}
    for source in SOURCES:
        uri = f"{landing}/{source}/{business_date.isoformat()}/_manifest.json"
        out[source] = json.loads(fs.read_text(uri)) if fs.exists(uri) else None
    return out


def reports_for(landing: str) -> str:
    """The reports folder next to the landing folder."""
    return landing.rstrip("/").rsplit("/", 1)[0] + "/reports"


def require_completed(spark, names: Names, stage: str, business_date: date) -> None:
    """Refuse to build on a batch whose upstream stage has not COMPLETED since it last STARTED."""
    t, bid = names.t("ref", "load_audit"), batch_id(business_date)
    rows = [] if not table_exists(spark, t) else spark.sql(
        f"SELECT status FROM {t} WHERE batch_id = '{bid}' AND stage = '{stage}' AND entity = '*' "
        f"AND status IN ('STARTED', 'COMPLETED', 'FAILED') ORDER BY ended_at DESC LIMIT 1").collect()
    if not rows or rows[0][0] != "COMPLETED":
        state = rows[0][0] if rows else "not run"
        raise RuntimeError(f"{stage} is {state} for {bid}; run or re-run it first")


class HadoopFS:
    """Files on the landing zone through the Hadoop FileSystem of the Spark session."""

    def __init__(self, spark):
        self.jvm = spark._jvm
        self.conf = spark._jsc.hadoopConfiguration()

    def _fs(self, uri: str):
        path = self.jvm.org.apache.hadoop.fs.Path(uri)
        return path.getFileSystem(self.conf), path

    def write_bytes(self, uri: str, data: bytes) -> None:
        fs, path = self._fs(uri)
        out = fs.create(path, True)
        try:
            out.write(bytearray(data))
        finally:
            out.close()

    def exists(self, uri: str) -> bool:
        fs, path = self._fs(uri)
        return bool(fs.exists(path))

    def delete(self, uri: str) -> None:
        fs, path = self._fs(uri)
        if fs.exists(path):
            fs.delete(path, True)

    def read_text(self, uri: str) -> str:
        """Also reads _-prefixed files, which Hadoop input formats treat as hidden."""
        fs, path = self._fs(uri)
        stream = fs.open(path)
        try:
            return bytes(self.jvm.org.apache.commons.io.IOUtils.toByteArray(stream)).decode("utf-8")
        finally:
            stream.close()


class LocalFS:
    """The same interface over a local folder (file:// URIs or plain paths), for a laptop or CI."""

    @staticmethod
    def _path(uri: str) -> Path:
        return Path(uri[len("file://"):] if uri.startswith("file://") else uri)

    def write_bytes(self, uri: str, data: bytes) -> None:
        p = self._path(uri)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def exists(self, uri: str) -> bool:
        return self._path(uri).exists()

    def delete(self, uri: str) -> None:
        import shutil

        p = self._path(uri)
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()


def filesystem(spark, uri: str):
    return HadoopFS(spark) if spark is not None and not uri.startswith(("file:", "/")) else LocalFS()


def as_uri(path_or_uri: str) -> str:
    """Spark reads file:// and s3a:// alike; a bare local path becomes file://."""
    if "://" in path_or_uri:
        return path_or_uri.rstrip("/")
    return "file://" + str(Path(path_or_uri).resolve())


# ---------------------------------------------------------------- source parsers (run on executors)


def _basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def dump_columns(text: str, table: str) -> list[str]:
    """Column order of `table` from the CREATE TABLE statement of a mysqldump."""
    cols, inside = [], False
    for line in text.splitlines():
        if line.startswith(f"CREATE TABLE `{table}` ("):
            inside = True
            continue
        if inside:
            s = line.strip()
            if s.startswith("`"):
                cols.append(s[1:s.index("`", 1)])
            elif s.startswith(")"):
                break
    return cols


def _tuples(values: str):
    """Split the VALUES part of an extended INSERT into tuples of str | None."""
    row, cur, out = [], [], []
    i, n, in_str, in_tuple, quoted = 0, len(values), False, False, False
    while i < n:
        ch = values[i]
        if in_str:
            if ch == "\\" and i + 1 < n:
                cur.append({"n": "\n", "t": "\t", "0": "\0"}.get(values[i + 1], values[i + 1]))
                i += 2
                continue
            if ch == "'" and i + 1 < n and values[i + 1] == "'":
                cur.append("'")
                i += 2
                continue
            if ch == "'":
                in_str = False
            else:
                cur.append(ch)
        elif ch == "'":
            in_str, quoted = True, True
        elif ch == "(" and not in_tuple:
            in_tuple, row, cur, quoted = True, [], [], False
        elif ch in ",)" and in_tuple:
            token = "".join(cur)
            row.append(None if (not quoted and token.strip().upper() == "NULL") else token if quoted else token.strip())
            cur, quoted = [], False
            if ch == ")":
                in_tuple = False
                out.append(row)
        elif in_tuple:
            cur.append(ch)
        i += 1
    return out


def parse_dump(path: str, text: str, table: str, columns: list[str]):
    """(file, row_no, value per contract column) for every row of `table` in a mysqldump."""
    order = dump_columns(text, table)
    prefix = f"INSERT INTO `{table}` VALUES "
    row_no = 0
    for line in text.splitlines():
        if not line.startswith(prefix):
            continue
        for values in _tuples(line[len(prefix):].rstrip(";")):
            row_no += 1
            rec = dict(zip(order, values))
            yield (_basename(path), row_no, *[rec.get(c) for c in columns])


def parse_psv(path: str, text: str, columns: list[str]):
    """(file, line_no, value per contract column) for the data lines of a pipe-delimited extract."""
    lines = text.splitlines()
    if not lines:
        return
    header = lines[0].split("|")
    for no, line in enumerate(lines[1:], 2):
        if not line or line.startswith("T|"):
            continue
        rec = dict(zip(header, line.split("|")))
        yield (_basename(path), no, *[(rec.get(c) if rec.get(c) != "" else None) for c in columns])


def psv_stats(path: str, text: str):
    """(file, header, data lines, trailer count, trailer total) of a pipe-delimited extract."""
    lines = [x for x in text.splitlines() if x]
    trailer = next((x for x in reversed(lines) if x.startswith("T|")), None)
    parts = trailer.split("|") if trailer else []
    count = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    total = float(parts[2]) if len(parts) > 2 and parts[2] not in ("", "None") else None
    data = len([x for x in lines[1:] if not x.startswith("T|")])
    return (_basename(path), "|".join(lines[0].split("|")) if lines else "", data, count, total)


def parse_jsonl(path: str, text: str):
    """(file, line_no, raw JSON line, parse error)"""
    for no, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            json.loads(line)
            err = None
        except ValueError as e:
            err = str(e)[:200]
        yield (_basename(path), no, line, err)


def parse_json_array(path: str, text: str):
    """(file, element_no, compact JSON of the element, parse error)"""
    try:
        docs = json.loads(text)
    except ValueError as e:
        yield (_basename(path), 0, text[:2000], str(e)[:200])
        return
    for no, doc in enumerate(docs, 1):
        yield (_basename(path), no, json.dumps(doc, separators=(",", ":"), sort_keys=False), None)


# ---------------------------------------------------------------- typing and standardisation (Spark columns)

DECIMAL = "decimal(18,2)"
SOURCE_TZ = "Asia/Kolkata"
PAN_RE = r"^[A-Z]{5}[0-9]{4}[A-Z]$"
TITLE_RE = r"^(MR|MRS|MS|MISS|DR|SHRI|SMT|PROF) "
ADDRESS_ABBREVIATIONS = ((r"\bRD\.", "ROAD"), (r"\bST\.", "STREET"), (r"\bNGR\b", "NAGAR"),
                         (r"\bMG\.", "MARG"), (r"\bNR\.", "NEAR"))
NAME_VARIANTS = ((r"\bMOHD\b", "MOHAMMED"), (r"\bMOHAMMAD\b", "MOHAMMED"))


def typed(value, coldef: dict):
    """A bronze string as the contract's type; blank -> NULL; dates in any of the source's formats."""
    from pyspark.sql import functions as F

    v = F.trim(value)
    v = F.when(v != "", v)
    kind = coldef["type"]
    fmts = coldef.get("formats") or ([coldef["format"]] if coldef.get("format") else [])
    if kind in ("int", "bigint"):
        return v.cast(kind)
    if kind == "decimal":
        return v.cast(DECIMAL)
    if kind == "date":
        return F.coalesce(*[F.to_date(v, f) for f in fmts]) if fmts else v.cast("date")
    if kind == "timestamp":
        return F.to_timestamp(v, fmts[0]) if fmts else v.cast("timestamp")
    return v


def typed_columns(contract: dict, value_of=None) -> list:
    from pyspark.sql import functions as F

    value_of = value_of or (lambda c: F.col(c["name"]))
    return [typed(value_of(c), c).alias(c["name"]) for c in contract["columns"]]


def source_ts(col):
    """A source-local (IST) wall-clock timestamp as UTC."""
    from pyspark.sql import functions as F

    return F.to_utc_timestamp(col, SOURCE_TZ)


def _squash(x):
    from pyspark.sql import functions as F

    return F.trim(F.regexp_replace(x, r"\s+", " "))


def std_name(col):
    """Upper case, letters only, no title, common spelling variants folded."""
    from pyspark.sql import functions as F

    x = _squash(F.regexp_replace(F.upper(col), r"[^A-Z ]", " "))
    x = F.regexp_replace(x, TITLE_RE, "")
    for pattern, repl in NAME_VARIANTS:
        x = F.regexp_replace(x, pattern, repl)
    return F.when(x != "", x)


def name_key(std):
    """Order-free form of a standardised name ('AGARWAL AJAY' == 'AJAY AGARWAL')."""
    from pyspark.sql import functions as F

    return F.array_join(F.array_sort(F.split(std, " ")), " ")


def std_mobile(col):
    """Indian mobile in E.164 (+91 and 10 digits from 6-9), or NULL."""
    from pyspark.sql import functions as F

    d = F.regexp_replace(col, r"[^0-9]", "")
    d = (F.when((F.length(d) == 12) & d.startswith("91"), F.substring(d, 3, 10))
         .when((F.length(d) == 11) & d.startswith("0"), F.substring(d, 2, 10)).otherwise(d))
    return F.when(d.rlike(r"^[6-9][0-9]{9}$"), F.concat(F.lit("+91"), d))


def std_pan(col):
    from pyspark.sql import functions as F

    p = F.upper(F.trim(col))
    return F.when(p.rlike(PAN_RE), p)


def std_email(col):
    from pyspark.sql import functions as F

    e = F.lower(F.trim(col))
    return F.when(e.rlike(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"), e)


def std_address(col):
    """Upper case, the sources' abbreviations expanded, punctuation dropped."""
    from pyspark.sql import functions as F

    x = F.upper(col)
    for pattern, repl in ADDRESS_ABBREVIATIONS:
        x = F.regexp_replace(x, pattern, repl)
    x = _squash(F.regexp_replace(x, r"[^A-Z0-9 ]", " "))
    return F.when(x != "", x)


def std_pincode(col):
    from pyspark.sql import functions as F

    p = F.regexp_extract(col, r"(\d{6})", 1)
    return F.when(p != "", p)


def fx_rates(spark):
    """currency -> INR rate from config/pipeline.json, as a small DataFrame."""
    rates = load_json("config/pipeline.json")["currencies"]
    return spark.createDataFrame([(c["code"], float(c["inr_rate"])) for c in rates], "currency string, fx_rate_inr double")


def fail_on_partition(partition_id: int, target: int = 1) -> int:
    """For the failure drill: one task of the write fails, so Spark aborts the job before the commit."""
    if partition_id == target:
        raise RuntimeError(f"simulated failure in task for partition {partition_id}")
    return partition_id


def error_summary(e: BaseException, limit: int = 500) -> str:
    """One line for the audit: the innermost Python error when Spark wraps a worker traceback."""
    text = str(e)
    errors = [ln.strip() for ln in text.splitlines() if re.match(r"\s*\w+(Error|Exception):", ln)]
    return (errors[-1] if errors else f"{type(e).__name__}: {text.strip().splitlines()[0] if text.strip() else ''}")[:limit]
