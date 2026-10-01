#!/usr/bin/env python3
"""
Run the CDE jobs on a laptop (or in CI) against local Spark + Iceberg (a Hadoop
catalog under --warehouse), with the landing zone in a local folder. The job
files are the ones CDE runs; only the Spark session differs.

  python scripts/run_local.py all                              # land 5 dates, then every stage per date
  python scripts/run_local.py all --customers 200 --warehouse /tmp/w --landing /tmp/l
  python scripts/run_local.py land
  python scripts/run_local.py bronze --dates 2026-09-23 -- --fail-after lms_loan
  python scripts/run_local.py silver mdm gold recon --dates 2026-09-21,2026-09-22
  python scripts/run_local.py semantic                         # sql/semantic/*.sql on Spark SQL, per date
  python scripts/run_local.py adhoc time-travel                # sql/adhoc.sql, sql/time_travel.sql (last date)
  python scripts/run_local.py history rsingh_gdl_bronze.lms_loan
  python scripts/run_local.py sql "SELECT * FROM rsingh_gdl_ref.load_audit"

Stages run for each date in turn, in pipeline order (a date's stages complete
before the next date starts), as the Airflow DAG does. Arguments after `--` go
to every job. Experiments belong in scratch folders (--warehouse, --landing),
not in data/.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = ROOT / "cde" / "jobs"
sys.path.insert(0, str(JOBS_DIR))
sys.path.insert(0, str(ROOT / "scripts"))
import gdl_common as C  # noqa: E402

# GDL_ICEBERG_PACKAGE=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 with pyspark 3.5.4
# and GDL_LOCAL_CATALOG=hadoop reproduces CDE's Spark and Iceberg for the jobs (that Iceberg's
# JDBC catalog has no views and locks SQLite, so no semantic stage).
ICEBERG_PACKAGE = os.environ.get("GDL_ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0")
SQLITE_PACKAGE = "org.xerial:sqlite-jdbc:3.46.1.3"
STAGES = {"bronze": "ingest_bronze.py", "silver": "build_silver.py", "mdm": "build_mdm.py",
          "gold": "build_gold.py", "recon": "reconcile.py"}
CFG = C.load_json("config/pipeline.json")


def local_spark(warehouse: Path, driver_memory: str = "4g"):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ["PYTHONPATH"] = os.pathsep.join([str(JOBS_DIR), os.environ.get("PYTHONPATH", "")])
    warehouse.mkdir(parents=True, exist_ok=True)
    # Iceberg JDBC catalog on a SQLite file: unlike the Hadoop catalog it stores views, which
    # the semantic layer needs (CDE and CDW use the Hive metastore for the same).
    b = (SparkSession.builder.appName("gdl-local").master("local[4]")
         .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.warehouse", str(warehouse)))
    if os.environ.get("GDL_LOCAL_CATALOG") == "hadoop":
        b = b.config("spark.jars.packages", ICEBERG_PACKAGE).config("spark.sql.catalog.local.type", "hadoop")
    else:
        b = (b.config("spark.jars.packages", f"{ICEBERG_PACKAGE},{SQLITE_PACKAGE}")
             .config("spark.sql.catalog.local.type", "jdbc")
             .config("spark.sql.catalog.local.uri", f"jdbc:sqlite:{warehouse.resolve() / 'catalog.db'}")
             .config("spark.sql.catalog.local.jdbc.schema-version", "V1"))
    spark = (b
             .config("spark.sql.defaultCatalog", "local")
             .config("spark.driver.memory", driver_memory)
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.default.parallelism", "4")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    C.configure(spark)
    return spark


def load_job(filename: str):
    """Load a job module without registering it in sys.modules, so its closures pickle by value."""
    spec = importlib.util.spec_from_file_location(filename[:-3], JOBS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


def run_stage(spark, stage: str, d: str, common: list, extra: list):
    job = load_job(STAGES[stage])
    print(f"\n=== {stage} {d}", flush=True)
    return job.run(spark, ["--business-date", d, *common, *extra])


def main() -> int:
    if "--" in sys.argv:
        i = sys.argv.index("--")
        argv, extra = sys.argv[1:i], sys.argv[i + 1:]
    else:
        argv, extra = sys.argv[1:], []
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("stages", nargs="+")
    p.add_argument("--dates", default="all", help="comma-separated business dates, or all")
    p.add_argument("--landing", default=str(ROOT / "data" / "landing"))
    p.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse"))
    p.add_argument("--db-prefix", default=CFG["db_prefix"])
    p.add_argument("--customers", type=int, default=CFG["customers"])
    args = p.parse_args(argv)
    dates = CFG["business_dates"] if args.dates == "all" else args.dates.split(",")
    stages = args.stages

    if stages[0] in ("history", "sql"):
        spark = local_spark(Path(args.warehouse))
        if stages[0] == "history":
            spark.sql(f"SELECT committed_at, snapshot_id, parent_id, operation, summary['added-records'] AS added, "
                      f"summary['deleted-records'] AS deleted, summary['total-records'] AS total "
                      f"FROM {stages[1]}.snapshots ORDER BY committed_at").show(100, truncate=False)
        else:
            spark.sql(" ".join(stages[1:])).show(200, truncate=False)
        return 0

    if stages == ["all"]:
        stages = ["land", *STAGES, "semantic"]
    if "land" in stages:
        land = load_job("land_sources.py")
        for d in dates:
            land.main(["--business-date", d, "--landing", args.landing, "--customers", str(args.customers)])
    per_date = [s for s in stages if s in STAGES or s == "semantic"]
    once = [s for s in stages if s in ("adhoc", "time-travel")]
    unknown = [s for s in stages if s not in STAGES and s not in ("land", "semantic", "adhoc", "time-travel")]
    if unknown:
        p.error(f"unknown stages {unknown}")
    if not per_date and not once:
        return 0
    spark = local_spark(Path(args.warehouse))
    common = ["--landing", args.landing, "--db-prefix", args.db_prefix]
    import run_semantic

    engine = run_semantic.SparkEngine(spark)
    views_done = False
    for d in dates:
        for stage in per_date:
            if stage == "semantic":
                print(f"\n=== semantic {d}", flush=True)
                steps = ("load", "check") if views_done else ("views", "load", "check")
                run_semantic.run(engine, args.db_prefix, [date.fromisoformat(d)], steps,
                                 fail_on_mismatch="--fail-on-mismatch" in extra)
                views_done = True
            else:
                run_stage(spark, stage, d, common, extra)
    if once:
        print(f"\n=== {' '.join(once)} {dates[-1]}", flush=True)
        run_semantic.run(engine, args.db_prefix, [date.fromisoformat(dates[-1])], once)
    return 0


if __name__ == "__main__":
    sys.exit(main())
