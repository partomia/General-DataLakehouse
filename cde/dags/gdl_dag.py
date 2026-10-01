"""
Airflow DAG (CDE): one batch of the General Data Lakehouse, one business date per run.

  land_sources -> ingest_bronze -> build_silver -> build_mdm -> build_gold -> reconcile

land_sources stands in for the six source systems dropping their extracts on the landing
zone (s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing/). Every other task reads only
what landed there. reconcile runs whatever happened upstream (trigger rule all_done), so a
failed batch still gets its mismatch report in ref.recon_results and under
<landing>/../reports/recon/<date>/; the stage gates in the jobs (silver needs a COMPLETED
bronze, and so on) stop a later stage from building on a broken one.

The semantic layer (certified KPI views, regulatory datasets, KPI consistency check) runs on
CDW Impala after the batch: python scripts/run_semantic.py --engine impala --dates <date>.

Not scheduled: trigger it per business date (Trigger DAG w/ config), in date order:
  {"business_date": "2026-09-21"}
The failed-batch drill (docs/FAILED_BATCH_DEMO.md):
  {"business_date": "2026-09-23", "bronze_mode": "fail-during:lms_loan"}   the run fails mid-write
  {"business_date": "2026-09-23", "bronze_mode": "resume"}                 finishes the batch

Job names must match cde/scripts/deploy_jobs.sh (CDEJobRunOperator fails with 404 otherwise).
"""

from datetime import datetime, timedelta

from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

JOB_PREFIX = "rsingh-gdl"
DB_PREFIX = "rsingh_gdl"
LANDING = "s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing"
BUSINESS_DATE = "{{ params.business_date }}"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="general_datalakehouse",
    description="Six source systems -> bronze / silver / MDM / gold on Iceberg, reconciled per batch",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 9, 20),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    params={"business_date": "2026-09-21", "bronze_mode": "normal"},
    tags=["lakehouse", "iceberg", "mdm", "scd2", "reconciliation"],
) as dag:

    def stage(task_id: str, job: str, *extra: str, **kwargs) -> CDEJobRunOperator:
        # run-time args replace the job's own, so every task passes the full set
        return CDEJobRunOperator(
            task_id=task_id,
            job_name=f"{JOB_PREFIX}-{job}",
            overrides={"spark": {"args": ["--business-date", BUSINESS_DATE, "--landing", LANDING,
                                          "--db-prefix", DB_PREFIX, "--pipeline-run", "{{ run_id }}", *extra]}},
            wait=True,
            **kwargs,
        )

    land = stage("land_sources", "land")
    bronze = stage("ingest_bronze", "bronze", "--mode", "{{ params.bronze_mode }}")
    silver = stage("build_silver", "silver")
    mdm = stage("build_mdm", "mdm")
    gold = stage("build_gold", "gold")
    recon = stage("reconcile", "recon", trigger_rule="all_done")

    land >> bronze >> silver >> mdm >> gold >> recon
