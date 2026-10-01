# Demo runbook

How to deploy the lakehouse on the federal Cloudera environment, load the five business dates,
and present it. Every command is from the repository root. Credentials come from environment
variables (or an untracked `.env`), never from files in the repository:

```bash
export GDL_IMPALA_USER=... GDL_IMPALA_PASSWORD=...      # CDP workload user (CDW Impala)
export GDL_WORKLOAD_USER=... GDL_WORKLOAD_PASSWORD=...  # the same user, for Atlas and Ranger
export GDL_VIZ_API_KEY=...                              # Data Visualization API key
```

## 1. Deploy (once, and after a change)

```bash
git push                                  # CDE reads the job code from the GitHub repository
bash cde/scripts/deploy_jobs.sh           # repository rsingh-gdl-pipeline + jobs rsingh-gdl-{land,bronze,silver,mdm,gold,recon}
bash cde/scripts/deploy_dag.sh            # Airflow job rsingh-gdl-orchestration, registered paused, no schedule
```

After a code change, `git push` and `cde repository sync --name rsingh-gdl-pipeline` is enough
for the Spark jobs; re-run `deploy_dag.sh` after a DAG change.

## 2. Load the business dates

One batch per business date, in date order. Either trigger the DAG (Airflow UI: Trigger DAG
w/ config, or the CLI):

```bash
cde job run --name rsingh-gdl-orchestration --config-json '{"business_date": "2026-09-21"}'
```

or run the stages one by one, which is what the demo was built and verified with:

```bash
for s in land bronze silver mdm gold recon; do
  cde job run --name rsingh-gdl-$s --wait --arg=--business-date --arg=2026-09-21 \
    --arg=--landing --arg=s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing
done
```

Then the semantic layer on CDW Impala (certified KPI views, MIS and dashboard views, the
regulatory datasets for the date, and the KPI consistency check):

```bash
python scripts/run_semantic.py --engine impala --dates 2026-09-21 --steps views,load,check
python scripts/run_semantic.py --engine impala --dates 2026-09-22 --steps load,check   # later dates
```

Day 3 (2026-09-23) carries a planted defect: the LMS repayment file's trailer says one record
more than the file has, so its reconciliation shows one MISMATCH. That is the expected result.

## 3. Governance and dashboards

```bash
python scripts/governance.py apply && python scripts/governance.py verify
python dataviz/build_dashboard.py --import --connection federal-impala-1
python dataviz/build_dashboard.py --verify
```

Run both again whenever tables or views were recreated (a reset, or `--steps views`).

## 4. What to show

| Topic | Where | What to point at |
|---|---|---|
| Sources and landing | S3 `landing/<source>/<date>/`, `_manifest.json` | a mysqldump on day 1, CDC JSON from day 2, CSV trailers, nested payments JSON (a `device` field appears on day 4), e-mails, KYC text and scans, the screening list |
| Validation and quarantine | `rsingh_gdl_bronze.quarantine`, `ref.schema_drift` | reject reasons per record; the schema drift row on day 4 |
| CDC | `rsingh_gdl_silver.cdc_exception` | re-sent, out-of-order, stale and unknown-key events, and what was done with each |
| MDM | dashboard "GDL MDM & Golden Record" | golden records merged from several systems, the review queue (similar names kept apart), a PAN conflict not merged, precision and recall against the generator truth |
| SCD2 | `SELECT * FROM rsingh_gdl_gold.dim_party WHERE party_id IN (SELECT party_id FROM rsingh_gdl_gold.dim_party WHERE version > 1) ORDER BY party_id, version` | versions, effective dates, `changed_attributes`, the source record of each version |
| Time travel | `sql/time_travel.sql` in Hue | a golden record before and after the address-change request, a snapshot diff, the regulatory dataset as submitted |
| KPIs | dashboard "GDL Banking KPIs MIS"; `sql/adhoc.sql` in Hue; `rsingh_gdl_semantic.reg_*` | the same certified figure on the dashboard, in the ad-hoc answer and in the regulatory dataset; the "KPI consistency" table on the Reconciliation dashboard proves it |
| Reconciliation | dashboard "GDL Reconciliation & Data Quality" | every check per layer and batch, the explained differences with their reasons, the planted trailer mismatch |
| Failed batch | [FAILED_BATCH_DEMO.md](FAILED_BATCH_DEMO.md) | a run that dies mid-write, what it left, the mismatch report, the resume |
| Lineage and glossary | Atlas: search `rsingh_gdl_semantic.kpi_npa_exposure` | lineage from the landing files to the dashboard view; the glossary term on the view and its consumers |
| Masking | Hue as federal01, then as rsingh ([GOVERNANCE.md](GOVERNANCE.md#masking)) | names hashed, PAN and mobile last 4, date of birth year only |
| Extension | dashboard "GDL AML Alerts"; [BANKING_MODEL.md](BANKING_MODEL.md#extending-the-model-aml-as-the-worked-example) | a new source and domain added without touching the others; the alert queue; alerts checked against the planted truth |

## 5. Reset

Everything the demo creates is in the `rsingh_gdl_*` databases and under the landing prefix.
To start again, drop every table and view in the six databases (Impala, as the workload user),
re-run section 2 from day 1, then section 3. SCD2 is built forward, so re-running an early
date after later ones needs this reset.
