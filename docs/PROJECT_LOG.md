# Project log

What ran on the federal Cloudera environment, what broke there, and the results. Newest last.

## Environment

| Piece | Value |
|---|---|
| CDE | Spark 3.5.4, Iceberg 1.5.2, `spark_catalog` a Hive-backed session catalog; repository `rsingh-gdl-pipeline` (git-backed, this GitHub repo); jobs `rsingh-gdl-{land,bronze,silver,mdm,gold,recon}` |
| CDW | Impala 4.5 warehouse `federal-impala-1` (Hue, LDAP) |
| Data Visualization | the CDW instance `viz-indianbank-spend-analytics`, version 8.1.4, connection `federal-impala-1` |
| SDX | Atlas and Ranger through the data lake's Knox gateway; tag service `cm_tag` linked to `cm_hive` |
| Landing | `s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing/` |
| Laptop and CI | Spark 4.0.1 with Iceberg 1.10 on a JDBC SQLite catalog; `GDL_ICEBERG_PACKAGE` and `GDL_LOCAL_CATALOG=hadoop` reproduce CDE's Spark 3.5 and Iceberg 1.5.2 |

## What the cluster found, and the fixes

Each fix has a test, so it cannot come back.

| Symptom on CDE / CDW | Cause | Fix |
|---|---|---|
| silver failed at import with an `AssertionError` from `F.lit` | a Spark Column built when the module loaded, before a SparkSession existed (the local runner always had one) | the default built inside the function; `tests/test_job_imports.py` imports every job in a fresh process with no SparkSession |
| gold: "does not support REPLACE TABLE AS SELECT" | a bare `writeTo().createOrReplace()` goes to the Hive session catalog, which made a Hive table | every table creation through `gdl_common` (`using("iceberg")`); a test rejects a bare `writeTo(...).create*` elsewhere |
| Impala: "TIMESTAMPTZ column that Impala cannot write" | Impala 4.5 cannot write Iceberg timestamptz; it writes `ref.load_audit`, `transform_log` and `recon_results` too | those columns are `timestamp_ntz` holding UTC (Spark sessions run in UTC); a test checks the three schemas; the tables were dropped and day 1 re-run |
| Impala: syntax errors at `change` and `matched` | reserved words in Impala, not in Spark | renamed; the SQL portability test checks every alias against Impala's reserved words |
| Atlas searches returned nothing | Iceberg tables are `iceberg_table` / `iceberg_column` in Atlas, views `hive_table` / `hive_column` | `scripts/governance.py` searches both |
| Data Visualization API: HTTP 401 with a password | the instance signs users in with SAML | a Data Visualization API key (`GDL_VIZ_API_KEY`) |
| `cde job run --wait` hung on a dropped network | client-side wait | submit without `--wait`, then poll the run |
| Atlas: `fact_aml_alert` had no Spark lineage | the `is_new` lookup reads the table being written; the Spark Atlas hook logs "Detected cycle - same entity observed to both input and output" and drops the outputs | only that lookup is checkpointed, so the rest of the plan, and its lineage, stays visible |

Found later on the laptop: the generator planted its AML structuring deposits on a different
sample of accounts every date (the sample was drawn from the date's active accounts), so no
account ever built up the pattern. The sample is now the first date's, kept.

## Runs

Final run on CDE, 1 Oct 2026, from empty databases (every `rsingh_gdl_*` table and view
dropped first), one `cde job run` per stage:

| Business date | CDE runs (land to recon) | Reconciliation | KPI consistency | Gross NPA | CASA | CRV |
|---|---|---|---|---|---|---|
| 2026-09-21 | 121-126 | 52 matched, 4 explained, 0 mismatches | 23/23 | 10.52% | 40.47% | 22,464,771.12 |
| 2026-09-22 | 127-132 | 48 / 4 / 0 | 23/23 | 10.34% | 40.52% | 22,495,334.09 |
| 2026-09-23 | 133-141 (failed-batch drill) | 47 / 4 / 1 (the planted trailer fault) | 23/23 | 11.40% | 40.57% | 22,547,501.32 |
| 2026-09-24 | 142-147 | 46 / 6 / 0 | 23/23 | 11.58% | 40.62% | 22,573,579.11 |
| 2026-09-25 | 148-153 | 48 / 4 / 0 | 23/23 | 11.29% | 40.73% | 22,670,829.96 |

The results match the laptop run of the same code, apart from a 0.01 rounding difference in
CRV on the last date. The day-3 drill is written up in [FAILED_BATCH_DEMO.md](FAILED_BATCH_DEMO.md).

Afterwards:

- `scripts/governance.py apply` made 128 changes (PII tags on the recreated tables and views,
  and glossary terms on the KPI views); `verify` tagged 115 PII columns, with none left to tag.
- `dataviz/build_dashboard.py --import` loaded 4 dashboards, 13 datasets and 65 visuals.
  `--verify` checked every visual through the Data API, and the KPI tiles agree with Impala.
  On the latest date the AML dashboard shows 5 alerts: 2 critical PAN matches on the
  screening list, and 1 alert that is new that day.
- Masking checked in Hue on `dim_party`: as `federal01` the PII columns come back masked,
  as `rsingh` in clear.
- The Airflow DAG `rsingh-gdl-orchestration` is registered, paused, with its schedule off.
- Atlas lineage runs from bronze to silver, gold and semantic, through Spark and Impala
  processes. For example, `kpi_npa_exposure` traces back to `fact_loan_position_daily`, then
  to `silver.lms_loan_daily` and `bronze.lms_loan`. After the lineage fix above, gold and
  recon for 2026-09-25 were run again (runs 161 and 163), with the same results: 48 / 4 / 0,
  and 23/23 for KPI consistency.
