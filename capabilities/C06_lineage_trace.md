# C06. End-to-end lineage, from a dashboard figure to its source column

## Summary

The figure: **Gross NPA ratio 11.29%** on the "NPA exposure" sheet of the dashboard "GDL
Banking KPIs MIS" (business date 2026-09-25; gross NPA 60,566,765.50 over gross advances
536,438,057.35). It traces to two columns of the LMS loan file, `principal_outstanding` and
`dpd`, through seven hops. Atlas holds the lineage of the five hops inside the lakehouse,
automatically, with column lineage; the two edge hops (file to bronze, view to dashboard) are
covered by the pipeline's own metadata and the dashboard code.

**Streaming: there is no streaming pipeline in this project.** Every source, including the
CDC feed, arrives as a daily batch of files. Say so, and see the last section for what to
offer.

## The hops

| # | From | To | Tool | Code | What happens | Lineage evidence |
|---|---|---|---|---|---|---|
| 1 | LMS extract | `landing/lms/2026-09-25/lms_loan_20260925.csv` | the source | `cde/jobs/land_sources.py` (synthetic LMS) | pipe-delimited, header, trailer | the manifest |
| 2 | landing file | `bronze.lms_loan` | CDE Spark | `cde/jobs/ingest_bronze.py` | parsed, validated against `contracts/lms_loan.json`; strings as received; non-numeric `dpd` quarantined | `_source_file`, `_source_row` on the row; `transform_log` (source `landing:lms/2026-09-25/lms_loan_*.csv`); not in Atlas |
| 3 | `bronze.lms_loan` | `silver.lms_loan_daily` | CDE Spark | `build_silver.py` `loan_daily` | typed (decimal, integer, dd/MM/yyyy dates) | Atlas `spark_process`; `_src_position` |
| 4 | `silver.lms_loan_daily` + `gold.dim_loan` | `gold.fact_loan_position_daily` | CDE Spark | `build_gold.py` | `is_npa` = DPD above 90 or loss flag, IRAC asset class and provision from `config/kpi.json` | Atlas `spark_process`; `src_position`; `model/source_mapping.csv` |
| 5 | fact + `dim_loan`, `dim_branch`, `dim_product`, `ref.kpi_parameter` | `semantic.kpi_npa_exposure` | CDW Impala view | `sql/semantic/10_kpi_npa_exposure.sql` | certified KPI: `gross_npa` = outstanding when NPA; `gross_advance` = outstanding | Atlas `impala_process`, column lineage |
| 6 | `kpi_npa_exposure` | `semantic.mis_npa_trend` | CDW Impala view | `sql/semantic/20_mis_views.sql` | `SUM(gross_npa) / SUM(gross_advance)` per date, `is_latest` | Atlas `impala_process` |
| 7 | `mis_npa_trend` | tile "Gross NPA ratio %" | Data Visualization | `dataviz/build_dashboard.py` (dataset "GDL - NPA trend") | `max([gross_npa_ratio])`, filter `is_latest = 1` | the dashboard code; not in Atlas |

## Walkthrough (about 8 minutes)

**1. The figure (30 s).** Open the dashboard, NPA exposure sheet: 11.29%. In
`dataviz/build_dashboard.py` the tile is `title="Gross NPA ratio %"` on dataset `npa_trend` =
`mis_npa_trend`.

**2. Atlas (3 min).** Search `rsingh_gdl_semantic.kpi_npa_exposure`, Lineage tab. Upstream:
`fact_loan_position_daily` (Impala process), then `silver.lms_loan_daily` and `dim_loan` (Spark
processes from the `rsingh-gdl-gold` job), then `bronze.lms_loan` (Spark process from
`rsingh-gdl-silver`). Click a process to show the job, and a column (`gross_npa`) for column
lineage. Downstream of `kpi_npa_exposure`: the MIS views and `reg_asset_classification`.
Show the glossary term "NPA exposure" on the view (definition, formula, owner).

**3. The same figure, row by row (3 min).** One NPA loan, every layer:

```sql
SELECT loan_id, principal_outstanding, dpd, _source_file, _source_row, _batch_id
FROM rsingh_gdl_bronze.lms_loan
WHERE loan_id = 'LN10000216' AND _business_date = DATE '2026-09-25';
-- '6794000.00', '462', lms_loan_20260925.csv, row 217 (strings, as received)

SELECT loan_id, principal_outstanding, dpd, _src_position
FROM rsingh_gdl_silver.lms_loan_daily
WHERE loan_id = 'LN10000216' AND as_of_date = DATE '2026-09-25';
-- 6794000.00 (decimal), 462 (int), lms_loan_20260925.csv:217

SELECT loan_key, principal_outstanding, dpd, is_npa, asset_class, src_position
FROM rsingh_gdl_gold.fact_loan_position_daily
WHERE loan_key = 'LN:LMS:LN10000216' AND business_date = DATE '2026-09-25';
-- is_npa true, DOUBTFUL_1, lms_loan_20260925.csv:217

SELECT loan_key, gross_advance, gross_npa, asset_class
FROM rsingh_gdl_semantic.kpi_npa_exposure
WHERE loan_key = 'LN:LMS:LN10000216' AND reporting_date = DATE '2026-09-25';
-- 6794000.00 in both: this loan is 6.79 million of the 60.57 million gross NPA

SELECT reporting_date, gross_npa, gross_advances, gross_npa_ratio
FROM rsingh_gdl_semantic.mis_npa_trend WHERE is_latest = 1;
-- 60566765.50 / 536438057.35 = 0.1129
```

Open the file in Hue at row 217 to close the loop.

**4. The custom code and its record (1 min).**

```sql
SELECT job, step, transform_type, source_tables, target_table, details
FROM rsingh_gdl_ref.transform_log
WHERE batch_id = 'B20260925' AND target_table LIKE '%loan%' ORDER BY logged_at;
```

`ingest lms_loan` (from the landing path), `cleanse lms_loan_daily` (typed; dates parsed),
`fact_loan_position_daily` (IRAC class and provision from DPD, `config/kpi.json`). And
`rsingh_gdl_ref.source_mapping` for the column rules:

```sql
SELECT * FROM rsingh_gdl_ref.source_mapping
WHERE target_table = 'fact_loan_position_daily' AND target_column IN ('is_npa', 'principal_outstanding', 'asset_class');
```

**5. Proof the figure is the certified one (30 s).** The "KPI consistency" table on the
Reconciliation dashboard: 23 checks per date that the dashboard, ad-hoc and regulatory paths
give the same NPA, CASA and CRV.

## Streaming lineage

Not in the project. What to say and offer:

- The CDC feed is change events (Debezium style), delivered here as a daily file. In production
  the same events come through Kafka.
- On this platform a streaming pipeline would be Cloudera DataFlow (NiFi) or a Spark
  Structured Streaming job on CDE reading the events, writing an Iceberg table. NiFi reports
  its flow to Atlas as `nifi_flow` and processor lineage; the Spark Atlas hook records a
  streaming query as a `spark_process` like a batch one.
- It can be added: a Structured Streaming job reading the CDC events from a landing folder (or
  Kafka, if the environment has a Streams Messaging Data Hub) into an Iceberg table, then the
  same Atlas search. Ask whether the environment has Kafka or DataFlow before committing to it.

## Common questions

- **"Why are the two edge hops not in Atlas?"** Bronze reads the landing files itself (to keep
  the source row number and parse the dump), not through a Spark data source, so the Atlas hook
  sees no file input. The row-level metadata covers it more precisely than Atlas would (file
  and row, not just path). Data Visualization does not report to Atlas.
- **"If someone changes the NPA rule?"** It lives in `config/kpi.json` and the certified view;
  the glossary term carries the version; the consistency check fails if any consumer computes
  it differently.
