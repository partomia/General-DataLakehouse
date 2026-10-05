# C07. Failed-batch recovery and automated reconciliation

## Summary

The bronze run for 2026-09-23 is made to fail in the middle of writing `lms_loan` (a Spark
task raises an error). Three entities had already committed; `lms_loan` and the rest had not.

- **Rows already written:** the three committed entities stay, complete, and are recorded as
  `COMMITTED` in `ref.load_audit` with their snapshot ids. `lms_loan` has **no rows and no
  snapshot** from the failed attempt: each entity is one Iceberg commit, and a write that dies
  never commits. Nothing is half-written.
- **Next stage refuses:** silver will not start on a batch whose last bronze run `FAILED`.
- **Automated mismatch report:** reconciliation runs anyway and compares every entity with the
  source manifest (record counts, control totals, file trailers): 11 matched, 19 mismatched.
- **Re-run (`--mode resume`):** skips the 3 committed entities, loads the rest; one batch, no
  duplicates. Reconciliation after: 47 matched, 4 explained, 1 mismatch (a planted fault in
  the source file itself).

Full drill notes: [`docs/FAILED_BATCH_DEMO.md`](../docs/FAILED_BATCH_DEMO.md).

## How the guarantees are built

| Concern | Mechanism | Code |
|---|---|---|
| No partial entity | one Iceberg commit per entity, replacing the date's partition (`overwritePartitions`) | `cde/jobs/ingest_bronze.py` |
| What committed | `load_audit`: per entity `COMMITTED` with snapshot before / after; the run `STARTED`, then `COMPLETED` or `FAILED` with the error | `cde/jobs/gdl_common.py` (`Audit`) |
| No building on a broken batch | `require_completed`: each stage needs the previous stage `COMPLETED` for the batch | `cde/jobs/gdl_common.py` |
| Re-run without duplicates | `--mode resume` skips entities committed since the last complete run; `--mode normal` is also safe (partition replace) | `ingest_bronze.py` |
| Mismatch report | `reconcile.py` against the source manifest: `load_status`, `landed_records`, `control_total`, `file_trailer`, silver counts and totals, AML truth checks | `cde/jobs/reconcile.py` |
| Orchestration | Airflow: `reconcile` runs whatever happened (`all_done`); `batch_complete` needs all stages (`all_success`), so the failed run is red | `cde/dags/gdl_dag.py` |
| Alternative: roll back | `load_audit` holds every snapshot before the batch; `CALL system.rollback_to_snapshot(...)` | Spark SQL |

## Walkthrough (about 10 minutes)

The drill already ran (DAG runs 199 and 203, 2 Oct). Show the evidence; do not re-run it once
later dates are loaded. The resume run rebuilds MDM for 23 Sep, then gold refuses the earlier
date (SCD2 is built forward), which leaves MDM and gold on different dates. The same holds for any
date before the latest one loaded (25 Sep), 21 Sep included. To run it live, run the bronze drill
in a sandbox: the
same jobs with `--db-prefix rsingh_gdl_demo` and their own `--landing` folder (commands in
`docs/PRESENTER_RUNBOOK.md`, slide 15).

**1. Trigger the failure (the recorded run: DAG run 199).**

```bash
cde job run --name rsingh-gdl-orchestration \
  --config-json '{"business_date": "2026-09-23", "bronze_mode": "fail-during:lms_loan"}'
```

Airflow UI: land green, bronze red (`RuntimeError: simulated failure in task for partition 1`),
silver / mdm / gold skipped, reconcile green, `batch_complete` red, run **failed**.

**2. What was written before the failure.**

```sql
SELECT run_id, entity, status, rows_out, snapshot_before, snapshot_after, ended_at, message
FROM rsingh_gdl_ref.load_audit
WHERE batch_id = 'B20260923' AND stage = 'bronze' ORDER BY ended_at;
```

The failed run: `cbs_cdc_event` (42), `cbs_eod_balance` (1,455), `lms_borrower` (332)
`COMMITTED`, then `*` `FAILED` with the error. The resume run: the same three `SKIPPED`, the rest
`COMMITTED`, `*` `COMPLETED`.

**3. Nothing half-written.**

```sql
DESCRIBE HISTORY rsingh_gdl_bronze.lms_loan;
```

One snapshot per day loaded: 23:20 and 23:34 on 1 Oct (the two earlier days), then 06:20:43
on 2 Oct, which is the resume. The failed attempt ran 06:15-06:16 and left no snapshot.

```sql
SELECT _batch_id, COUNT(*) FROM rsingh_gdl_bronze.lms_loan GROUP BY _batch_id ORDER BY 1;
-- B20260923: 361, once
```

**4. The mismatch report of the failed attempt.** The resume's reconciliation replaced the
batch's results, but Iceberg keeps the earlier snapshot, so the report of the failure is still
queryable:

```sql
SELECT status, COUNT(*) FROM rsingh_gdl_ref.recon_results
FOR SYSTEM_VERSION AS OF 3386754692264325777
WHERE batch_id = 'B20260923' GROUP BY status;
-- MATCHED 11, MISMATCH 19

SELECT layer, entity, check_name, expected, actual, difference, detail
FROM rsingh_gdl_ref.recon_results
FOR SYSTEM_VERSION AS OF 3386754692264325777
WHERE batch_id = 'B20260923' AND status = 'MISMATCH'
ORDER BY layer, entity, check_name;
```

What it says:

| Layer | Entity | Check | Expected | Actual | Detail |
|---|---|---|---|---|---|
| bronze | `*` | batch_status | 1 | 0 | last bronze run FAILED: RuntimeError: simulated failure in task for partition 1 |
| bronze | lms_loan | landed_records | 361 | 0 | 0 accepted + 0 quarantined vs 361 in the manifest |
| bronze | lms_loan | control_total | 537,910,847.14 | 0 | vs manifest principal_outstanding |
| bronze | pay_transaction | landed_records / control_total | 1,505 / 72,303,633.34 | 0 | not loaded |
| bronze | lms_repayment, crm_customer, doc_document, aml_watchlist | load_status, landed_records | | 0 | not loaded by the last run (FAILED) |
| silver | cbs_eod_balance | silver_records / silver_total | 1,455 / 475,724,531.95 | 0 | silver not built |
| gold | fact_aml_alert | truth_watchlist_hits | 2 | 0 | missed WL000021, WL000016 |

And what matched: the three committed entities, each against the manifest (`cbs_eod_balance`
1,455 records, control total 475,724,531.95, trailer 1,455).

The same report is written as `reports/recon/2026-09-23/mismatch_report.csv` and on the dashboard "GDL
Reconciliation & Data Quality".

**5. Silver refuses.** In the recorded stage-by-stage run (CDE run 136): `refused, last bronze
run for B20260923 is FAILED`. In the DAG, silver is skipped (upstream failed).

**6. The re-run.**

```bash
cde job run --name rsingh-gdl-orchestration \
  --config-json '{"business_date": "2026-09-23", "bronze_mode": "resume"}'
```

All green. Then the current report:

```sql
SELECT status, COUNT(*) FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260923' AND run_id LIKE 'reconcile%' GROUP BY status;
-- MATCHED 47, EXPLAINED 4, MISMATCH 1

SELECT layer, entity, check_name, expected, actual, detail
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260923' AND status <> 'MATCHED' AND run_id LIKE 'reconcile%';
```

The one mismatch is real and expected: `lms_repayment_20260923.csv: trailer says 13, file has
12 data lines` (a fault planted in the source file). The 4 explained are differences with a
known reason: 1 duplicate payment message removed in silver (count and amount), and 6 balance
and 8 payment rows on the UNKNOWN party because their customer was quarantined at bronze. The KPI consistency check: 23 matched.

## Common questions

- **"Why not roll back the three committed entities?"** They are complete and verified against
  the manifest; reloading them is wasted work. If the source itself was wrong, roll back:
  `load_audit.snapshot_before` gives the id for `rollback_to_snapshot`, or run
  `--mode normal`, which replaces the date's partition.
- **"What if it fails during the commit itself?"** Iceberg's commit is an atomic swap of the
  metadata pointer; it either happened or not.
- **"Is the report automatic?"** Yes: the `reconcile` task runs after every attempt, success
  or not, and writes `recon_results`, the CSV and the dashboard.
