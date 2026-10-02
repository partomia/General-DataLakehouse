# Failed-batch drill

A bronze run for 2026-09-23 dies in the middle of writing `lms_loan`. The drill shows what that
leaves behind, how the batch's reconciliation reports it, why the next stage refuses to build on
it, and how the re-run finishes the batch without duplicating anything.

## Why a failure leaves nothing half-written

- Every bronze entity is written as one Iceberg commit that replaces the business date's
  partition. A Spark task that dies mid-write means the commit never happens: the table still
  holds its previous snapshot, so readers see either the whole entity for the date or none of it.
- `rsingh_gdl_ref.load_audit` records every entity as it commits (`COMMITTED`, with the table's
  snapshot id before and after), and the run as `STARTED`, then `COMPLETED` or `FAILED` with the
  error.
- Each stage refuses to start unless the previous stage `COMPLETED` for the batch since it last
  started (`require_completed`), so silver cannot build on a broken bronze batch.
- `--mode resume` skips the entities committed since the last complete run and loads the rest.
  Re-running the whole batch instead (`--mode normal`) is also safe: every write replaces the
  date's partition.

## Run it on CDE

```bash
D=2026-09-23
cde job run --name rsingh-gdl-land   --wait --arg=--business-date --arg=$D
cde job run --name rsingh-gdl-bronze --wait --arg=--business-date --arg=$D --arg=--mode --arg=fail-during:lms_loan   # fails
cde job run --name rsingh-gdl-recon  --wait --arg=--business-date --arg=$D          # the mismatch report
cde job run --name rsingh-gdl-silver --wait --arg=--business-date --arg=$D          # refuses: bronze not COMPLETED
cde job run --name rsingh-gdl-bronze --wait --arg=--business-date --arg=$D --arg=--mode --arg=resume
for s in silver mdm gold recon; do cde job run --name rsingh-gdl-$s --wait --arg=--business-date --arg=$D; done
```

Through the DAG it is two triggers: `{"business_date": "2026-09-23", "bronze_mode":
"fail-during:lms_loan"}` (bronze fails; reconcile still runs, trigger rule `all_done`), then
`{"business_date": "2026-09-23", "bronze_mode": "resume"}`.

Locally the same drill runs with `scripts/run_local.py ... -- --fail-during lms_loan`, and CI runs
it on every push (`.github/workflows/ci.yml`).

## What to look at (Hue, Impala)

```sql
-- 1. the batch's attempts: entities committed before the failure, the FAILED run and its error
SELECT entity, status, rows_out, ended_at, message
FROM rsingh_gdl_ref.load_audit
WHERE batch_id = 'B20260923' AND stage = 'bronze' ORDER BY ended_at;

-- 2. the automatic mismatch report (also a CSV under .../reports/recon/2026-09-23/)
SELECT layer, entity, check_name, expected, actual, detail
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260923' AND status = 'MISMATCH';

-- 3. the table's Iceberg history: no snapshot from the failed attempt
DESCRIBE HISTORY rsingh_gdl_bronze.lms_loan;

-- 4. the rows by batch, after the resume: one batch, no duplicates
SELECT _batch_id, COUNT(*) FROM rsingh_gdl_bronze.lms_loan GROUP BY _batch_id ORDER BY 1;
```

The dashboard "GDL Reconciliation & Data Quality" shows the same: the failed attempt and its
error on the Load audit sheet ("The failed-batch trail"), and the mismatches per business date.

Rolling back is the alternative to re-running: `ref.load_audit` holds every table's snapshot id
before the batch, and `CALL system.rollback_to_snapshot('rsingh_gdl_bronze.lms_loan', <id>)` in
Spark restores it.

## Recorded run (CDE, 1 Oct 2026)

| Step | CDE run | Result |
|---|---|---|
| land 2026-09-23 | 133 | succeeded |
| bronze `--mode fail-during:lms_loan` | 134 | failed: `RuntimeError: simulated failure in task for partition 1` |
| recon | 135 | `MATCHED 11, EXPLAINED 0, MISMATCH 19` |
| silver | 136 | failed: refused, last bronze run for B20260923 is FAILED |
| bronze `--mode resume` | 137 | succeeded; 3 entities skipped as already committed |
| silver, mdm, gold | 138-140 | succeeded |
| recon | 141 | `MATCHED 47, EXPLAINED 4, MISMATCH 1` |

After the failure, `load_audit` showed `cbs_cdc_event`, `cbs_eod_balance` and `lms_borrower`
COMMITTED, then `* FAILED`. Bronze had 332 `lms_borrower` rows for the date and 0 for
`lms_loan`, `lms_repayment` and `pay_transaction`. The 19 mismatches were the batch status,
the load status, record counts and control totals for each entity not loaded, the silver
balances not built, and the two screening hits for the day not yet alerted (`WL000021`,
`WL000016`).

`DESCRIBE HISTORY rsingh_gdl_bronze.lms_loan` had two snapshots, one for each earlier day;
the failed attempt left none. After the resume it had a third, and the day had 361
`lms_loan` rows, 12 `lms_repayment` rows and 1503 `pay_transaction` rows (2 quarantined for
a missing amount).

The one mismatch left is the planted fault: `lms_repayment_20260923.csv: trailer says 13,
file has 12 data lines`. The KPI consistency check passed (23 matched, 0 mismatches; gross
NPA 11.40%, CASA 40.57%).

## Recorded run through the Airflow DAG (CDE, 2 Oct 2026)

The same drill, one DAG run per attempt:

```bash
cde job run --name rsingh-gdl-orchestration \
  --config-json '{"business_date": "2026-09-23", "bronze_mode": "fail-during:lms_loan"}'
cde job run --name rsingh-gdl-orchestration \
  --config-json '{"business_date": "2026-09-23", "bronze_mode": "resume"}'
```

| DAG run | Tasks | Result |
|---|---|---|
| 199 | land succeeded, bronze failed, silver / mdm / gold skipped (upstream failed), reconcile succeeded (`MATCHED 11, EXPLAINED 0, MISMATCH 19`), `batch_complete` failed | **failed** |
| 203 | land, bronze (3 entities skipped as already committed), silver, mdm, gold, reconcile (`MATCHED 47, EXPLAINED 4, MISMATCH 1`), `batch_complete` | succeeded |

`reconcile` runs whatever happened upstream (`trigger_rule="all_done"`), so the failed
batch is reconciled and the mismatches are on the dashboard. `batch_complete` needs every
stage to succeed, so the Airflow grid shows the failed attempt red. Without it, the first
try of this drill (run 195) showed as succeeded: Airflow takes a run's state from its last
tasks. `load_audit`, the row counts and the snapshots were the same as in the run above.
