# C01. Batch ingestion with schema and record validation

## Summary

Every source arrives as a daily batch of files in the landing zone, with a manifest of record
counts and control totals. The bronze job (`cde/jobs/ingest_bronze.py`) parses each file,
checks every record against the entity's contract, loads the good records, quarantines the
rest with reason codes, logs schema drift and file trailers, and writes an audit row per
entity. Reconciliation later proves that accepted plus quarantined equals the manifest.

Say up front: the RDBMS source is the core banking MySQL database, delivered as a nightly
`mysqldump` plus a CDC event feed, which is how banks usually hand data over. There is no live
JDBC pull in the project.

## The sources

| Source | Format | What it shows |
|---|---|---|
| Core banking (MySQL) | `cbs_dump_<date>.sql` (`CREATE TABLE` + `INSERT`), CDC events | An RDBMS extract, parsed in the `CREATE TABLE` column order; CDC images checked against the table they change |
| Loans | pipe-delimited files with header and trailer | trailer count checked against the file |
| Payments | JSON lines | schema drift (a new `device` field from D4) |
| CRM, AML screening list | JSON | semi-structured |
| Documents | `.eml` files, scans | unstructured, metadata checks |

Landing: `s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing/`, one folder per business
date, each with a manifest.

## Walkthrough (about 8 minutes)

**1. The source files (1 min).** Hue file browser on the landing path: open
`cbs_dump_20260921.sql`, a loan file and its trailer line, and the manifest.

**2. The contract is the schema (1 min).** Open `contracts/cbs_customer.json`: type, date
format in the source's own form, required, pattern (PAN, Aadhaar, pincode), allowed values
(KYC status, segment), and severity. `reject` sends the record to quarantine; `warn` loads it
and lists the finding in `_warnings`. That is "unusable record" versus "flag it, but do not
lose a customer row".

**3. Run it (2 min).** Do not trigger the whole DAG for an earlier date: gold builds SCD2
forward only and refuses an earlier date. Run the bronze job alone; it replaces the date's
partition, so nothing duplicates:

```bash
cde job run --name rsingh-gdl-bronze --arg=--business-date --arg=2026-09-22 \
  --arg=--db-prefix --arg=rsingh_gdl \
  --arg=--landing --arg=s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing
```

While it runs, walk the steps in the job's docstring: parse, validate, split, metadata, drift.

**4. Record validation: quarantine (2 min).**

```sql
SELECT q._business_date, q._entity, q._source_file, q._source_row, r.item AS reason
FROM rsingh_gdl_bronze.quarantine q, q._reject_reasons r
ORDER BY 1, 2, 4;
```

| Date | Entity | Rejected |
|---|---|---|
| D1 | `cbs_customer` | 3 with no `last_name`; 2 with an invalid date of birth (dump rows 153 and 355) |
| D2 | `cbs_eod_balance` | `ledger_balance` not a decimal |
| D2 | `lms_loan` | `dpd` not an integer |
| D2 | `pay_transaction` | `currency` not an allowed value |
| every date | `pay_transaction` | 2 with no `amount` |
| D3 | `doc_document` | an e-mail file below the minimum size |

Each reject keeps the source file, the row number and the raw record, so the source can fix
it.

**5. Nothing is lost (1 min).**

```sql
SELECT entity, status, rows_in, rows_out, rows_rejected
FROM rsingh_gdl_ref.load_audit
WHERE batch_id = 'B20260921' AND stage = 'bronze' ORDER BY ended_at;
```

`cbs_customer` 966 in, 961 accepted, 5 rejected; the D1 batch 6,625 in, 6,618 accepted, 7
rejected. Reconciliation checks accepted + quarantined against the manifest for every entity.

**6. Schema checks (1 min).**

- `SELECT * FROM rsingh_gdl_ref.schema_drift`: payments gained `device` on D4; logged, the
  load does not fail.
- `SELECT * FROM rsingh_gdl_ref.file_control WHERE data_records <> trailer_records`: the D3
  repayments trailer says 13, the file has 12 - the one reconciliation mismatch left on D3.
- Close on the dashboard "GDL Reconciliation & Data Quality".

## Common questions

- **"Can it pull from a live database?"** The ingest is contract-driven and the parser is per
  format; a JDBC read from MySQL or Postgres would feed the same validation, quarantine and
  audit. Not built, because the environment has no source database.
- **"What if a new column appears?"** It is logged in `ref.schema_drift` and the record still
  loads; the contract is updated deliberately, not automatically.
- **"What if a run fails half way?"** See the failed-batch drill,
  [docs/FAILED_BATCH_DEMO.md](../docs/FAILED_BATCH_DEMO.md).
