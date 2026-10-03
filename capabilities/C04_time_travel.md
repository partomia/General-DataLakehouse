# C04. Time travel and table snapshots

## Summary

Every table is Iceberg, so every commit is a snapshot. The pipeline records the snapshot id of
each table before and after every batch in `rsingh_gdl_ref.load_audit`, so "as the D3 batch
left it" is a lookup, not a guess. Impala reads a past snapshot with
`FOR SYSTEM_VERSION AS OF <snapshot id>` or `FOR SYSTEM_TIME AS OF '<timestamp>'`. All the
queries are in `sql/time_travel.sql` (Hue asks for the `${...}` values).

The example: Shreya Naidu (`P0A4F2E33DF8E`) e-mailed an address change on D3 and another on D4.

## Walkthrough (about 6 minutes)

**1. Which snapshot each batch wrote (1 min).**

```sql
SELECT batch_id, stage, entity, snapshot_before, snapshot_after, started_at
FROM rsingh_gdl_ref.load_audit
WHERE entity = 'golden_party' AND snapshot_after IS NOT NULL ORDER BY started_at;
```

| Batch | Snapshot after |
|---|---|
| B20260921 | 5928431924475437446 |
| B20260922 | 8050324314265930857 |
| B20260923 | 3946087871829650566 |
| B20260924 | 3455461364467914291 |
| B20260925 | 8029040660417881821 |

**2. The record as it stood in the past (2 min).**

```sql
SELECT party_id, full_name, address, pincode
FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF 8050324314265930857
WHERE party_id = 'P0A4F2E33DF8E';
```

| As of | Address |
|---|---|
| D2 (8050...0857) | 35, Station Road, Near Water Tank, Pune |
| D3 (3946...0566) | 194, Shivaji Nagar, Near Bus Depot, Pune - 411036 |
| D4 (3455...4291) | 233, Park Street, Near Railway Station, Kochi - 682041 |

By wall-clock time instead of an id:

```sql
SELECT party_id, address FROM rsingh_gdl_mdm.golden_party
FOR SYSTEM_TIME AS OF '2026-10-02 06:30:00' WHERE party_id = 'P0A4F2E33DF8E';
```

returns the D3 address: at 06:30 on 2 October the D3 batch had committed and D4 had not.

**3. The snapshots (1 min).**

```sql
DESCRIBE HISTORY rsingh_gdl_mdm.golden_party;
SELECT snapshot_id, committed_at, operation FROM rsingh_gdl_mdm.golden_party.snapshots;
```

Five snapshots, one per MDM run. For a table changed by MERGE and append, show
`DESCRIBE HISTORY rsingh_gdl_silver.cbs_customer`: six snapshots, each the parent of the next.
(`golden_party` is rebuilt in full per batch, so each snapshot starts a new chain; time travel
to any of them still works, as step 2 shows.)

**4. What changed between two snapshots (1 min).** `snapshot_diff` in
`sql/time_travel.sql`, with D3 and D4: every golden record whose address, mobile or e-mail
changed that day.

**5. Time travel versus SCD2 (1 min).** The same customer in `gold.dim_party`, in the current
snapshot:

```sql
SELECT version, effective_from, effective_to, is_current, change_reason, changed_attributes, address
FROM rsingh_gdl_gold.dim_party WHERE party_id = 'P0A4F2E33DF8E' ORDER BY version;
```

Three versions: D1-D2, D3, and D4 onwards. Time travel answers "what did the table hold at that
moment, including mistakes later corrected"; SCD2 answers "what was true for the business on
that date", without snapshots.

## Also worth showing

- **The regulatory figure as submitted.** `kpi_as_previously_reported` reads
  `semantic.reg_deposit_composition` at the snapshot of the load that produced the return, so
  the CASA ratio sent to the regulator can be reproduced after any restatement.
- **The pipeline itself uses time travel.** Silver, MDM and gold read their inputs as the
  batch committed them (`gdl_common.read_as_of_batch`), so re-running a later stage for an old
  date reads the same data it read the first time.
- **Rollback.** `CALL system.rollback_to_snapshot('rsingh_gdl_bronze.lms_loan', <id>)` in Spark
  (see C07).

## Common questions

- **"How long are snapshots kept?"** Until `expire_snapshots` runs; nothing expires them in
  this environment. In production, a retention set by the regulatory need (for example the reporting
  period), with tags on the snapshots of submitted returns so they are kept.
