-- Iceberg time travel, for Hue (Impala). Hue asks for the ${...} variables; the first query
-- lists the snapshot ids each batch recorded in ref.load_audit, to paste into the others.
-- Spark SQL accepts the same FOR SYSTEM_VERSION / FOR SYSTEM_TIME clauses;
-- scripts/run_semantic.py rewrites DESCRIBE HISTORY for Spark and runs every query here.
--
-- Time travel answers "what did the table hold at that moment" (a snapshot, including
-- mistakes later corrected). SCD2 answers "what was true for the business on that date"
-- (history kept as rows, queryable without snapshots). The last query shows both for one party.

-- name: batch_snapshots
-- Snapshot of each table before and after every committed batch step.
SELECT batch_id, stage, entity, status, snapshot_before, snapshot_after, started_at, run_id
FROM rsingh_gdl_ref.load_audit
WHERE entity = '${entity}' AND snapshot_after IS NOT NULL
ORDER BY started_at;

-- name: table_history
-- Every snapshot of the golden record table: one per MDM run.
DESCRIBE HISTORY rsingh_gdl_mdm.golden_party;

-- name: golden_record_as_of_batch
-- A customer's golden record as each batch left it, before and after the address change.
SELECT 'before' AS as_of, party_id, full_name, address, pincode, address_city, mobile, email
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF ${snapshot_before}) b
WHERE party_id = '${party_id}'
UNION ALL
SELECT 'after' AS as_of, party_id, full_name, address, pincode, address_city, mobile, email
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF ${snapshot_after}) a
WHERE party_id = '${party_id}';

-- name: golden_record_as_of_time
-- The same record by wall-clock time instead of snapshot id.
SELECT party_id, address, pincode, address_city
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_TIME AS OF '${as_of_time}') t
WHERE party_id = '${party_id}';

-- name: snapshot_diff
-- Golden records whose survived attributes changed between two snapshots.
SELECT a.party_id,
       b.address AS address_before, a.address AS address_after,
       b.mobile  AS mobile_before,  a.mobile  AS mobile_after,
       b.email   AS email_before,   a.email   AS email_after
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF ${snapshot_after}) a
JOIN (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF ${snapshot_before}) b
  ON b.party_id = a.party_id
WHERE COALESCE(a.address, '') <> COALESCE(b.address, '')
   OR COALESCE(a.mobile, '') <> COALESCE(b.mobile, '')
   OR COALESCE(a.email, '') <> COALESCE(b.email, '')
ORDER BY a.party_id;

-- name: parties_new_in_batch
-- Parties the later snapshot has and the earlier one does not (new customers, or new clusters).
SELECT a.party_id, a.full_name, a.source_systems
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF ${snapshot_after}) a
LEFT ANTI JOIN (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF ${snapshot_before}) b
  ON b.party_id = a.party_id
ORDER BY a.party_id;

-- name: kpi_as_previously_reported
-- The CASA ratio a regulatory dataset reported, from the snapshot written by that load,
-- whatever restatements came later.
SELECT reporting_date, SUM(CASE WHEN is_casa THEN balance_inr ELSE 0 END) AS casa_balance,
       SUM(balance_inr) AS total_deposits,
       CAST(SUM(CASE WHEN is_casa THEN balance_inr ELSE 0 END) AS DOUBLE) / CAST(SUM(balance_inr) AS DOUBLE) AS casa_ratio
FROM (SELECT * FROM rsingh_gdl_semantic.reg_deposit_composition FOR SYSTEM_VERSION AS OF ${reg_snapshot}) r
GROUP BY reporting_date
ORDER BY reporting_date;

-- name: scd2_versus_time_travel
-- The same customer through SCD2: every business version as rows, in the current snapshot.
SELECT party_id, version, effective_from, effective_to, is_current, change_reason, end_reason,
       address, pincode, src_record_ids, created_batch_id
FROM rsingh_gdl_gold.dim_party
WHERE party_id = '${party_id}'
ORDER BY version;
