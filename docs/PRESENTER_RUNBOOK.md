# Presenter runbook: slides 2 to 17

The commands, jobs and queries behind each section of the presenter guide (the PDF that
explains the deck slide by slide). One section per slide, in the same order, with the guide's
page numbers. Everything here was run against the cluster on 4 October 2026; the expected
results are the ones the deck quotes (business dates 21 to 25 September 2026, synthetic data).

- **SQL** runs in Hue (CDW Impala editor) as the workload user unless a step says otherwise.
  Every table is in one of the six databases `rsingh_gdl_{bronze,silver,mdm,gold,semantic,ref}`.
- **Shell** commands run from the repository root, with the CDE CLI configured for the
  virtual cluster and the credentials in environment variables (never in the repository).
- **Capability documents** (C01 to C09) in [`capabilities/`](../capabilities/README.md) carry
  the longer walkthrough for each topic.

## Before the demo (about 10 minutes, the day before and again an hour before)

```bash
export GDL_IMPALA_USER=... GDL_IMPALA_PASSWORD=...      # CDW Impala
export GDL_WORKLOAD_USER=... GDL_WORKLOAD_PASSWORD=...  # Atlas and Ranger (the same user)
export GDL_VIZ_API_KEY=...                              # Data Visualization API

python scripts/governance.py verify            # exit 0: every PII column tagged, masks in place
python dataviz/build_dashboard.py --verify     # every visual through CDV; KPI tiles = Impala
cde run list --filter 'job[eq]rsingh-gdl-orchestration'   # DAG runs 181 ... 217, 199 failed
```

If `verify` fails (a table was recreated since the last apply):
`python scripts/governance.py apply && python scripts/governance.py verify`.

Quick health check in Hue:

```sql
SELECT batch_id, status, COUNT(*) AS checks
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260925' AND run_id LIKE 'reconcile%'
GROUP BY batch_id, status;
-- MATCHED 48, EXPLAINED 4
```

Masking check for slide 13 (not yet proven in CDV): sign in to CDV as federal01, open
"GDL MDM & Golden Record". Masked PAN and hashed names mean CDV runs queries as the signed-in
user and the claim can be made; clear values mean show masking in Hue only (slide 14).

Tabs to have open: CDE (Jobs, Job Runs, Airflow UI), Hue (Impala), Data Visualization,
Atlas (Data Catalog), Ranger, GitHub Actions.

---

## Slide 2 · Cloudera Data Platform architecture (guide pages 3 to 4)

Nothing to run: framing slide.

## Slide 3 · Indian Bank DLH: solution architecture (guide pages 5 to 6)

Nothing to run: framing slide.

---

## Slide 4 · Use case architecture by layer (guide pages 7 to 8) · C03, C07

Optional, to show the layers are real Iceberg tables:

```sql
SHOW TABLES IN rsingh_gdl_gold;
SHOW CREATE TABLE rsingh_gdl_gold.dim_party;   -- STORED AS ICEBERG, 'format-version'='2'
```

One business date through every stage (the batch trail):

```sql
SELECT stage, status, rows_in, rows_out, rows_rejected, started_at, ended_at
FROM rsingh_gdl_ref.load_audit
WHERE batch_id = 'B20260925' AND entity = '*' AND status IN ('COMPLETED', 'FAILED')
ORDER BY ended_at;
-- bronze, silver, mdm, gold: COMPLETED
```

How a date is run (reference; not live, the dates are loaded):

```bash
cde job run --name rsingh-gdl-orchestration --config-json '{"business_date": "2026-09-25"}'
python scripts/run_semantic.py --engine impala --dates 2026-09-25 --steps load,check
```

The DAG runs land, bronze, silver, mdm, gold, reconcile and the `batch_complete` gate; the
semantic layer runs after it on Impala.

---

## Slide 5 · Unified ingestion: six sources, three shapes (guide pages 9 to 10) · C03, C01

**Airflow.** CDE › Jobs › `rsingh-gdl-orchestration` › Airflow UI › DAG `general_datalakehouse`,
Grid view: one run per business date, run 199 (23 Sep) red. From the CLI:

```bash
cde run list --filter 'job[eq]rsingh-gdl-orchestration'
cde run describe --id 199      # conf: business_date 2026-09-23, bronze_mode fail-during:lms_loan
```

**The landing zone.** Hue › Files ›
`s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing/`, one folder per source and date:

| Open | What to point at |
|---|---|
| `cbs/2026-09-21/cbs_dump_20260921.sql` | the MySQL dump: `CREATE TABLE`, extended `INSERT`s |
| `cbs/2026-09-25/cbs_cdc_20260925.jsonl` | CDC events: `op`, `before`, `after`, `ts_ms`, binlog position |
| `lms/2026-09-25/lms_loan_20260925.csv` | pipe-delimited, `dd/MM/yyyy`, the `T\|count\|total` trailer on the last line |
| `payments/2026-09-24/payments_20260924.jsonl` | nested JSON; the new `device` object |
| `crm/2026-09-25/crm_customers_20260925.json` | one JSON array, arrays of identifiers and addresses |
| `documents/2026-09-24/` | `.eml` e-mails, `KYC-*.txt`, `SCAN-*.png` |
| `compliance/2026-09-25/aml_watchlist_20260925.csv` | the screening list |
| any `_manifest.json` | the packing slip: records, control totals, bytes, sha256 per file |

**What each source delivered on 25 Sep, as loaded:**

```sql
SELECT entity, rows_in, rows_out AS accepted, rows_rejected AS quarantined, message
FROM rsingh_gdl_ref.load_audit
WHERE batch_id = 'B20260925' AND stage = 'bronze' AND status = 'COMMITTED'
ORDER BY entity;
```

**CDC: a dump once, then changes:**

```sql
SELECT `table`, op, COUNT(*) AS events
FROM rsingh_gdl_bronze.cbs_cdc_event
WHERE _business_date = DATE '2026-09-25'
GROUP BY `table`, op ORDER BY 1, 2;
```

**Schema drift: the `device` field, kept and logged:**

```sql
SELECT business_date, entity, kind, field, records
FROM rsingh_gdl_ref.schema_drift ORDER BY business_date, entity;
-- from 2026-09-24: pay_transaction NEW_FIELD device
```

**Unstructured: what silver extracted from e-mails and KYC documents:**

```sql
SELECT file_name, doc_type, intent, extraction_status, pan_std, mobile_e164, account_key, cust_id
FROM rsingh_gdl_silver.doc_extract
WHERE business_date = DATE '2026-09-24'
ORDER BY file_name;
```

---

## Slide 6 · Bronze: data contracts and data quality (guide pages 11 to 12) · C01, C07

**A contract is configuration:** GitHub › `contracts/lms_loan.json` (format, key, types,
`dd/MM/yyyy`, the `^LN[0-9]{8}$` pattern, severity).

**Quarantine, by reason:**

```sql
SELECT q._entity, r.item AS reason, COUNT(*) AS records
FROM rsingh_gdl_bronze.quarantine q, q._reject_reasons r
GROUP BY q._entity, r.item
ORDER BY records DESC;
```

**One quarantined record, back to its file and line:**

```sql
SELECT q._business_date, q._entity, q._source_file, q._source_row, r.item AS reason, q._record
FROM rsingh_gdl_bronze.quarantine q, q._reject_reasons r
WHERE q._entity = 'lms_loan';
-- lms_loan_20260922.csv, dpd:NOT_INTEGER (the planted "abc")
```

**Reconciliation for 25 Sep:**

```sql
SELECT layer, entity, check_name, expected, actual, status, detail
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260925' AND run_id LIKE 'reconcile%'
ORDER BY CASE status WHEN 'MISMATCH' THEN 0 WHEN 'EXPLAINED' THEN 1 ELSE 2 END, layer, entity;
-- 48 MATCHED, 4 EXPLAINED, 0 MISMATCH
```

**If asked for a mismatch: 23 Sep, the planted trailer:**

```sql
SELECT layer, entity, check_name, expected, actual, detail
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260923' AND run_id LIKE 'reconcile%' AND status = 'MISMATCH';
-- lms_repayment file_trailer: trailer says 13, file has 12 data lines

SELECT source_file, data_records, trailer_records, trailer_total
FROM rsingh_gdl_ref.file_control
WHERE batch_id = 'B20260923' AND entity = 'lms_repayment';
```

**Operational metadata (bottom band):**

```sql
SELECT entity, status, rows_in, rows_out, snapshot_before, snapshot_after
FROM rsingh_gdl_ref.load_audit
WHERE batch_id = 'B20260925' AND stage = 'bronze' ORDER BY ended_at;

SELECT job, step, transform_type, source_tables, target_table, rows_in, rows_out
FROM rsingh_gdl_ref.transform_log
WHERE batch_id = 'B20260925' AND job = 'ingest_bronze' ORDER BY logged_at;
```

---

## Slide 7 · MDM: standardise, match, cluster (guide pages 13 to 14) · C08

**Dashboard:** CDV › "GDL MDM & Golden Record" › Matching sheet (pairs by rule and decision,
the review queue, match quality).

**Pairs by rule and decision:**

```sql
SELECT rule, decision, COUNT(*) AS pairs
FROM rsingh_gdl_mdm.match_pair GROUP BY rule, decision ORDER BY pairs DESC;
-- PAN_EXACT 706, MOBILE_DOB 273, PAN_CONFLICT 65, SOURCE_XREF 21,
-- NAME_DOB_PIN 5, PRIOR_LINK 1, NAME_SUBSET_DOB_PIN 1 (REVIEW)
```

**1,812 records into 1,009 parties, 616 of them merged:**

```sql
SELECT COUNT(*) AS source_records, COUNT(DISTINCT party_id) AS parties
FROM rsingh_gdl_mdm.party_xref;

SELECT cluster_size, COUNT(DISTINCT party_id) AS parties
FROM rsingh_gdl_mdm.party_xref GROUP BY cluster_size ORDER BY cluster_size;
-- 1: 393, 2: 445, 3: 155, 4: 16
```

**The namesakes, never merged:**

```sql
SELECT id_a, id_b, rule, decision, name_similarity, name_a, name_b, same_dob, same_pan
FROM rsingh_gdl_mdm.match_pair
WHERE rule = 'PAN_CONFLICT' AND name_similarity >= 0.9;
-- cbs:100499 / cbs:100966, SUNIL KUMAR, similarity 1.0, same DOB, PANs differ: NO_MATCH
```

**The review case, and the earlier link that keeps it:**

```sql
SELECT id_a, id_b, rule, decision, score, name_a, name_b, party_a, party_b
FROM rsingh_gdl_mdm.match_pair
WHERE decision = 'REVIEW' OR rule = 'PRIOR_LINK';
-- RAHUL RAMESH IYER / RAHUL IYER: NAME_SUBSET_DOB_PIN REVIEW, and PRIOR_LINK MERGE, party P99945510AB18
```

**Match quality against the answer key:**

```sql
SELECT business_date, source_records, parties, true_pairs, predicted_pairs, correct_pairs,
       precision, recall, split_persons, merged_persons
FROM rsingh_gdl_mdm.match_quality ORDER BY business_date;
-- precision 1.0, recall 1.0 every date
```

---

## Slide 8 · Golden record: survivorship per attribute (guide page 15) · C08, C02

**Arun Agarwal, `P97F981593876`: 4 records from 3 systems.**

```sql
SELECT src_system, src_key, match_rule, match_confidence, cluster_size
FROM rsingh_gdl_mdm.party_xref WHERE party_id = 'P97F981593876';
-- cbs:100758, crm:CRM-98329341, lms:LB200257, lms:LB200258 (the 2 LMS duplicates)

SELECT candidate_id, name_std, dob, pan_std, mobile_e164, email_std, record_ts, kyc_status
FROM rsingh_gdl_mdm.party_candidate
WHERE candidate_id IN ('cbs:100758', 'crm:CRM-98329341', 'lms:LB200257', 'lms:LB200258');

SELECT attribute, value, src_system, src_key, distinct_values, rule
FROM rsingh_gdl_mdm.golden_attribute WHERE party_id = 'P97F981593876' ORDER BY attribute;
-- identity from CBS (KYC verified), e-mail and mobile from CRM (most recent)

SELECT party_id, full_name, dob, pan, mobile, email, address, source_systems, member_records,
       attribute_sources
FROM rsingh_gdl_mdm.golden_party WHERE party_id = 'P97F981593876';
```

**The customer's own e-mail beats the older CBS address (Shreya Naidu):**

```sql
SELECT attribute, value, src_system, src_key, rule
FROM rsingh_gdl_mdm.golden_attribute
WHERE party_id = 'P0A4F2E33DF8E' AND attribute = 'address';
-- doc: EML-20260924-0001.eml

SELECT file_name, doc_type, intent, linked_via, cust_key, party_id
FROM rsingh_gdl_mdm.document_party WHERE party_id = 'P0A4F2E33DF8E';
```

**The live golden record (optional, about 4 minutes on CDE):**

```bash
cde job run --name rsingh-gdl-mdm-live --arg=--business-date --arg=2026-09-25
```

The two records are in `config/mdm_live_pair.json` (Meera Krishnan in CBS and CRM). To change a
value live, pass the pair instead:
`--arg=--records-json --arg='{"records": [...]}'`. Then, in Hue (another engine than the Spark
job that wrote it):

```sql
SELECT id_a, id_b, rule, decision, score, name_similarity, name_a, name_b, party_a, run_id
FROM rsingh_gdl_mdm.live_match_pair;
-- NAME_DOB_PIN MERGE 0.9286

SELECT party_id, full_name, dob, pan, mobile, email, address, source_systems, attribute_sources
FROM rsingh_gdl_mdm.live_golden_party;
-- PC05F2F1E8685, mobile and e-mail from CRM

SELECT step, transform_type, source_tables, target_table, rows_in, rows_out, details
FROM rsingh_gdl_ref.transform_log
WHERE job = 'mdm_live' ORDER BY logged_at DESC LIMIT 5;
```

**Never dropped: the UNKNOWN party and the party_resolved check:**

```sql
SELECT business_date, COUNT(*) AS rows_on_unknown
FROM rsingh_gdl_gold.fact_deposit_balance_daily
WHERE party_id = 'UNKNOWN' GROUP BY business_date ORDER BY business_date;

SELECT entity, actual, status, detail
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260925' AND run_id LIKE 'reconcile%' AND check_name = 'party_resolved';
```

---

## Slide 9 · Banking data model: four domains plus AML (guide pages 16 to 17) · C09

**One customer across domains (Arun Agarwal):**

```sql
SELECT `role`, domain, account_key
FROM rsingh_gdl_gold.bridge_party_account WHERE party_id = 'P97F981593876';
-- PRIMARY DEPOSITS ACC:CBS:..., BORROWER LENDING LN:LMS:LN10000283, LN10000284

SELECT f.business_date, p.full_name, p.version AS party_version, a.account_key, a.product_code,
       f.deposit_type, f.is_casa, f.balance_inr
FROM rsingh_gdl_gold.fact_deposit_balance_daily f
JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = f.party_sk
JOIN rsingh_gdl_gold.dim_account a ON a.account_sk = f.account_sk
WHERE f.party_id = 'P97F981593876'
ORDER BY f.business_date;

SELECT business_date, loan_key, principal_outstanding, dpd, sma_status, asset_class, is_npa,
       provision_rate, provision_amount
FROM rsingh_gdl_gold.fact_loan_position_daily
WHERE party_id = 'P97F981593876'
ORDER BY loan_key, business_date;
```

**SMA and IRAC across the book on 25 Sep:**

```sql
SELECT sma_status, asset_class, COUNT(*) AS loans, SUM(principal_outstanding) AS outstanding,
       SUM(provision_amount) AS provision
FROM rsingh_gdl_gold.fact_loan_position_daily
WHERE business_date = DATE '2026-09-25'
GROUP BY sma_status, asset_class ORDER BY asset_class, sma_status;
```

**Conformed dimensions: the Indian fiscal year:**

```sql
SELECT calendar_date, date_key, fiscal_year, fiscal_quarter
FROM rsingh_gdl_gold.dim_date
WHERE calendar_date IN (DATE '2026-03-31', DATE '2026-04-01', DATE '2026-09-25');
```

---

## Slide 10 · History: SCD Type 2 and Iceberg time travel (guide pages 18 to 19) · C04, C05

If asked about late-arriving data or a back-dated correction: the SCD2 build is forward only;
see C05 for the honest answer.

**SCD2: Shreya Naidu's three versions:**

```sql
SELECT party_id, version, effective_from, effective_to, is_current, change_reason,
       changed_attributes, end_reason, address, pincode, src_record_ids, created_batch_id
FROM rsingh_gdl_gold.dim_party WHERE party_id = 'P0A4F2E33DF8E' ORDER BY version;
-- v1 NEW 21 to 22 Sep, v2 CHANGED 23 Sep, v3 CHANGED from 24 Sep, is_current (Kochi)
```

**Facts point at the version valid on their date:**

```sql
SELECT f.business_date, f.party_sk, p.version
FROM rsingh_gdl_gold.fact_deposit_balance_daily f
JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = f.party_sk
WHERE f.party_id = 'P0A4F2E33DF8E'
ORDER BY f.business_date;
-- v1 on 21-22 Sep, v2 on 23 Sep, v3 on 24-25 Sep
```

**Time travel.** In Hue open `sql/time_travel.sql` (it prompts for the `${...}` values), or run
these with the values filled in:

```sql
-- batch_snapshots: the snapshot ids each batch recorded
SELECT batch_id, stage, entity, status, snapshot_before, snapshot_after, started_at
FROM rsingh_gdl_ref.load_audit
WHERE entity = 'golden_party' AND snapshot_after IS NOT NULL ORDER BY started_at;
-- 24 Sep (B20260924) after: 3455461364467914291, 23 Sep after: 3946087871829650566

DESCRIBE HISTORY rsingh_gdl_mdm.golden_party;

-- golden_record_as_of_batch: before and after the address-change e-mail
SELECT 'before' AS as_of, party_id, full_name, address, pincode, address_city
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF 3946087871829650566) b
WHERE party_id = 'P0A4F2E33DF8E'
UNION ALL
SELECT 'after' AS as_of, party_id, full_name, address, pincode, address_city
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF 3455461364467914291) a
WHERE party_id = 'P0A4F2E33DF8E';
-- before: 194, Shivaji Nagar, ... Pune, after: 233, Park Street, ... Kochi

-- golden_record_as_of_time: by clock time
SELECT party_id, address, pincode, address_city
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_TIME AS OF '2026-10-02 06:30:00') t
WHERE party_id = 'P0A4F2E33DF8E';
-- the 23 Sep address (Pune - 411036)

-- snapshot_diff: every golden record the 24 Sep batch changed
SELECT a.party_id, b.address AS address_before, a.address AS address_after,
       b.mobile AS mobile_before, a.mobile AS mobile_after
FROM (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF 3455461364467914291) a
JOIN (SELECT * FROM rsingh_gdl_mdm.golden_party FOR SYSTEM_VERSION AS OF 3946087871829650566) b
  ON b.party_id = a.party_id
WHERE COALESCE(a.address, '') <> COALESCE(b.address, '')
   OR COALESCE(a.mobile, '') <> COALESCE(b.mobile, '')
ORDER BY a.party_id;

-- kpi_as_previously_reported: the CASA dataset as the 22 Sep load left it
SELECT reporting_date,
       CAST(SUM(CASE WHEN is_casa THEN balance_inr ELSE 0 END) AS DOUBLE)
         / CAST(SUM(balance_inr) AS DOUBLE) AS casa_ratio
FROM (SELECT * FROM rsingh_gdl_semantic.reg_deposit_composition
      FOR SYSTEM_VERSION AS OF 6487990241457738144) r
GROUP BY reporting_date ORDER BY reporting_date;
-- 21 and 22 Sep only: what was reported then
```

---

## Slide 11 · Source-to-target mapping and extending the model (guide pages 20 to 21) · C09, C06

**The mapping, as a table:**

```sql
SELECT target_column, source_system, source_entity, source_field, transform_type, rule
FROM rsingh_gdl_ref.source_mapping
WHERE target_table = 'dim_party' ORDER BY target_column;

SELECT * FROM rsingh_gdl_ref.source_mapping
WHERE target_table = 'fact_loan_position_daily'
  AND target_column IN ('is_npa', 'sma_status', 'asset_class', 'principal_outstanding');

SELECT target_table, COUNT(*) AS columns_mapped
FROM rsingh_gdl_ref.source_mapping GROUP BY target_table ORDER BY target_table;
-- 224 rows, fact_aml_alert and dim_aml_rule are the 29 AML rows
```

The file: GitHub › `model/source_mapping.csv`. The rendered tables in
`docs/BANKING_MODEL.md` come from `python scripts/render_mapping.py`.

**The AML extension, by file** (GitHub): `contracts/aml_watchlist.json` (contract),
`config/pipeline.json` (one entry), `config/aml_rules.json` (the rules),
`sql/semantic/60_aml_views.sql` (the view), CDV "GDL AML Alerts".

```sql
SELECT business_date, rule_code, severity, COUNT(*) AS alerts
FROM rsingh_gdl_gold.fact_aml_alert
WHERE business_date = DATE '2026-09-25'
GROUP BY business_date, rule_code, severity ORDER BY severity;
-- 5 alerts, 2 CRITICAL

SELECT layer, entity, check_name, expected, actual, status, detail
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260925' AND run_id LIKE 'reconcile%' AND entity = 'fact_aml_alert';
-- the rules checked against the planted truth
```

---

## Slide 12 · Semantic layer: KPIs certified once, read three ways (guide pages 22 to 23) · C06

**The certified definitions:**

```sql
SELECT kpi_code, kpi_name, owner, version, certified_on, certified_view, grain, formula
FROM rsingh_gdl_ref.kpi_definition;

SELECT kpi_code, parameter, value, applies_to FROM rsingh_gdl_ref.kpi_parameter ORDER BY kpi_code;
```

**The same NPA figure three ways (25 Sep):**

```sql
-- certified view
SELECT SUM(gross_npa) / SUM(gross_advance) AS gross_npa_ratio
FROM rsingh_gdl_semantic.kpi_npa_exposure WHERE reporting_date = DATE '2026-09-25';

-- MIS (what the dashboard tile reads)
SELECT reporting_date, gross_npa, gross_advances, gross_npa_ratio
FROM rsingh_gdl_semantic.mis_npa_trend WHERE is_latest = 1;

-- regulatory dataset
SELECT SUM(CASE WHEN is_npa THEN outstanding ELSE 0 END) / SUM(outstanding) AS gross_npa_ratio
FROM rsingh_gdl_semantic.reg_asset_classification WHERE reporting_date = DATE '2026-09-25';
-- 0.1129 in all three
```

Ad-hoc: in Hue open `sql/adhoc.sql`, query `npa_borrowers` (NPA borrowers with their golden
record).

**CASA and CRV:**

```sql
SELECT reporting_date, casa_ratio FROM rsingh_gdl_semantic.mis_casa_trend WHERE is_latest = 1;
-- 0.4073

SELECT COUNT(DISTINCT party_id) AS parties, SUM(crv) AS crv
FROM rsingh_gdl_semantic.kpi_customer_relationship_value
WHERE reporting_date = DATE '2026-09-25';
-- 991, 22,670,829.96
```

**The consistency check (23 checks):**

```sql
SELECT entity, check_name, expected, actual, status
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260925' AND layer = 'semantic' ORDER BY entity, check_name;
```

Re-run it live (about a minute, safe):
`python scripts/run_semantic.py --engine impala --dates 2026-09-25 --steps check`.

**"Prove the 11.29%": one loan down every layer to the source line:**

```sql
SELECT loan_key, gross_advance, gross_npa, asset_class
FROM rsingh_gdl_semantic.kpi_npa_exposure
WHERE loan_key = 'LN:LMS:LN10000216' AND reporting_date = DATE '2026-09-25';

SELECT loan_key, principal_outstanding, dpd, is_npa, asset_class, src_position
FROM rsingh_gdl_gold.fact_loan_position_daily
WHERE loan_key = 'LN:LMS:LN10000216' AND business_date = DATE '2026-09-25';

SELECT loan_id, principal_outstanding, dpd, _src_position
FROM rsingh_gdl_silver.lms_loan_daily
WHERE loan_id = 'LN10000216' AND as_of_date = DATE '2026-09-25';

SELECT loan_id, principal_outstanding, dpd, _source_file, _source_row, _batch_id
FROM rsingh_gdl_bronze.lms_loan
WHERE loan_id = 'LN10000216' AND _business_date = DATE '2026-09-25';
-- 6,794,000.00, DPD 462, DOUBTFUL_1, lms_loan_20260925.csv row 217
```

Then open the file in Hue › Files at line 217.

---

## Slide 13 · Data Visualization: four dashboards, built as code (guide page 24) · C06 (hop 7)

**Open in CDV:** "GDL Banking KPIs MIS" (NPA tile 11.29%), "GDL Reconciliation & Data Quality"
(25 Sep 48 / 4 / 0, KPI consistency), "GDL MDM & Golden Record" (review queue),
"GDL AML Alerts".

**Dashboards as code:**

```bash
python dataviz/build_dashboard.py --verify    # every visual through the Data API; tiles = Impala
python dataviz/build_dashboard.py --check     # every visual's query directly on Impala
```

The tile definition: GitHub › `dataviz/build_dashboard.py`, `title="Gross NPA ratio %"` on
dataset `"GDL - NPA trend"` = `rsingh_gdl_semantic.mis_npa_trend`.

**No extracts: what a dataset reads:**

```sql
SELECT * FROM rsingh_gdl_semantic.dash_recon WHERE is_latest = 1;
```

---

## Slide 14 · One security, one governance: SDX as code (guide pages 25 to 26) · C06 (Atlas)

**Same query, two users.** Run in Hue as federal01, then as rsingh:

```sql
SELECT party_id, full_name, dob, pan, mobile, email, address
FROM rsingh_gdl_gold.dim_party
WHERE party_id = 'P97F981593876' AND is_current;
-- federal01: name hashed, PAN and mobile last 4, DOB 1 January, address redacted
-- rsingh: clear values
```

**Whole-record columns (federal01):**

```sql
SELECT _entity, _source_file, _record FROM rsingh_gdl_bronze.quarantine LIMIT 3;   -- redacted
SELECT file_name, text_content, content FROM rsingh_gdl_bronze.doc_document LIMIT 3;  -- redacted, NULL
```

**Governance as code:**

```bash
python scripts/governance.py            # plan: what apply would change (read-only)
python scripts/governance.py verify     # exit 1 unless every tag, term and policy is in place
python scripts/profiler_rules.py evaluate   # the 7 profiler tag rules against every column
```

**Atlas:** search `rsingh_gdl_gold.dim_party`, column `pan` › classification `GDL_PII_LAST_4`.
Glossary "GDL Banking KPIs" › term "NPA exposure" › assigned entities. Lineage tab on
`rsingh_gdl_semantic.kpi_npa_exposure`: upstream to `fact_loan_position_daily`,
`silver.lms_loan_daily`, `bronze.lms_loan`; downstream to `mis_npa_trend` and
`reg_asset_classification`.

**Ranger:** Tag-based policies › the `rsingh-gdl-pii-*` masking policies (one per tag:
LAST_4, HASH, REDACT, YEAR, NULL).

**Data Catalog profilers:** Data Catalog › Profilers › Tag rules: the 4 created (PAN, Aadhaar,
Mobile, Account number); create the other 3 live from
[PROFILER_TAG_RULES.md](PROFILER_TAG_RULES.md).

---

## Slide 15 · Reconciliation and the failed-batch re-run (guide page 27) · C07

**Airflow:** DAG `general_datalakehouse`, run 199 (23 Sep, red: bronze failed, silver to gold
skipped, reconcile green, `batch_complete` red), then run 203 (resume, green). The triggers:

```bash
cde job run --name rsingh-gdl-orchestration \
  --config-json '{"business_date": "2026-09-23", "bronze_mode": "fail-during:lms_loan"}'
cde job run --name rsingh-gdl-orchestration \
  --config-json '{"business_date": "2026-09-23", "bronze_mode": "resume"}'
```

Do not run them live on this cluster (21 to 25 Sep are loaded; SCD2 builds forward only).

**What the failed attempt left, and what the resume did:**

```sql
SELECT run_id, entity, status, rows_out, snapshot_after, ended_at, message
FROM rsingh_gdl_ref.load_audit
WHERE batch_id = 'B20260923' AND stage = 'bronze' AND ended_at > '2026-10-02 06:00:00'
ORDER BY ended_at;
-- failed run: cbs_cdc_event, cbs_eod_balance, lms_borrower COMMITTED, then * FAILED
-- resume: those 3 SKIPPED, the rest COMMITTED, * COMPLETED
```

**Nothing half-written:**

```sql
DESCRIBE HISTORY rsingh_gdl_bronze.lms_loan;
-- 1 Oct 23:20 and 23:34, then 2 Oct 06:20:43 (the resume): none from the failure at 06:15-06:16

SELECT _batch_id, COUNT(*) AS loan_rows
FROM rsingh_gdl_bronze.lms_loan GROUP BY _batch_id ORDER BY _batch_id;
-- B20260923: 361, once
```

**The failed run's own mismatch report, by time travel:**

```sql
SELECT status, COUNT(*) AS checks
FROM rsingh_gdl_ref.recon_results FOR SYSTEM_VERSION AS OF 3386754692264325777
WHERE batch_id = 'B20260923' GROUP BY status;
-- MATCHED 11, MISMATCH 19

SELECT layer, entity, check_name, expected, actual, detail
FROM rsingh_gdl_ref.recon_results FOR SYSTEM_VERSION AS OF 3386754692264325777
WHERE batch_id = 'B20260923' AND status = 'MISMATCH'
ORDER BY layer, entity, check_name;
```

**After the resume:**

```sql
SELECT status, COUNT(*) AS checks
FROM rsingh_gdl_ref.recon_results
WHERE batch_id = 'B20260923' AND run_id LIKE 'reconcile%' GROUP BY status;
-- MATCHED 47, EXPLAINED 4, MISMATCH 1 (the planted trailer)
```

**Roll back (say it, don't run it).** Spark SQL, with a `snapshot_before` from `load_audit`:

```sql
CALL spark_catalog.system.rollback_to_snapshot('rsingh_gdl_bronze.lms_loan', <snapshot_before>);
```

---

## Slide 16 · DevOps: everything is code and runs in CI (guide page 28)

```bash
gh run list --repo partomia/General-DataLakehouse --limit 5   # the last pushes, all green
gh run view --repo partomia/General-DataLakehouse <run-id>    # unit tests, pipeline, KPI checks, drill
```

Or GitHub › Actions › the latest run. Locally, the same checks:

```bash
.venv/bin/python -m pytest -q                                            # 68 tests
python scripts/run_local.py all --customers 200 --warehouse /tmp/w --landing /tmp/l   # 5 dates on local Spark
```

Deployment from the repository:

```bash
bash cde/scripts/deploy_jobs.sh     # the CDE repository and the Spark jobs
bash cde/scripts/deploy_dag.sh      # the Airflow DAG
cde repository sync --name rsingh-gdl-pipeline   # after a code change
```

---

## Slide 17 · What this use case proves against the RFP (guide page 29)

Nothing to run. If the panel wants a row backed, the capability documents are in
[`capabilities/`](../capabilities/README.md): C09 model, C08 and C02 master data, C04 and C05
history, C06 semantic layer and lineage, C03 and C01 ingestion and quality, C07 reconciliation.
