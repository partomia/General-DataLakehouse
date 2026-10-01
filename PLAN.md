# Build plan: General Data Lakehouse on Cloudera

A small, governed banking lakehouse for demos. Six synthetic source systems land
files (a MySQL dump, MySQL CDC events, CSV extracts with control trailers, nested
JSON, documents, and a compliance screening list), and CDE Spark carries them through bronze, silver, MDM and gold
on Iceberg v2. The output is a banking data model over four domains with SCD Type 2
dimensions, one golden customer record across all layers, and three certified KPIs
(NPA exposure, CASA ratio, customer relationship value) that MIS reports, ad-hoc
queries and regulatory datasets all read from the same definitions. Every batch is reconciled against the source's own control counts. A
batch that fails halfway can be re-run, and the demo shows what happened to the rows
already written.

Demo on synthetic data, deliberately small: about 1,000 customers and 5 business
days of batches, so a full run takes minutes and a laptop runs it too.

## Requirements map

| # | Requirement | Where it is met |
|---|---|---|
| 1 | Ingestion from files / RDBMS dump / CDC; record validation; schema | `cde/jobs/land_sources.py` (generator), `cde/jobs/ingest_bronze.py`, `contracts/*.json`, `bronze.quarantine` |
| 2 | Structured, semi-structured, unstructured sources | MySQL dump + CSV (structured); CDC JSON + nested JSON (semi); emails, KYC text, images (unstructured) |
| 3 | Medallion on Iceberg | `rsingh_gdl_{bronze,silver,mdm,gold,semantic,ref}`, Iceberg v2 throughout |
| 4, 9 | MDM: standardise, match, de-duplicate, survivorship, golden record, one truth across layers | `cde/jobs/build_mdm.py`, `mdm.party_xref` (source id -> `party_id`), every silver and gold row carries `party_id` |
| 5 | Time travel | `sql/time_travel.sql` (`FOR SYSTEM_TIME / SYSTEM_VERSION AS OF`, which Impala and Spark both accept), snapshot ids recorded per batch |
| 6 | SCD2 with versions, timestamps, link to original records | `gold.dim_party`, `gold.dim_account`: `version`, `effective_from/to`, `is_current`, `record_hash`, `src_batch_id`, `src_record_id` |
| 7 | Metadata and lineage across ingest, transform, consume | Atlas (Spark on CDE, Impala views), `ref.transform_log` (cleanse / standardise / deduplicate / enrich / normalise / aggregate / consume), Atlas glossary terms on the KPI views and their consumers, Atlas PII classifications + Ranger tag masking (`scripts/governance.py`, `docs/GOVERNANCE.md`) |
| 8 | Reconciliation, failed-batch re-run, automated mismatch report | Batch manifests with control counts, `ref.load_audit`, `ref.recon_results`, `cde/jobs/reconcile.py`, `--fail-during` / `--fail-after` / `--resume` flags, `docs/FAILED_BATCH_DEMO.md` |
| 10 | Banking data model, 3+ domains | Customer, Deposits, Lending, Payments, AML as the extension; `docs/BANKING_MODEL.md` (ER, canonical keys, SCD2, conformed dims, source mappings, extension method) |
| 11 | Certified KPIs consumed three ways | `semantic.kpi_npa_exposure`, `kpi_casa_ratio`, `kpi_customer_relationship_value`, each read by an MIS dashboard (CDV), `sql/adhoc.sql` (Hue) and a regulatory / reporting dataset; a check proves the consumers agree with the certified view |

## Platform mapping

| Layer | Service | What runs there |
|---|---|---|
| Landing zone | Cloud storage (S3), written by a CDE job | `landing/<source>/<business_date>/...` files + `_manifest.json` per batch |
| Ingest + medallion + MDM | CDE Spark 3, Iceberg v2 | bronze (raw + validation), silver (typed, standardised, CDC applied), mdm, gold (model, SCD2) |
| Reconciliation + DQ | CDE Spark | per batch and layer, results in `ref.recon_results`; record-level rejects in `bronze.quarantine` |
| Orchestration | CDE Airflow | one DAG run per business date; a failed task leaves the batch `FAILED` in `ref.load_audit`; the semantic layer and regulatory datasets then run on Impala (`scripts/run_semantic.py --engine impala`) |
| Semantic layer, ad-hoc, time travel | CDW Impala (Hue) | certified KPI views, MIS views, regulatory datasets (`INSERT OVERWRITE` per reporting date, so Atlas records column lineage), time travel queries |
| Lineage, glossary, classification | SDX Atlas | lineage captured automatically from Spark and Impala; glossary `GDL Banking KPIs` with a term per KPI, assigned to the certified view and its consumers; PII classifications on every PII column of every layer, not propagated |
| Masking | SDX Ranger, tag service `cm_tag` | tag-based masking policies for this project's PII tags |
| End consumption | Cloudera Data Visualization, built as code | dashboards "GDL Banking KPIs MIS", "GDL Reconciliation & Data Quality", "GDL MDM & Golden Record", "GDL AML Alerts" (`docs/DATAVIZ.md`) |
| CI | GitHub Actions | pytest, the whole pipeline on local Spark + Iceberg, the failed-batch drill |

No CAI jobs, models or applications in this phase. Data Visualization runs on the
existing CDW instance (decision 6).

## Names

- Databases `rsingh_gdl_{bronze,silver,mdm,gold,semantic,ref}`; the prefix is on the
  database only (`rsingh_gdl_gold.dim_party`). `--db-prefix` on the CDE side.
- Job code in `cde/jobs/` (PySpark + standard library, shared helpers in `gdl_common.py`);
  configuration as JSON (`config/pipeline.json`, `contracts/`); env var prefix `GDL_`.
- CDE: repository `rsingh-gdl-pipeline`, python-env `rsingh-gdl-python-env` (only if a job
  needs a package beyond PySpark), jobs `rsingh-gdl-{land,bronze,silver,mdm,gold,recon}`, DAG job `rsingh-gdl-orchestration` (dag_id `general_datalakehouse`).
- Data Visualization: the instance's existing connection `federal-impala-1`; datasets are views in
  `rsingh_gdl_semantic`; dashboards, datasets and visuals named `GDL ...` with fixed primary keys 9000+.
- Landing: `s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing/<source>/<business_date>/`

- Atlas classifications `GDL_PII_LAST_4`, `GDL_PII_HASH`, `GDL_PII_REDACT`, `GDL_PII_YEAR`
  (own names, so the existing `PII_*` tags and policies of the other demos
  are not touched); glossary `GDL Banking KPIs`; Ranger policies `rsingh-gdl-pii-*` in `cm_tag`.

## Source systems (synthetic, deterministic, seeded)

| System | Type | Format | Batches |
|---|---|---|---|
| Core banking (CBS) on MySQL | structured | D1: `mysqldump`-style `.sql` (CREATE TABLE + multi-row INSERT) for `customer`, `account`, `branch`, `product`. D2..D5: Debezium-style CDC JSON lines (`op` c/u/d, `before`, `after`, `ts_ms`, `source.file/pos`) | full dump, then CDC |
| Loan management (LMS) | structured | CSV per entity (`loan_account`, `repayment`) with a header row and a trailer `T|<record_count>|<sum_amount>` | daily full extract |
| Payments hub | semi-structured | JSON lines, nested (debtor / creditor / amount{value,ccy} / remittance), an extra field appears from D4 (schema drift) | daily events |
| CRM / digital onboarding | semi-structured | JSON documents, variable attributes, arrays of addresses and contacts | daily changes |
| Correspondence + KYC | unstructured | `.eml` emails (address change, complaint), KYC declaration `.txt`, small `.png` scans | daily |
| Compliance screening (AML extension) | structured | pipe-delimited sanctions / PEP list with header and trailer | daily full extract |

Each batch folder has `_manifest.json`: file name, size, sha256, record count and
control totals per entity, as the source system reports them. Reconciliation is
against this manifest, never against what Spark happened to read.

Scale: ~1,000 real people, ~1,400 customer records across CBS, LMS and CRM (the same
person in several systems, with typos, initials, formatted phones, old addresses),
~1,600 accounts, ~400 loans, ~3,000 payments a day, ~50 documents a day.
`--customers` scales it.

Injected faults (so validation, quarantine and MDM have something to find): missing
mandatory fields, bad PAN / IFSC / dates, duplicate re-sends, out-of-order CDC, a
delete of a record never inserted, a CSV whose trailer count is off by one.

## Layers

- **Bronze** (as received, one partition per business date): one table per source entity, all
  columns as strings plus `_batch_id`, `_business_date`, `_source_system`, `_source_file`,
  `_source_row`, `_ingested_at`, `_warnings`, `_record_hash`. Unstructured files go into
  `bronze.doc_document` (file, mime, size, sha256, text content where there is text, bytes).
  Each record is checked against its contract (`contracts/<entity>.json`: columns, types,
  mandatory, regex, allowed values, keys). Good
  rows go to the entity table and rejects go to `bronze.quarantine` with a reason code. A new
  JSON field is kept (Iceberg schema evolution) and recorded as schema drift.
- **Silver** (typed, cleansed, standardised, one current state per source): types cast;
  names, phones (E.164), PAN, addresses and dates normalised; CDC applied in `ts_ms`
  order (MERGE: insert / update / soft delete); duplicate re-sends removed; entities
  extracted from documents (PAN, mobile, account number, intent) and linked to a party.
- **MDM** (`rsingh_gdl_mdm`): `party_candidate` (every source customer record, standardised),
  `match_pair` (rule, score, decision), `party_xref` (source system + source id -> `party_id`,
  with match rule and confidence), `golden_party` (survived attributes with the source of each).
- **Gold** (banking model, below): SCD2 dimensions, conformed dimensions, daily facts.
- **Semantic** (Impala views): the certified KPI and its three consumers.
- **Ref**: `load_audit`, `recon_results`, `dq_results`, `transform_log`, `source_mapping`,
  `kpi_definition`.

## MDM

1. **Standardise**: upper-case, strip titles (MR/MRS/DR), collapse spaces, transliterate
   common variants (MOHD -> MOHAMMED); phone to E.164; PAN upper-case and pattern-checked;
   address tokens (RD -> ROAD, pin code extracted); DOB to ISO.
2. **Match**: blocking on PAN, mobile, and (soundex of last name + DOB year). Rules in
   order: exact PAN (auto-merge); mobile + DOB (auto-merge); name Jaro-Winkler / Levenshtein
   similarity >= 0.92 + DOB + pin code (auto-merge); 0.85 to 0.92 (review: kept apart, shown in
   the "MDM & Golden Record" dashboard's review queue). Every decision is a row in `match_pair`.
3. **Cluster**: connected components over auto-merge pairs (iterative min-label propagation in
   Spark, no extra package), giving a stable `party_id` (a hash of the smallest member key, so
   a re-run gives the same id).
4. **Survivorship**, per attribute: PAN, DOB from the KYC'd source (CBS) first; mobile and email
   most recent verified; address most recent, with a correspondence change request beating
   older addresses; name from the highest-trust source, longest non-initial form as the tie-break.
   `golden_party` records which source and record each value came from.
5. **One version of truth**: `party_xref` is the only way to a `party_id`; silver accounts,
   loans, payments and documents and all gold facts carry it. A test checks that no layer has a
   customer reference without one.

## Banking data model (gold)

Four domains: **Customer**, **Deposits**, **Lending**, **Payments**; **AML** added as the extension
(`dim_aml_rule`, `fact_aml_alert`). Details in `docs/BANKING_MODEL.md`.

| Kind | Tables |
|---|---|
| Conformed dimensions | `dim_date`, `dim_branch`, `dim_product`, `dim_currency`, `dim_party` (SCD2) |
| Customer | `dim_party` (SCD2, from `golden_party`), `bridge_party_account` (role: primary / joint / guarantor) |
| Deposits | `dim_account` (SCD2: status, branch, product), `fact_deposit_balance_daily` |
| Lending | `dim_loan` (SCD2: restructure, rate), `fact_loan_position_daily` (outstanding, overdue, DPD, IRAC asset class) |
| Payments | `fact_payment` (channel, direction, counterparty bank from IFSC, amount in INR) |

- **Canonical keys**: `party_id` (MDM), `account_key` = `ACC:<source>:<source account no>`,
  `loan_key`, `branch_code` (IFSC-derived), `product_code`, `date_key` (yyyymmdd). Surrogate keys
  for SCD2 versions: `party_sk`, `account_sk`.
- **SCD2**: `version`, `effective_from`, `effective_to` (9999-12-31 when open), `is_current`,
  `record_hash` (of the tracked attributes), `change_reason`, and the link to the original
  record (`src_system`, `src_record_id`, `src_batch_id`, plus the bronze `_record_hash`). Facts join
  the version current on their business date.
- **Source mappings**: `model/source_mapping.csv`, one row per target column: source system, entity,
  field, transformation (cleanse / standardise / enrich / normalise / aggregate). Loaded into
  `ref.source_mapping`, rendered in `docs/BANKING_MODEL.md`, and checked by a test against the
  real gold columns.
- **Extension methodology**: a new domain adds a contract, a bronze entity, a silver
  standardiser, mappings in the CSV, and gold tables that join only through conformed dimensions
  and canonical keys. Documented step by step, and shown once by adding a small AML domain
  (`fact_aml_alert` from payment rules and a screening list) as the worked example.

## Certified KPIs and their consumers

Each KPI is defined once, as an Impala view in `rsingh_gdl_semantic` at the finest grain
(account or party x reporting date), with its row in `ref.kpi_definition` (owner, formula,
parameters, version, certified on) and an Atlas glossary term attached to its columns. Every
consumer only selects from (aggregates) that view; none re-derives the formula.

| KPI | Certified definition | Domains |
|---|---|---|
| **NPA exposure** | RBI IRAC: a loan is NPA when 90+ days past due on the reporting date; asset class Standard / Sub-standard (NPA up to 12 months) / Doubtful 1-3 / Loss; gross NPA = outstanding of NPA loans; gross NPA ratio = gross NPA / gross advances; provision by class at demo rates | Lending, Customer |
| **CASA ratio** | (current + savings balances) / total deposits (CASA + term), end-of-day balances on the reporting date | Deposits, Customer |
| **Customer relationship value** | per party, annualised: CASA balance x CASA spread + term deposit balance x TD spread + loan outstanding x lending margin + fee income of the last 30 days x 12; rates in `ref.kpi_parameter` | Customer, Deposits, Lending, Payments |

| Consumer | NPA exposure | CASA ratio | Customer relationship value |
|---|---|---|---|
| MIS report (CDV dashboard "Banking KPIs MIS") | by branch, product, asset class, trend | by branch, segment, trend | by segment, branch, value band; top relationships |
| Ad-hoc query (`sql/adhoc.sql`, Hue) | NPA borrowers with their golden record and other relationships | branches whose CASA ratio fell between two batches (time travel) | a party's value broken down by product |
| Regulatory / external dataset (Impala table per reporting date) | `reg_asset_classification`: borrower, facility, asset class, outstanding, provision | `reg_deposit_composition`: deposits by type, branch and size band (every synthetic account is INR, so there is no residency split) | `rpt_customer_profitability`: management reporting extract (no regulator uses this KPI; it is the third consumer) |

Customer relationship value is certified at two grains from one definition:
`kpi_crv_component` (party x component x product, so the ad-hoc product breakdown aggregates it
rather than re-deriving spreads) and `kpi_customer_relationship_value` (its sum per party).

All semantic SQL (`sql/semantic/*.sql`, `sql/adhoc.sql`, `sql/time_travel.sql`) is written once
in the dialect Impala and Spark share; `scripts/run_semantic.py --engine impala|spark` runs it
(Spark locally and in CI, on an Iceberg JDBC catalog because the Hadoop catalog has no views).

**Consistency check**: `run_semantic.py --steps check` asserts that the MIS, ad-hoc and regulatory
totals for a reporting date equal the certified view's, for all three KPIs (23 checks, including
certified provision vs the rate gold applied), and writes the result to `ref.recon_results` with
layer `semantic` (shown on the Reconciliation dashboard). Plain SQL, so it runs on either engine.

## Reconciliation and the failed-batch demo

- `ref.load_audit`: one row per batch x stage (`STARTED`, `COMMITTED`, `FAILED`), with the
  Iceberg snapshot id each table had before and after the stage.
- `reconcile.py` after each layer: manifest count vs landed, bronze accepted + quarantined, silver,
  and control totals (amount sums); `MATCHED` / `MISMATCH` per entity in `ref.recon_results`,
  plus a mismatch report (Impala view, CSV and a Data Visualization sheet).
- **Drill**: `ingest_bronze.py --fail-during lms_loan` kills a Spark task in the middle of the
  `lms_loan` write (`--fail-after <entity>` fails just after that entity's commit instead). The
  demo shows:
  1. `load_audit` has the batch `FAILED` with the error. The entities committed before it are
     there, `lms_loan` has no rows for the date (its commit never happened, so nothing half-written
     is visible), and the later entities are absent.
  2. The automatic mismatch report: which entities are short and by how many rows.
  3. The Iceberg history of the affected tables, and the rows by `_batch_id`.
  4. Re-run: every write replaces the business date's partition in one commit
     (`overwritePartitions`), so re-running the whole batch duplicates nothing; `--resume` instead
     skips the entities committed since the last complete run. The recon report goes to `MATCHED`.
  5. The alternative: roll a table back to the pre-batch snapshot from `load_audit`
     (`CALL system.rollback_to_snapshot`).

## Time travel

Each batch records snapshot ids per table. `sql/time_travel.sql` shows a customer's golden record
before and after a scanned address-change request (`FOR SYSTEM_VERSION AS OF` with the snapshot
ids from `load_audit`, and `FOR SYSTEM_TIME AS OF`), a diff between two snapshots, the parties a
batch added, a regulatory dataset as it was submitted, and `DESCRIBE HISTORY`. Time travel (what
a table held at a moment) is set against SCD2 (business history kept as rows). The bronze table
before the failed batch is in the drill (`docs/FAILED_BATCH_DEMO.md`).

## Metadata and lineage

- **Atlas** records lineage on its own: Spark on CDE at table level, and Impala views / CTAS at
  column level, which is why the semantic layer is built in Impala.
- **`ref.transform_log`**: one row per job step, with source tables, target table, transform type
  (ingest, validate, cleanse, standardise, deduplicate, match, survive, enrich, normalise,
  aggregate, consume), rows in and out, batch, and snapshot id, so the operational metadata Atlas
  does not hold (counts, rules) can be queried next to it.
- **Governance as code** (`scripts/governance.py` from `config/governance.json`, standard
  library, over the data lake's Knox gateway
  `https://federal-aw-dl-gateway.federal.dp5i-5vkq.cloudera.site/federal-aw-dl/cdp-proxy-api/`
  with the workload user; `plan` / `apply` / `verify`; details in `docs/GOVERNANCE.md`):
  - Classifications `GDL_PII_*` on every PII column of bronze, silver, MDM, gold and semantic,
    by column name (date of birth by type), each attached with propagation off, so a derived
    column never inherits a mask (the churn demo's `date_of_birth` -> `age` lesson).
  - Glossary `GDL Banking KPIs`, one term per KPI (definition, formula, owner, version), assigned
    to the certified view and to every MIS view and regulatory dataset that reads it.
  - Tag-based masking policies in `cm_tag`, one per tag (last 4, hash, redact, year only via a
    custom `TRUNC({col}, 'YYYY')`): masked for federal01 and federal07, clear for `rsingh`.

## Phases

- [x] **0. Scaffold**: layout, config, contracts, requirement files, local Spark + Iceberg runner,
  commit hook, CI skeleton.
- [x] **1. Sources + landing**: generator for the six systems, 5 business days, manifests,
  injected faults; tests.
- [x] **2. Bronze**: ingest with contract validation, quarantine, schema drift, `load_audit`,
  `--fail-during` / `--fail-after` / `--resume`; recon at bronze.
- [x] **3. Silver**: typing, standardisation, CDC MERGE, document extraction; `transform_log`.
- [x] **4. MDM**: candidates, matching, clustering, survivorship, `party_xref`, `golden_party`.
- [x] **5. Gold**: banking model, SCD2 dims, facts, conformed dims, source mappings check.
- [x] **6. Semantic + KPIs**: three certified views, MIS / ad-hoc / regulatory consumers,
  consistency check, time-travel SQL.
- [x] **7. Cloudera live**: CDE jobs + DAG, CDW views, five batches end to end, the failed-batch
  drill on the cluster, Atlas lineage checked.
- [ ] **8. Governance**: Atlas classifications, glossary and terms; Ranger tag masking policies;
  verified as a masked and a clear user.
- [x] **9. Data Visualization**: Banking KPIs MIS, Reconciliation & Data Quality, MDM & Golden
  Record and AML Alerts dashboards as code.
- [x] **10. Docs + extension**: README, `BANKING_MODEL.md`, `FAILED_BATCH_DEMO.md`,
  `DEMO_RUNBOOK.md`; AML added as the worked extension example.

## How we work

1. Read the sibling repo's file before writing its counterpart; copy, then adapt.
2. One commit per phase. No attribution trailers of any kind in commits or docs; the local
   `commit-msg` hook strips them.
3. `pytest -q` green before moving on; verify each Cloudera resource live, one at a time, and
   record what ran in `docs/PROJECT_LOG.md`.
4. Experiments in scratch folders, never the default local warehouse.
5. Ask Ravi before `git push`, the first CDW write, and the first DAG trigger or unpause. Nothing
   secret goes into the public repo.

## Decisions

1. KPIs: NPA exposure, CASA ratio and customer relationship value, all three certified
   (2026-10-01).
2. Domains: Customer, Deposits, Lending, Payments; AML is the extension example.
3. No CAI jobs or applications for now; Data Visualization is the end consumer. The regulatory
   datasets are built in Impala by the DAG's last task instead of a CAI job.
4. Landing under `s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing/`.
5. Atlas glossary terms on the KPIs, PII classifications, and Ranger tag masking policies, all
   scripted.

6. Data Visualization host: the existing CDW Data Visualization instance
   `https://viz-indianbank-spend-analytics.dw-federal-cdp-env.dp5i-5vkq.cloudera.site/arc/apps/`;
   this project adds its own connection and dashboards there and touches nothing else.
7. Masked demo users for the Ranger policies: `federal01` and `federal07`; `rsingh` sees clear values.
8. Impala after the DAG: a laptop step, `scripts/run_semantic.py --engine impala`, after each
   DAG run (the same script runs the semantic layer on Spark locally and in CI).
9. AML as the extension: a compliance screening-list source plus payment rules, raising
   `fact_aml_alert`; alerts checked against the generator truth by reconciliation (2026-10-02).

## Open

None.
