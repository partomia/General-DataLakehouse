# Governance: classifications, glossary, masking, lineage

Governance is code: `config/governance.json` (what), `scripts/governance.py` (how), applied to
the data lake's Atlas and Ranger through its Knox gateway (`datalake_api` in
`config/pipeline.json`) as the workload user. Only this project's objects are created or
changed: `GDL_PII_*` classifications, the `GDL Banking KPIs` glossary and Ranger policies
named `rsingh-gdl-pii-*`. The `PII_*` tags and `cm_tag` policies of the other demos are left
alone (a test checks the config cannot name them).

```bash
export GDL_WORKLOAD_USER=... GDL_WORKLOAD_PASSWORD=...   # never committed
python scripts/governance.py                  # plan: what apply would change, read-only
python scripts/governance.py apply            # or --only typedefs|classifications|glossary|masking
python scripts/governance.py verify           # exit 1 unless everything is in place
```

Run `apply` after the first full run (tables and views exist in Atlas) and again whenever a
table or view is recreated: Atlas gives a recreated object a new entity, without the old one's
classifications and glossary assignments. `plan` lists exactly which.

## PII classifications

Every column of `rsingh_gdl_{bronze,silver,mdm,gold,semantic}` whose name is in the
`columns` map gets its classification, on Iceberg tables (`iceberg_column`) and views
(`hive_column`) alike. The map covers the source contracts' personal fields and every name
silver and gold derive from them (`pan_std`, `name_key`, `mobile_e164`, ...); a test fails if a
contract adds a personal-looking column without a classification.

| Classification | Columns | Ranger mask | federal01 / federal07 see |
|---|---|---|---|
| `GDL_PII_LAST_4` | PAN, Aadhaar, mobile, account number | `MASK_SHOW_LAST_4` | only the last 4 characters in clear |
| `GDL_PII_HASH` | names, e-mail | `MASK_HASH` | a hash: joins, counts and distinct counts still work |
| `GDL_PII_REDACT` | addresses, a date of birth held as text; whole records and texts (`_record`, `payload`, `text_content`, MDM's `golden_attribute.value`) | `MASK` | letters `x`, digits `n` |
| `GDL_PII_YEAR` | date of birth as a DATE | `CUSTOM`: `TRUNC({col}, 'YYYY')` | 1 January of the year |
| `GDL_PII_NULL` | raw document bytes (`doc_document.content`) | `MASK_NULL` | NULL |

The whole-record columns hold every personal field of a record at once: the rejected record in
`bronze.quarantine._record`, the source JSON in bronze `payload`, the e-mail and KYC text in
`doc_document`, and each surviving golden value in `golden_attribute.value`.

`dob` and `date_of_birth` are classified by type: YEAR on a DATE column (silver onwards), REDACT
on bronze, where every column is still a string.

**Not propagated.** Each classification is attached with propagation off. Atlas propagation
follows lineage to every downstream column, including derived ones that are not personal (the
lesson from the churn demo, where `date_of_birth` masked the `age` derived from it). Instead every
personal column in every layer is classified by name, which is explicit and reviewable.

## Masking

One tag-based masking policy per classification in the `cm_tag` service (linked to `cm_hive`, so
it applies in Impala, Hive and Spark through HiveServer2), masking for `federal01` and
`federal07`. Everyone else, including `rsingh`, sees clear values. Both users already have
SELECT through the existing `cm_hive` resource policy, so nothing else is granted.

To check, in Hue on the CDW Impala warehouse, run the same query once as `federal01` and once as
`rsingh`:

```sql
SELECT party_id, full_name, dob, pan, mobile, email, address
FROM rsingh_gdl_gold.dim_party
WHERE is_current AND party_id <> 'UNKNOWN'
ORDER BY party_id
LIMIT 5;

SELECT acct_no, full_name, pan, dob FROM rsingh_gdl_silver.cbs_customer c
JOIN rsingh_gdl_silver.cbs_account a USING (cust_id) LIMIT 5;
```

## Auto-classification: Data Catalog profiler tag rules

The Data Compliance profiler of Cloudera Data Catalog can apply the same `GDL_PII_*`
classifications by itself, on its schedule, from the data: a new source or column is classified
without anyone editing the name map or running `governance.py`. The masking policies are tag
based, so a column the profiler tags is masked for `federal01` and `federal07` straight away.

The rules are code, in `config/profiler_tag_rules.json`; `scripts/profiler_rules.py` renders them
and checks them against the data:

```bash
python scripts/profiler_rules.py render     # governance/profiler/*.csv and test_data.csv
python scripts/profiler_rules.py evaluate   # score every table column on Impala (GDL_IMPALA_*)
```

| Tag rule | Tag | Value regex (weight) | Column name |
|---|---|---|---|
| GDL PAN | `GDL_PII_LAST_4` | `AAAAA9999A` (85%) | `pan` |
| GDL Aadhaar | `GDL_PII_LAST_4` | 12 digits (50%) | `aadhaar` |
| GDL Mobile | `GDL_PII_LAST_4` | Indian mobile, any of the source formats (85%) | `mobile`, `phone` |
| GDL Account number | `GDL_PII_LAST_4` | 14 digits (30%) | `acct_no`, `account_no` |
| GDL E-mail | `GDL_PII_HASH` | e-mail address (85%) | `email` |
| GDL Person name | `GDL_PII_HASH` | letters (20%) | `first_name` ... `customer_name` |
| GDL Postal address | `GDL_PII_REDACT` | digits and words (20%) | `address`, `addr_line1`, ... |

A column's score for a rule is the value weight times the share of its values that match, plus
the rest of 100 if its name matches; the tag is applied at 70 or more, the profiler's own
threshold. With a high weight the values decide: a PAN, mobile or e-mail is found whatever the
column is called. With a low weight the name decides, for three reasons:

- Names and addresses are free text, shaped like product, branch and rule names.
- Account numbers also fill join keys (`account_key`, `debtor_account`); masking a join key
  would break joins for the masked users.
- Counterparty account numbers in payments are 12 digits, like an Aadhaar number.

Two things stay with `governance.py`: the date-of-birth columns, because the mask depends on
the column type (YEAR on a DATE, REDACT on a bronze string), which a tag rule cannot see; and
the semantic views, because the profilers profile tables.

**Checked against the data.** `evaluate` on the five loaded dates (844 columns in bronze,
silver, MDM and gold) agreed with the name map on every PII column apart from the 13
date-of-birth columns and an empty one (`lms_borrower.email_std`; loan borrowers have no
e-mail). It also found a gap in the map: `addr_line1` and `addr_line2` in `cbs_customer` (bronze
and silver) and silver `crm_customer` were not classified, so `federal01` saw street addresses.
They are in the map now, and tagged. No other column was tagged by mistake. A test runs the
same comparison on the generator's landing files.

**Setting it up** (Data Catalog, on the environment's data lake; compute-cluster profilers):

1. A Power User launches the profilers once (UI: Profilers > Setup Profiler, or
   `cdp datacatalog launch-profilers --datalake <crn>`). The Kubernetes node group takes 15 to 30
   minutes.
2. Data Compliance profiler > Configuration: an allow-list rule, Database name starts with
   `rsingh_gdl_`. Incremental profiling on, which reads only new Iceberg data.
3. Tag Rules > Create Tag Rule, once per rule (step by step, with screenshots:
   [PROFILER_TAG_RULES.md](PROFILER_TAG_RULES.md)). Pick the tag and upload
   `governance/profiler/<rule>.csv` as the regular-expression file, or type the expressions
   from the table above. Set the column value weightage, then use `test_data.csv` in Test Tag
   Rule.
4. Dry Run on up to 10 tables, then Enable.
5. Review the suggested tags under Job History > Profiled Assets and approve them.
6. Set `"profiler_tags_on_tables": true` in `config/governance.json`. `governance.py apply` then
   leaves the profiler's tags on table columns, and keeps tagging the date-of-birth columns and
   the views.

## Glossary

Glossary `GDL Banking KPIs`, one term per certified KPI from `config/kpi.json` (definition,
formula, grain, certified view, owner, version, certified on). Each term is assigned to its
certified view and to every view and dataset that consumes it:

| Term | Assigned to (`rsingh_gdl_semantic`) |
|---|---|
| NPA exposure | `kpi_npa_exposure`, `mis_npa_trend`, `mis_npa_breakdown`, `reg_asset_classification` |
| CASA ratio | `kpi_casa_ratio`, `mis_casa_trend`, `mis_casa_breakdown`, `reg_deposit_composition` |
| Customer relationship value | `kpi_crv_component`, `kpi_customer_relationship_value`, `mis_crv_segment`, `mis_crv_top_relationships`, `rpt_customer_profitability` |

A test checks every assigned object reads (or is) the certified view, so a term cannot be put on
a dataset that computes the KPI some other way.

## Lineage and operational metadata

- **Atlas** records lineage by itself: CDE Spark jobs as `spark_process` entities between the
  Iceberg tables they read and write, and Impala views and `INSERT OVERWRITE` statements as
  `impala_process` entities, with column lineage. That is why the semantic layer and the
  regulatory datasets are built in Impala.
- **`rsingh_gdl_ref.transform_log`**: one row per job step, with its sources, target, transform
  type (ingest, validate, cleanse, standardise, deduplicate, match, survive, historise, enrich,
  normalise, aggregate, consume), rows in and out, and the target's snapshot id; the counts and
  rules Atlas does not hold.
- **`rsingh_gdl_ref.load_audit`**: every stage and entity per batch, with the Iceberg snapshot ids
  before and after (used by time travel and the failed-batch drill).
- **`rsingh_gdl_ref.source_mapping`**: source system, entity, field and transform of every gold
  column ([BANKING_MODEL.md](BANKING_MODEL.md#source-mappings)).
