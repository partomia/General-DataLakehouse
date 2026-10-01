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
| `GDL_PII_REDACT` | addresses, a date of birth held as text | `MASK` | letters `x`, digits `n` |
| `GDL_PII_YEAR` | date of birth as a DATE | `CUSTOM`: `TRUNC({col}, 'YYYY')` | 1 January of the year |

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
