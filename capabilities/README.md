# Capabilities

One file per capability of the lakehouse: what it does, how it is built, how to see it working
(what to open and run, and what to expect), and the questions it usually raises. The numbers
and names are from the run on CDE recorded in [docs/PROJECT_LOG.md](../docs/PROJECT_LOG.md).

| # | Capability | Scope | Document |
|---|---|---|---|
| 1 | Batch ingestion | From an RDBMS or file source, with schema and record validation | [C01_batch_ingestion_validation.md](C01_batch_ingestion_validation.md) |
| 2 | Live golden record | Two customer records that share no identifier: the match, the survivorship decision, the audit entry, and the record read by another engine | [C02_golden_record_live.md](C02_golden_record_live.md) |
| 3 | Multi-structured data | Structured, semi-structured and unstructured data in one database on an open table format, and the flow | [C03_structured_semi_unstructured.md](C03_structured_semi_unstructured.md) |
| 4 | Time travel | A table queried as it stood in the past, and the table's snapshots | [C04_time_travel.md](C04_time_travel.md) |
| 5 | SCD Type 2 | A type 2 dimension end to end, with late-arriving updates and corrections to closed records | [C05_scd2_late_and_corrections.md](C05_scd2_late_and_corrections.md) |
| 6 | Lineage | One dashboard figure traced to its source column across every hop; streaming lineage | [C06_lineage_trace.md](C06_lineage_trace.md) |
| 7 | Failed-batch recovery | A batch that fails halfway, the re-run, the rows already written, and the automated mismatch report | [C07_failed_batch.md](C07_failed_batch.md) |
| 8 | Master data management | Matching, deduplication, survivorship and golden-record creation | [C08_mdm.md](C08_mdm.md) |
| 9 | Banking data model | Domains, entities, relationships, canonical keys, SCD2, common dimensions, source mappings, extension methodology | [C09_banking_model.md](C09_banking_model.md) |
