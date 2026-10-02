# Demo questions

One file per question the demo has to answer: what to say, what to open and run, what the
audience should see, and the follow-ups to expect. The numbers and names in the answers are
from the run on CDE recorded in [docs/PROJECT_LOG.md](../docs/PROJECT_LOG.md).

| # | Question | Answer |
|---|---|---|
| 1 | Batch ingestion from an RDBMS or file source, with schema and record validation | [Q01_batch_ingestion_validation.md](Q01_batch_ingestion_validation.md) |
| 2 | A golden record, live, from two customer records that share no identifier: the match, the survivorship decision, the audit entry, and the record read by another engine | [Q02_golden_record_live.md](Q02_golden_record_live.md) |
| 3 | Structured, semi-structured and unstructured data ingested into one database in an open table format, and the flow | [Q03_structured_semi_unstructured.md](Q03_structured_semi_unstructured.md) |
| 4 | A time-travel query against a table as it stood in the past, and the table's snapshots | [Q04_time_travel.md](Q04_time_travel.md) |
| 5 | A type 2 dimension end to end, with a late-arriving update and a correction to a closed record | [Q05_scd2_late_and_corrections.md](Q05_scd2_late_and_corrections.md) |
| 6 | One dashboard figure traced to its source column across every hop; lineage for a streaming pipeline | [Q06_lineage_trace.md](Q06_lineage_trace.md) |
| 7 | A batch that fails halfway, the re-run, the rows already written, and the automated mismatch report | [Q07_failed_batch.md](Q07_failed_batch.md) |
| 8 | MDM matching, deduplication, survivorship and golden-record creation | [Q08_mdm.md](Q08_mdm.md) |
| 9 | The banking model: domains, entities, relationships, canonical keys, SCD2, common dimensions, source mappings, extension methodology | [Q09_banking_model.md](Q09_banking_model.md) |
