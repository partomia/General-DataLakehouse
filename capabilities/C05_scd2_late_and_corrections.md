# C05. SCD Type 2 dimensions: late-arriving updates and corrections to closed records

## Summary

Partly. SCD2 is maintained end to end on three dimensions, from source change to the fact rows
that point at the right version. Two of the asked cases are handled at the edges, not in the
dimension itself:

| Case | Status |
|---|---|
| New key, changed attribute, record removed at source, key reappears, same-day re-run | **Built.** NEW, CHANGED, REMOVED_AT_SOURCE, REAPPEARED; a re-run of the same date restates that version in place |
| Late-arriving dimension member (the fact arrives before its customer) | **Built, forward only.** The fact is kept on the `UNKNOWN` party and reconciliation explains it; the customer gets a version when it arrives and later facts point at it; facts already written stay on `UNKNOWN` |
| Late-arriving update (a change effective before the latest version) | **Not built.** SCD2 is built forward by business date; a change opens a version on the date it arrives, and gold refuses to rebuild an earlier date |
| Correction to a closed version | **Not built.** Closed versions are never rewritten; a correction arrives as a new version |

Say this plainly; it is a design choice for a daily batch, and the follow-up section is what to
offer.

## What is built, and how to show it

`gold.dim_party` (SCD2 over the MDM golden record), `gold.dim_account` and `gold.dim_loan`,
in `build_gold.py` `scd2()`:

| Column | Meaning |
|---|---|
| `version`, `effective_from`, `effective_to`, `is_current` | 9999-12-31 while open |
| `record_hash` | SHA-256 of the tracked attributes; a version opens only when it changes |
| `change_reason`, `changed_attributes` | NEW, CHANGED or REAPPEARED, and which attributes changed |
| `end_reason` | SUPERSEDED or REMOVED_AT_SOURCE |
| `src_record_ids`, `src_batch_id`, `created_batch_id` | the source records and batch behind the version |

Counts after the five dates: `dim_party` 1,136 rows (126 versions opened by a change, 126 closed), `dim_account`
1,504 (40 closed), `dim_loan` 362 (1 closed).

**1. End to end for one customer (3 min).** Shreya Naidu, `P0A4F2E33DF8E`:

```sql
SELECT version, effective_from, effective_to, is_current, change_reason, end_reason,
       changed_attributes, address, created_batch_id
FROM rsingh_gdl_gold.dim_party WHERE party_id = 'P0A4F2E33DF8E' ORDER BY version;
```

| v | from | to | reason | changed | address |
|---|---|---|---|---|---|
| 1 | 2026-09-21 | 2026-09-22 | NEW, SUPERSEDED | | 35, Station Road, Pune |
| 2 | 2026-09-23 | 2026-09-23 | CHANGED, SUPERSEDED | address, pincode | 194, Shivaji Nagar, Pune |
| 3 | 2026-09-24 | 9999-12-31 | CHANGED, current | address, pincode, address_city | 233, Park Street, Kochi |

Walk the hops: the e-mail in bronze `doc_document`, the extracted address in
`silver.doc_extract`, survivorship in `mdm.golden_attribute` (source `doc:EML-20260924-0001.eml`),
the version here. Then the facts:

```sql
SELECT f.business_date, f.party_sk, p.version, p.address
FROM rsingh_gdl_gold.fact_deposit_balance_daily f
JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = f.party_sk
WHERE f.party_id = 'P0A4F2E33DF8E' ORDER BY f.business_date;
```

Each date's balance points at the version valid on that date
(`effective_from <= date <= effective_to`).

**2. Late-arriving dimension member (2 min).** Customer `100807` was quarantined on D1 (no
last name) and corrected at source on D2 by a CDC update:

```sql
SELECT * FROM rsingh_gdl_silver.cdc_exception WHERE src_key = '100807';      -- UPSERT_UNKNOWN_KEY, INSERTED
SELECT business_date, COUNT(*) FROM rsingh_gdl_gold.fact_deposit_balance_daily
WHERE party_id = 'UNKNOWN' GROUP BY 1 ORDER BY 1;                             -- 7 on D1, 6 after
```

On D1 the balance of account `00090001001207` was kept on the `UNKNOWN` party (not dropped)
and reconciliation explained the row; from D2 the customer exists, gets party `P4B627928575E`
and a version, and the facts point at it. The D1 row stays on `UNKNOWN`: facts already written
are not re-pointed when the member arrives. The other 6 are accounts whose owner never loaded,
explained every day.

**3. Out-of-order source changes (1 min).** Silver applies CDC in source timestamp order, not
file order, before SCD2 sees anything: `cdc_exception` shows `OUT_OF_ORDER / APPLIED_BY_TS`
on D3, a re-sent event `DEDUPED` on D5, and a delete of an unknown key `IGNORED` on D4.

**4. The guard (30 s).** Re-running gold for an earlier date fails with "already has versions
from ...; SCD2 is built forward". That is the boundary of what is built.

## What a late-arriving update and a correction need

If the panel wants them, the change is in `scd2()` alone (facts already join by date range):

- **Late-arriving update** (effective date `e` before the open version): find the version
  whose range contains `e`, close it at `e - 1`, insert the new version from `e` to the start
  of the next version, and re-apply the change to every later version that did not itself
  change that attribute. Facts dated from `e` then pick up the right version; their
  `party_sk` values have to be re-pointed for those dates.
- **Correction to a closed version** (the value was wrong, not changed): rewrite that
  version's attributes in place, keep its dates, and record `corrected_batch_id` and
  `correction_reason`. Iceberg keeps the uncorrected table as a snapshot, so "what we
  reported before the correction" stays answerable by time travel (C04).

Both need an effective date from the source, which the current feeds do not carry for most
attributes (the business date is the effective date). The CRM feed's `address_updated_on` is
the one that does.
