# C08. Master data management: matching, deduplication, survivorship and golden record

## Summary

The MDM stage (`cde/jobs/build_mdm.py`, CDE job `rsingh-gdl-mdm`) runs every batch, after
silver, over all customer records of the three systems that hold people: core banking (CBS),
loans (LMS) and CRM. For 2026-09-25:

| Step | What | Output table (`rsingh_gdl_mdm`) | Result |
|---|---|---|---|
| Candidates | every source customer record, standardised in silver (name, PAN, mobile in E.164, e-mail, address, pincode) | `party_candidate` | 1,812 records |
| Matching | pairs from three blocks (same PAN, same date of birth, LMS's own CBS reference), each given a rule, score and decision | `match_pair` | 1,072 pairs |
| Deduplication | connected components over the MERGE pairs; one `party_id` per cluster, stable across runs | `party_xref` | 1,009 parties |
| Survivorship | each attribute chosen by its own rule; the winning source recorded | `golden_attribute` | 10 attributes per party |
| Golden record | one row per party, with its sources and a record hash | `golden_party` | 1,009 golden records |
| Quality | pairwise precision and recall against the generator's truth file | `match_quality` | precision 1.000, recall 1.000 |

Every step writes an audit row to `rsingh_gdl_ref.transform_log`. The tables are Iceberg, so
Impala, Hue and the dashboard read the golden record directly. C02 shows the same code run live
on two new records ([C02_golden_record_live.md](C02_golden_record_live.md)).

## The match rules

In order; the first that applies wins. Name similarity is 1 − Levenshtein distance / length of
the longer name, on the name and on its order-free form, whichever is higher.

| Rule | Condition | Decision | Score | Pairs on 2026-09-25 |
|---|---|---|---|---|
| PAN_CONFLICT | both have a PAN and they differ | NO_MATCH (wins over all) | name similarity | 65 |
| PAN_EXACT | same PAN | MERGE | 1.00 | 706 |
| SOURCE_XREF | the LMS borrower names the CBS customer | MERGE | 0.99 | 21 |
| MOBILE_DOB | same mobile and date of birth | MERGE | 0.95 | 273 |
| NAME_DOB_PIN | name similarity ≥ 0.92, same date of birth and pincode | MERGE | similarity | 5 |
| NAME_SUBSET_DOB_PIN | one name's words all in the other's (middle name dropped), same date of birth and pincode | REVIEW (kept apart) | similarity | 1 |
| NAME_DOB | name similarity ≥ 0.85, same date of birth | REVIEW (kept apart) | similarity | 0 |
| PRIOR_LINK | the previous run had both in one party, and no PAN conflict now | MERGE | 0.90 | 1 |

## Walkthrough (about 10 minutes)

Open the dashboard "GDL MDM & Golden Record" for the overview (match quality, pairs by rule,
cluster sizes, golden parties), then Hue for one party end to end.

**1. One person, four records (Arun Agarwal, party `P97F981593876`).**

```sql
SELECT candidate_id, name_std, dob, pan_std, mobile_e164, email_std, pincode_std, record_ts, kyc_status
FROM rsingh_gdl_mdm.party_candidate
WHERE candidate_id IN ('cbs:100758', 'crm:CRM-98329341', 'lms:LB200257', 'lms:LB200258');
```

| Record | Name | PAN | Mobile | E-mail | Updated |
|---|---|---|---|---|---|
| `cbs:100758` | ARUN AGARWAL | DBPPA6587E | +918588552087 | arunagarwal@oldmail.example | 2024-10-19 (KYC VERIFIED) |
| `crm:CRM-98329341` | ARUN AGAWAL (typo) | none | +918588552087 | arun.agarwal14@example.com | 2026-01-25 |
| `lms:LB200257` | AGARWAL ARUN (surname first) | DBPPA6587E | +918588552087 | none | 2024-10-13 |
| `lms:LB200258` | ARRUN AGARWAL (typo) | DBPPA6587E | +918588552087 | none | 2025-08-29 |

Same date of birth (1997-11-02) and pincode (380032). Two LMS borrower records are the same
person: a duplicate inside one system, as well as across systems.

**2. The match.**

```sql
SELECT id_a, id_b, rule, decision, score, name_similarity, name_a, name_b
FROM rsingh_gdl_mdm.match_pair
WHERE party_a = 'P97F981593876' OR party_b = 'P97F981593876';
```

Six pairs, all MERGE: the three PAN holders by PAN_EXACT (1.00); the CRM profile, which has no
PAN, by MOBILE_DOB (0.95) with each of the others. Names are not needed here, but the
similarity is shown (0.85 to 1.0).

**3. Deduplication.**

```sql
SELECT src_system, src_key, party_id, match_rule, match_confidence, cluster_size
FROM rsingh_gdl_mdm.party_xref WHERE party_id = 'P97F981593876';
```

Four records, one party, cluster size 4, with the strongest rule per record. Across the batch:
393 parties with one record, 445 with two, 155 with three, 16 with four.

The `party_id` is stable: a cluster keeps the id its members already had, and a new one is a
hash of its smallest member. A cluster that splits keeps the id on one side and records
`previous_party_id` on the other.

**4. Survivorship.**

```sql
SELECT attribute, value, src_system, src_key, distinct_values, rule
FROM rsingh_gdl_mdm.golden_attribute WHERE party_id = 'P97F981593876' ORDER BY attribute;
```

| Attribute | Survived value | From | Distinct values | Rule |
|---|---|---|---|---|
| full_name | ARUN AGARWAL | CBS | 4 | most trusted source (CBS, CRM, LMS), a full name over initials, then the longest |
| dob, pan | 1997-11-02, DBPPA6587E | CBS | 1 | KYC-verified CBS, then a KYC declaration, then CBS, LMS, CRM |
| email | arun.agarwal14@example.com | CRM | 2 | most recent valid value, then the most trusted source |
| mobile | +918588552087 | CRM | 1 | most recent valid value |
| address | 192, Brigade Road, ..., Ahmedabad | CBS | 3 | the newest of the CBS address and the customer's own change request; else CRM, then LMS |
| gender, segment, home_branch, kyc_status | M, MASS, BR103, VERIFIED | CBS | 1 | CBS |

The point: the rule is per attribute. Identity (name, date of birth, PAN) comes from the
KYC-verified core banking record; contact details (mobile, e-mail) from the most recent source,
here CRM, so the old `oldmail.example` address loses. The rules are in the `SURVIVORSHIP` dict
in `build_mdm.py` and are written onto every attribute row.

**5. The golden record.**

```sql
SELECT party_id, full_name, dob, pan, mobile, email, address, kyc_status,
       source_systems, member_records, attribute_sources, record_hash
FROM rsingh_gdl_mdm.golden_party WHERE party_id = 'P97F981593876';
```

`source_systems` `["cbs","crm","lms"]`, `member_records` 4, `attribute_sources` maps each
attribute to its record. `record_hash` changes only when a survived value changes. That is what
`gold.dim_party` (SCD2) uses to open a new version (C05).

As federal01 the PAN, mobile, e-mail and address are masked (Ranger tag policies), so the
golden record is governed like the sources.

**6. The cases that must not merge.**

```sql
SELECT id_a, id_b, rule, decision, name_similarity, name_a, name_b, same_dob, same_pincode
FROM rsingh_gdl_mdm.match_pair WHERE decision <> 'MERGE' OR rule = 'PRIOR_LINK';
```

- **Namesakes:** `cbs:100499` and `cbs:100966`, both SUNIL KUMAR, same date of birth, name
  similarity 1.0, different PANs: PAN_CONFLICT, NO_MATCH. Two people.
- **Review:** RAHUL RAMESH IYER (`cbs:100329`) and RAHUL IYER (`crm:CRM-7811cae4`), middle name
  dropped, same date of birth and pincode: NAME_SUBSET_DOB_PIN, REVIEW. A name alone does not
  merge. They are one party here only because an earlier batch matched them on mobile and date
  of birth, and that link is kept (PRIOR_LINK) after the mobile changed.

**7. The audit and the quality.**

```sql
SELECT step, transform_type, rows_in, rows_out, details
FROM rsingh_gdl_ref.transform_log
WHERE job = 'build_mdm' AND batch_id = 'B20260925' ORDER BY logged_at;

SELECT business_date, source_records, parties, true_pairs, predicted_pairs, correct_pairs,
       precision, recall, split_persons, merged_persons
FROM rsingh_gdl_mdm.match_quality ORDER BY business_date;
```

The audit lists candidates (1,812), match (pairs by rule), cluster (1,812 to 1,009), document
links and survive (all the rules). Quality for the five business days: precision and recall
1.000, no person split, no party mixing two persons. The generator knows who is who, so the
match is measured, not asserted.

## Common questions

- **"The synthetic data is easy; what of real data?"** The data plants the hard cases: typos,
  surname first, initials, missing PANs, a namesake with the same date of birth, a dropped middle
  name, a changed mobile, LMS duplicates. With real data the truth file is replaced by the data
  steward's review decisions, and the same table reports precision.
- **"Who resolves REVIEW pairs?"** A data steward, from the Match pairs sheet. Today the
  decision is "keep apart"; a steward's confirmation would be loaded as a manual link (the
  same mechanism as PRIOR_LINK).
- **"Why not probabilistic matching (Fellegi-Sunter, Splink)?"** Deterministic rules are
  explainable to an auditor rule by rule. The rule and score columns are where a probabilistic
  score would go, without changing the clustering or survivorship.
- **"Documents?"** KYC declarations and address-change requests are linked to the party
  (`document_party`) and take part in survivorship: a customer's own change request beats an
  older CBS address (Shreya Naidu in C04).
