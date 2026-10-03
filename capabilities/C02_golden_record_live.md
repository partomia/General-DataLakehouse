# C02. Golden record from customer records that share no identifier

Covers the match, the survivorship decision, the audit entry, and the record written back to
a table another engine can read.

## Summary

A CDE Spark job, `rsingh-gdl-mdm-live` (`cde/jobs/mdm_live.py`), takes two customer records
and runs them through the pipeline's own MDM code: silver's standardisation, then
`build_mdm.py`'s matching, clustering and survivorship. It writes the results to Iceberg tables
in `rsingh_gdl_mdm` and an audit row per step to `rsingh_gdl_ref.transform_log`. Impala then
reads the golden record: Spark wrote it, a different engine reads it, through the shared
metastore.

The live tables are separate (`live_*`), so a live run does not touch the pipeline's 991 parties.

## The two records

`config/mdm_live_pair.json`: one core banking customer and one CRM profile of the same person.

| | Core banking `cbs:LIVE-100001` | CRM `crm:CRM-LIVE-0001` | Shared? |
|---|---|---|---|
| Name | `Dr. Meera Krishnan` | `Krishnan, Mera` (surname first, typo) | no |
| PAN | `AKCPK4821M` | none | no |
| Mobile | `98470 12345` | `+91-90200-55555` | no |
| E-mail | `meera.krishnan@example.com` | `mkrishnan88@mail.example.org` | no |
| Address | `14, M.G. Rd., Ernakulam` | `Flat 14, Mahatma Gandhi Road, Ernakulam` | written differently |
| Date of birth | 1988-07-14 | 1988-07-14 | yes |
| Pincode | 682016 | 682016 | yes |
| Last updated | 2026-03-02 | 2026-09-30 | |

No PAN, mobile, e-mail or cross-reference in common: the link has to come from the person's
attributes.

## Walkthrough (about 8 minutes)

**1. The records (1 min).** Open `config/mdm_live_pair.json` and the table above. To make it
truly live, change a value in front of the audience and pass the pair on the command line
(step 2).

**2. Run it (3 min, most of it Spark start-up).**

```bash
cde job run --name rsingh-gdl-mdm-live --arg=--business-date --arg=2026-09-25 \
  --arg=--db-prefix --arg=rsingh_gdl
```

With an edited pair: add `--arg=--records-json --arg='{"records": [...]}'`. The driver log
(`cde run logs --id <run> --type driver/stdout`) prints the match and each surviving value.
While it runs, open `cde/jobs/build_mdm.py`: the match rules are in its docstring.

**3. Standardisation (1 min).** In Hue (Impala):

```sql
SELECT candidate_id, name_std, name_key, pan_std, mobile_e164, email_std, pincode_std
FROM rsingh_gdl_mdm.live_candidate;
```

`Dr. Meera Krishnan` becomes `MEERA KRISHNAN` (title dropped), with the order-free key
`KRISHNAN MEERA`; `Krishnan, Mera` becomes `KRISHNAN MERA`. Mobiles are in E.164.

**4. The match (1 min).**

```sql
SELECT id_a, id_b, rule, decision, score, name_similarity,
       same_pan, same_mobile, same_dob, same_pincode, name_a, name_b
FROM rsingh_gdl_mdm.live_match_pair;
```

Result from the recorded run (CDE run 224):

| id_a | id_b | rule | decision | score | same PAN | same mobile | same DOB | same pincode |
|---|---|---|---|---|---|---|---|---|
| cbs:LIVE-100001 | crm:CRM-LIVE-0001 | NAME_DOB_PIN | MERGE | 0.9286 | false | false | true | true |

How it got there:

- **Blocking.** Only records with the same PAN, the same date of birth, or an LMS reference to
  a CBS customer are compared, so the job never compares every record with every other.
  These two meet in the date-of-birth block.
- **Rules, strongest first.** PAN_CONFLICT, PAN_EXACT, SOURCE_XREF and MOBILE_DOB need an
  identifier, and none applies. NAME_DOB_PIN does: same date of birth, same pincode, and name
  similarity of at least 0.92.
- **Name similarity.** 1 - Levenshtein / length of the longer name, on the name and on its
  order-free form, whichever is higher. `KRISHNAN MEERA` against `KRISHNAN MERA` is 1 edit in
  14 characters: 1 - 1/14 = 0.9286.

**5. The survivorship decision (1 min).**

```sql
SELECT attribute, src_system, src_key, value, distinct_values, rule
FROM rsingh_gdl_mdm.live_golden_attribute ORDER BY attribute;
```

| Attribute | Golden value | From | Rule |
|---|---|---|---|
| full_name | MEERA KRISHNAN | CBS | most trusted source (CBS, CRM, LMS), a full name over initials, then the longest |
| dob | 1988-07-14 | CBS | KYC-verified CBS first |
| pan | AKCPK4821M | CBS | KYC-verified CBS first (CRM has none) |
| mobile | +919020055555 | **CRM** | most recent valid value: CRM was updated in September |
| email | mkrishnan88@mail.example.org | **CRM** | most recent valid value |
| address | 14, M.G. Rd., Ernakulam, Kochi | CBS | the CBS address or the customer's own change request, before CRM |
| gender, segment, home_branch, kyc_status | F, AFFLUENT, BR104, VERIFIED | CBS | CBS is the system of record |

Each attribute has its own rule, so the golden record mixes sources: identity from the
KYC-verified core banking record, contact details from the more recent CRM record.
`distinct_values` shows where the sources disagreed (2 for name, mobile, e-mail and address).

**6. The audit entry (1 min).**

```sql
SELECT step, transform_type, target_table, rows_in, rows_out, logged_at, details
FROM rsingh_gdl_ref.transform_log
WHERE job = 'mdm_live' ORDER BY logged_at DESC LIMIT 4;
```

Four rows per run, under one `run_id`:

| step | type | rows in, out | details |
|---|---|---|---|
| candidates | standardise | 2, 2 | what was standardised |
| match | match | 2, 1 pair | `cbs:LIVE-100001 ~ crm:CRM-LIVE-0001: NAME_DOB_PIN MERGE score 0.9286 (name similarity 0.9286, same PAN False, same mobile False, same DOB True, same pincode True)` |
| cluster | deduplicate | 2, 1 party | connected components over MERGE pairs |
| survive | survive | 2, 1 golden record | every attribute, the record it came from, and the rule |

The pipeline's own MDM job writes the same steps to the same log for every batch, plus
`load_audit` rows with the Iceberg snapshot before and after.

**7. Another engine reads it (1 min).** Everything above was written by Spark on CDE and is
read by Impala on CDW. The golden record:

```sql
SELECT party_id, full_name, dob, pan, mobile, email, address, kyc_status,
       member_records, source_systems, attribute_sources
FROM rsingh_gdl_mdm.live_golden_party;
```

`PC05F2F1E8685`, 2 member records from `cbs` and `crm`, and `attribute_sources` naming the
record behind each value. The Iceberg history is visible from Impala too:

```sql
SELECT snapshot_id, committed_at, operation FROM rsingh_gdl_mdm.live_golden_party.snapshots;
```

Each run replaces the live tables, and Iceberg keeps every earlier run as a snapshot (time
travel with `FOR SYSTEM_VERSION AS OF <snapshot_id>`).

## Variants

Edit the pair and run again (`--records-json`):

- **Different pincode.** The rule falls to NAME_DOB (similarity at least 0.85, same date of
  birth): decision REVIEW, the records stay apart. A data steward decides; nothing is merged on
  a name and a birthday alone.
- **Give the CRM record a different PAN.** PAN_CONFLICT, NO_MATCH, whatever the names say.
- **Drop the middle name** (`Rahul Ramesh Iyer` against `Rahul Iyer`). NAME_SUBSET_DOB_PIN,
  REVIEW.

## The same thing in the pipeline's data

`rsingh_gdl_mdm.match_pair` for B20260925 has 5 NAME_DOB_PIN merges. These pairs have no PAN
or mobile in common, but they do share an e-mail, which the rules do not use. The live pair
shares nothing. Match quality against the generator's truth file is on the dashboard
"GDL MDM & Golden Record" (precision and recall per batch).

## Common questions

- **"Is the name threshold a guess?"** 0.92 merges, 0.85 to 0.92 goes to review; both are
  constants in `build_mdm.py` (`MERGE_AT`, `REVIEW_AT`), checked every batch against the truth
  file for precision and recall.
- **"Who is masked?"** The live tables carry personal data. `scripts/governance.py apply` tags
  their columns like the pipeline's (16 columns, plus `live_golden_attribute.value`), so
  `federal01` and `federal07` see them masked in Impala. Run it after the first live run;
  later runs replace the tables in place.
- **"Does the party id change when the records change?"** No: a cluster keeps the id its
  members already had in `party_xref`; a new cluster gets an id from a hash of its smallest
  member.
