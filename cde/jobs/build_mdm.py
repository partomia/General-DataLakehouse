"""
Stage 3 - MDM: one party per real customer across CBS, LMS and CRM, with a golden record.

From silver as it stood when the batch committed it (Iceberg time travel to the snapshot
in ref.load_audit), recomputed in full for each business date, so a re-run gives the same
answer:

  party_candidate   every source customer record, standardised in silver
                    (CBS customers, LMS borrowers, CRM profiles)
  match_pair        candidate pairs from three blocks (PAN, date of birth, the LMS
                    borrower's own CBS reference), each with a rule, score and decision:
                      PAN_CONFLICT   both have a PAN and they differ -> NO_MATCH (wins over all)
                      PAN_EXACT      same PAN                         -> MERGE 1.00
                      SOURCE_XREF    LMS names the CBS customer       -> MERGE 0.99
                      MOBILE_DOB     same mobile and date of birth    -> MERGE 0.95
                      NAME_DOB_PIN   name similarity >= 0.92, same date of birth and pincode -> MERGE
                      NAME_SUBSET_DOB_PIN  one name's tokens all in the other's (a middle name
                                     dropped), same date of birth and pincode -> REVIEW (kept apart)
                      NAME_DOB       name similarity >= 0.85, same date of birth -> REVIEW (kept apart)
                      PRIOR_LINK     the previous run had both in one party, and no PAN conflict now
                                     -> MERGE 0.90 (a changed mobile does not undo a match)
                    name similarity: 1 - Levenshtein / length, on the name and on its
                    order-free form, whichever is higher
  party_xref        source system + key -> party_id: connected components over MERGE
                    pairs (min-label propagation); a cluster keeps the party_id its members
                    already had (one cluster per id; a split-off cluster gets a new id and
                    records the old one), else a new id from a hash of its smallest member
  golden_party      one row per party, each attribute survived by its own rule
  golden_attribute  which source record each golden value came from, and the rule
  document_party    documents linked to a party (KYC customer id, a scan's declaration, or
                    the account number)
  match_quality     pairwise precision / recall against the generator's _truth file, when present

Usage:
  spark-submit build_mdm.py --business-date 2026-09-22 [--landing URI] [--db-prefix P]
"""

from __future__ import annotations

import json
import sys
from itertools import combinations
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gdl_common as C  # noqa: E402
from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "mdm"
MERGE_AT, REVIEW_AT = 0.92, 0.85
NAME_TRUST = {"cbs": 0, "crm": 1, "lms": 2}
QUALITY_SCHEMA = ("batch_id string, business_date date, source_records bigint, parties bigint, true_pairs bigint, "
                  "predicted_pairs bigint, correct_pairs bigint, precision double, recall double, "
                  "split_persons bigint, merged_persons bigint, measured_at timestamp")
SURVIVORSHIP = {
    "full_name": "most trusted source (CBS, CRM, LMS), a full name over initials, then the longest",
    "dob": "KYC-verified CBS, then a KYC declaration, then CBS, LMS, CRM",
    "pan": "KYC-verified CBS, then a KYC declaration, then CBS, LMS, CRM",
    "gender": "KYC-verified CBS, then CBS, then CRM",
    "mobile": "most recent valid value, then the most trusted source",
    "email": "most recent valid value, then the most trusted source",
    "address": "the newest of the CBS address and the customer's own change request; else CRM, then LMS",
    "segment": "CBS",
    "home_branch": "CBS",
    "kyc_status": "CBS, VERIFIED if any CBS record is verified",
}


def table(spark, names, layer, name):
    t = names.t(layer, name)
    return spark.table(t) if C.table_exists(spark, t) else None


# ---------------------------------------------------------------- candidates


def candidates(spark, names, d) -> DataFrame:
    common = ["name_std", "name_key", "dob", "pan_std", "mobile_e164", "email_std", "address_std", "pincode_std"]
    cbs = C.read_as_of_batch(spark, names, "silver", "cbs_customer", d).select(
        F.lit("cbs").alias("src_system"), F.col("cust_id").cast("string").alias("src_key"), *common,
        "gender", "city", F.concat_ws(", ", "addr_line1", "addr_line2", "city").alias("address_display"),
        (F.col("kyc_status") == "VERIFIED").alias("kyc_verified"), "kyc_status", "segment", "home_branch",
        F.lit(None).cast("string").alias("cbs_ref"), F.col("_src_ts").alias("record_ts"),
        F.col("_src_ts").alias("address_ts"), (~F.col("_is_deleted")).alias("is_active"))
    lms = C.read_as_of_batch(spark, names, "silver", "lms_borrower", d).select(
        F.lit("lms").alias("src_system"), F.col("borrower_id").alias("src_key"), *common,
        F.lit(None).cast("string").alias("gender"), "city", F.concat_ws(", ", "address", "city").alias("address_display"),
        F.lit(False).alias("kyc_verified"), F.lit(None).cast("string").alias("kyc_status"),
        F.lit(None).cast("string").alias("segment"), F.lit(None).cast("string").alias("home_branch"),
        F.col("cbs_cust_ref").cast("string").alias("cbs_ref"), F.col("_src_ts").alias("record_ts"),
        F.col("_src_ts").alias("address_ts"), (~F.col("_is_deleted")).alias("is_active"))
    crm = C.read_as_of_batch(spark, names, "silver", "crm_customer", d).select(
        F.lit("crm").alias("src_system"), F.col("crm_id").alias("src_key"), *common,
        "gender", "city", F.concat_ws(", ", "addr_line1", "addr_line2", "city").alias("address_display"),
        F.lit(False).alias("kyc_verified"), F.lit(None).cast("string").alias("kyc_status"),
        F.lit(None).cast("string").alias("segment"), F.lit(None).cast("string").alias("home_branch"),
        F.lit(None).cast("string").alias("cbs_ref"), F.col("_src_ts").alias("record_ts"),
        F.col("address_updated_on").cast("timestamp").alias("address_ts"), (~F.col("_is_deleted")).alias("is_active"))
    return (cbs.unionByName(lms).unionByName(crm)
            .withColumn("candidate_id", F.concat_ws(":", "src_system", "src_key")))


# ---------------------------------------------------------------- matching


def _similarity(a, b):
    return F.when(a.isNull() | b.isNull(), F.lit(0.0)).otherwise(
        1 - F.levenshtein(a, b) / F.greatest(F.length(a), F.length(b)))


def match_pairs(cand: DataFrame) -> DataFrame:
    ids = cand.select("candidate_id", "pan_std", "dob", "src_system", "src_key", "cbs_ref")

    def block(cond_a, cond_b, how):
        a = ids.where(cond_a).alias("a")
        b = ids.where(cond_b).alias("b")
        return (a.join(b, how(a, b) & (F.col("a.candidate_id") != F.col("b.candidate_id")))
                .select(F.least("a.candidate_id", "b.candidate_id").alias("id_a"),
                        F.greatest("a.candidate_id", "b.candidate_id").alias("id_b")))

    keys = (block(F.col("pan_std").isNotNull(), F.col("pan_std").isNotNull(),
                  lambda a, b: F.col("a.pan_std") == F.col("b.pan_std"))
            .unionByName(block(F.col("dob").isNotNull(), F.col("dob").isNotNull(),
                               lambda a, b: F.col("a.dob") == F.col("b.dob")))
            .unionByName(block(F.col("cbs_ref").isNotNull(), F.col("src_system") == "cbs",
                               lambda a, b: F.col("a.cbs_ref") == F.col("b.src_key")))
            .distinct())
    side = lambda p: cand.select(*[F.col(c).alias(f"{p}_{c}") for c in cand.columns])  # noqa: E731
    pairs = (keys.join(side("a"), F.col("id_a") == F.col("a_candidate_id"))
             .join(side("b"), F.col("id_b") == F.col("b_candidate_id")))
    both = lambda c: F.col(f"a_{c}").isNotNull() & F.col(f"b_{c}").isNotNull()  # noqa: E731
    eq = lambda c: both(c) & (F.col(f"a_{c}") == F.col(f"b_{c}"))  # noqa: E731
    sim = F.round(F.greatest(_similarity(F.col("a_name_key"), F.col("b_name_key")),
                             _similarity(F.col("a_name_std"), F.col("b_name_std"))), 4)
    xref = (((F.col("a_src_system") == "lms") & (F.col("b_src_system") == "cbs") & (F.col("a_cbs_ref") == F.col("b_src_key")))
            | ((F.col("b_src_system") == "lms") & (F.col("a_src_system") == "cbs") & (F.col("b_cbs_ref") == F.col("a_src_key"))))
    tokens_a, tokens_b = F.split("a_name_std", " "), F.split("b_name_std", " ")
    shorter = F.least(F.size(tokens_a), F.size(tokens_b))
    subset = (shorter >= 2) & (F.size(F.array_intersect(tokens_a, tokens_b)) == shorter)
    pairs = pairs.withColumn("name_similarity", sim)
    rule = (F.when(both("pan_std") & ~eq("pan_std"), "PAN_CONFLICT")
            .when(eq("pan_std"), "PAN_EXACT")
            .when(xref, "SOURCE_XREF")
            .when(eq("mobile_e164") & eq("dob"), "MOBILE_DOB")
            .when(eq("dob") & eq("pincode_std") & (sim >= MERGE_AT), "NAME_DOB_PIN")
            .when(eq("dob") & eq("pincode_std") & subset, "NAME_SUBSET_DOB_PIN")
            .when(eq("dob") & (sim >= REVIEW_AT), "NAME_DOB"))
    decision = (F.when(F.col("rule") == "PAN_CONFLICT", "NO_MATCH")
                .when(F.col("rule").isin("NAME_DOB", "NAME_SUBSET_DOB_PIN"), "REVIEW").otherwise("MERGE"))
    score = (F.when(F.col("rule") == "PAN_EXACT", 1.0).when(F.col("rule") == "SOURCE_XREF", 0.99)
             .when(F.col("rule") == "MOBILE_DOB", 0.95).otherwise(F.col("name_similarity")))
    return (pairs.withColumn("rule", rule).where(F.col("rule").isNotNull())
            .withColumn("decision", decision).withColumn("score", score)
            .select("id_a", "id_b", "rule", "decision", "score", "name_similarity",
                    eq("pan_std").alias("same_pan"), eq("mobile_e164").alias("same_mobile"),
                    eq("dob").alias("same_dob"), eq("pincode_std").alias("same_pincode"),
                    F.col("a_name_std").alias("name_a"), F.col("b_name_std").alias("name_b")))


def prior_links(spark, names, cand: DataFrame, pairs: DataFrame) -> DataFrame | None:
    """Records the previous run put in one party stay linked (PRIOR_LINK), unless they now have
    conflicting PANs: a changed mobile or address does not undo an established match."""
    prev = table(spark, names, "mdm", "party_xref")
    if prev is None:
        return None
    members = (prev.select(F.concat_ws(":", "src_system", "src_key").alias("candidate_id"), "party_id")
               .join(cand.select("candidate_id", "pan_std"), "candidate_id"))
    anchor = members.groupBy("party_id").agg(F.min("candidate_id").alias("anchor"))
    m = members.join(anchor, "party_id").where(F.col("candidate_id") != F.col("anchor"))
    a = cand.select(F.col("candidate_id").alias("anchor"), F.col("pan_std").alias("anchor_pan"))
    edges = (m.join(a, "anchor")
             .where(~(F.col("pan_std").isNotNull() & F.col("anchor_pan").isNotNull()
                      & (F.col("pan_std") != F.col("anchor_pan"))))
             .select(F.least("anchor", "candidate_id").alias("id_a"), F.greatest("anchor", "candidate_id").alias("id_b")))
    merged = pairs.where("decision = 'MERGE'").select("id_a", "id_b")
    return (edges.join(merged, ["id_a", "id_b"], "left_anti")
            .withColumn("rule", F.lit("PRIOR_LINK")).withColumn("decision", F.lit("MERGE"))
            .withColumn("score", F.lit(0.9)))


def components(spark, nodes: DataFrame, edges: DataFrame) -> DataFrame:
    """candidate_id -> cluster label (the smallest member id), by min-label propagation."""
    both = edges.select(F.col("id_a").alias("src"), F.col("id_b").alias("dst")).unionByName(
        edges.select(F.col("id_b").alias("src"), F.col("id_a").alias("dst"))).localCheckpoint()
    labels = nodes.select("candidate_id", F.col("candidate_id").alias("label")).localCheckpoint()
    for _ in range(50):
        spread = both.join(labels, both.dst == labels.candidate_id).select(F.col("src").alias("candidate_id"), "label")
        new = labels.unionByName(spread).groupBy("candidate_id").agg(F.min("label").alias("label")).localCheckpoint()
        changed = new.alias("n").join(labels.alias("o"), "candidate_id").where(F.col("n.label") != F.col("o.label")).count()
        labels = new
        if changed == 0:
            break
    return labels


def assign_parties(spark, names, cand: DataFrame, labels: DataFrame, pairs: DataFrame, bid: str) -> DataFrame:
    prev = table(spark, names, "mdm", "party_xref")
    x = cand.select("candidate_id", "src_system", "src_key", "is_active").join(labels, "candidate_id")
    if prev is not None:
        x = x.join(prev.select(F.concat_ws(":", "src_system", "src_key").alias("candidate_id"),
                               F.col("party_id").alias("prev_party_id"),
                               F.col("first_batch_id").alias("prev_first_batch")), "candidate_id", "left")
    else:
        x = x.withColumn("prev_party_id", F.lit(None).cast("string")).withColumn("prev_first_batch", F.lit(None).cast("string"))
    cluster = Window.partitionBy("label")
    new_id = F.concat(F.lit("P"), F.upper(F.substring(F.sha2(F.col("label"), 256), 1, 12)))
    x = x.withColumn("claimed", F.min("prev_party_id").over(cluster))
    keeper = x.where(F.col("claimed").isNotNull()).groupBy("claimed").agg(F.min("label").alias("keeper"))
    x = (x.join(keeper, "claimed", "left")
         .withColumn("party_id", F.when(F.col("label") == F.col("keeper"), F.col("claimed")).otherwise(new_id))
         .withColumn("cluster_size", F.count(F.lit(1)).over(cluster)))
    merges = pairs.where("decision = 'MERGE'")
    edge_rule = merges.select(F.col("id_a").alias("candidate_id"), "rule", "score").unionByName(
        merges.select(F.col("id_b").alias("candidate_id"), "rule", "score"))
    order = F.when(F.col("rule") == "PAN_EXACT", 1).when(F.col("rule") == "SOURCE_XREF", 2) \
        .when(F.col("rule") == "MOBILE_DOB", 3).when(F.col("rule") == "PRIOR_LINK", 5).otherwise(4)
    best = (edge_rule.withColumn("_o", order)
            .withColumn("_rn", F.row_number().over(Window.partitionBy("candidate_id").orderBy("_o", F.desc("score"))))
            .where("_rn = 1").select("candidate_id", F.col("rule").alias("match_rule"),
                                     F.col("score").alias("match_confidence")))
    x = x.join(best, "candidate_id", "left")
    return x.select(
        "src_system", "src_key", "party_id",
        F.coalesce("match_rule", F.lit("SINGLETON")).alias("match_rule"),
        F.coalesce("match_confidence", F.lit(1.0)).alias("match_confidence"),
        "cluster_size", "is_active",
        F.when(F.col("prev_party_id").isNotNull() & (F.col("prev_party_id") != F.col("party_id")),
               F.col("prev_party_id")).alias("previous_party_id"),
        F.coalesce("prev_first_batch", F.lit(bid)).alias("first_batch_id"),
        F.lit(bid).alias("batch_id"))


# ---------------------------------------------------------------- survivorship


def documents_to_party(spark, names, xref: DataFrame, d) -> DataFrame | None:
    docs = table(spark, names, "silver", "doc_extract")
    if docs is not None:
        docs = docs.where(F.col("business_date") <= F.lit(d.isoformat()).cast("date"))
    accounts = C.read_as_of_batch(spark, names, "silver", "cbs_account", d)
    if docs is None:
        return None
    cbs = xref.where("src_system = 'cbs'").select(F.col("src_key").alias("cust_key"), "party_id")
    acct = accounts.select("account_key", F.col("cust_id").cast("string").alias("acct_cust")) if accounts is not None \
        else None
    declared = docs.where(F.col("cust_id").isNotNull()).select(F.col("file_name").alias("kyc_ref"),
                                                               F.col("cust_id").alias("scan_cust"))
    d = (docs.join(declared, "kyc_ref", "left")
         .withColumn("kyc_cust", F.coalesce("cust_id", "scan_cust").cast("string")))
    if acct is not None:
        d = d.join(acct, "account_key", "left")
    else:
        d = d.withColumn("acct_cust", F.lit(None).cast("string"))
    d = (d.withColumn("cust_key", F.coalesce("kyc_cust", "acct_cust"))
         .withColumn("linked_via", F.when(F.col("cust_id").isNotNull(), "KYC_CUSTOMER_ID")
                     .when(F.col("scan_cust").isNotNull(), "KYC_DECLARATION_OF_SCAN")
                     .when(F.col("acct_cust").isNotNull(), "ACCOUNT_NUMBER")))
    return d.join(cbs, "cust_key", "left").select(
        "file_name", "business_date", "doc_type", "intent", "extraction_status", "linked_via", "cust_key", "party_id",
        "document_ts", "dob", "pan_std", "mobile_e164", "address_text", "pincode_std", "_src_batch_id")


def attribute_rows(cand: DataFrame, xref: DataFrame, docs: DataFrame | None) -> DataFrame:
    c = cand.where("is_active").join(xref.select("src_system", "src_key", "party_id"), ["src_system", "src_key"])
    trust_kyc = (F.when((F.col("src_system") == "cbs") & F.col("kyc_verified"), 0)
                 .when(F.col("src_system") == "cbs", 2).when(F.col("src_system") == "lms", 3).otherwise(4))
    name_trust = F.when(F.col("src_system") == "cbs", 0).when(F.col("src_system") == "crm", 1).otherwise(2)
    epoch = lambda col: F.coalesce(F.col(col).cast("double"), F.lit(0.0))  # noqa: E731
    base = [F.col("party_id"), F.col("src_system"), F.col("src_key")]

    def rows(attr, value, o1, o2=F.lit(0.0), o3=F.lit(0.0), pincode=F.lit(None).cast("string"),
             city=F.lit(None).cast("string"), src=c):
        return src.select(*base, F.lit(attr).alias("attribute"), value.cast("string").alias("value"),
                          pincode.alias("pincode"), city.alias("city"), o1.cast("double").alias("o1"),
                          o2.cast("double").alias("o2"), o3.cast("double").alias("o3")).where(F.col("value").isNotNull())

    has_initial = F.exists(F.split("name_std", " "), lambda t: F.length(t) == 1).cast("int")
    parts = [
        rows("full_name", F.col("name_std"), name_trust, has_initial, -F.length("name_std")),
        rows("dob", F.col("dob"), trust_kyc, -epoch("record_ts")),
        rows("pan", F.col("pan_std"), trust_kyc, -epoch("record_ts")),
        rows("gender", F.col("gender"), trust_kyc, -epoch("record_ts")),
        rows("mobile", F.col("mobile_e164"), -epoch("record_ts"), trust_kyc),
        rows("email", F.col("email_std"), -epoch("record_ts"), trust_kyc),
        rows("address", F.col("address_display"),
             F.when(F.col("src_system") == "cbs", 0).when(F.col("src_system") == "crm", 1).otherwise(2),
             -epoch("address_ts"), pincode=F.col("pincode_std"), city=F.col("city")),
        rows("segment", F.col("segment"), trust_kyc, -epoch("record_ts")),
        rows("home_branch", F.col("home_branch"), trust_kyc, -epoch("record_ts")),
        rows("kyc_status", F.col("kyc_status"), F.when(F.col("kyc_status") == "VERIFIED", 0).otherwise(1),
             -epoch("record_ts")),
    ]
    if docs is not None:
        d = (docs.where(F.col("party_id").isNotNull() & (F.col("extraction_status") == "OK"))
             .withColumn("src_system", F.lit("doc")).withColumn("src_key", F.col("file_name")))
        kyc = d.where("intent = 'KYC_DECLARATION'")
        moved = d.where("intent = 'ADDRESS_CHANGE'")
        parts += [
            rows("dob", F.col("dob"), F.lit(1), -epoch("document_ts"), src=kyc),
            rows("pan", F.col("pan_std"), F.lit(1), -epoch("document_ts"), src=kyc),
            rows("address", F.col("address_text"), F.lit(0), -epoch("document_ts"), pincode=F.col("pincode_std"),
                 src=moved),
        ]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def survive(attrs: DataFrame) -> tuple[DataFrame, DataFrame]:
    w = Window.partitionBy("party_id", "attribute").orderBy("o1", "o2", "o3", "src_system", "src_key")
    chosen = attrs.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "o1", "o2", "o3")
    rules = F.create_map(*[x for k, v in SURVIVORSHIP.items() for x in (F.lit(k), F.lit(v))])
    candidates = attrs.groupBy("party_id", "attribute").agg(F.countDistinct("value").alias("distinct_values"))
    golden_attr = (chosen.join(candidates, ["party_id", "attribute"])
                   .withColumn("rule", F.element_at(rules, F.col("attribute"))))
    wide = golden_attr.groupBy("party_id").pivot("attribute", list(SURVIVORSHIP)).agg(F.first("value"))
    addr = golden_attr.where("attribute = 'address'").select("party_id", F.col("pincode").alias("pincode"),
                                                             F.col("city").alias("address_city"))
    sources = golden_attr.groupBy("party_id").agg(F.map_from_entries(F.collect_list(F.struct(
        "attribute", F.concat_ws(":", "src_system", "src_key")))).alias("attribute_sources"))
    return wide.join(addr, "party_id", "left").join(sources, "party_id"), golden_attr


# ---------------------------------------------------------------- quality


def quality(spark, landing: str, xref_rows: list, bid: str, d) -> tuple | None:
    fs = C.HadoopFS(spark)
    uri = f"{landing}/_truth/persons.json"
    if not fs.exists(uri):
        return None
    truth = json.loads(fs.read_text(uri))
    person = {}
    for p in truth["persons"]:
        for system in ("cbs", "lms", "crm"):
            for key in p[system]:
                person[f"{system}:{key}"] = p["pid"]
    party = {f"{r['src_system']}:{r['src_key']}": r["party_id"] for r in xref_rows}
    known = [k for k in party if k in person]
    by_person, by_party = {}, {}
    for k in known:
        by_person.setdefault(person[k], []).append(k)
        by_party.setdefault(party[k], []).append(k)
    true_pairs = {frozenset(p) for ks in by_person.values() for p in combinations(sorted(ks), 2)}
    pred_pairs = {frozenset(p) for ks in by_party.values() for p in combinations(sorted(ks), 2)}
    correct = len(true_pairs & pred_pairs)
    split = sum(1 for ks in by_person.values() if len({party[k] for k in ks}) > 1)
    merged = sum(1 for ks in by_party.values() if len({person[k] for k in ks}) > 1)
    precision = correct / len(pred_pairs) if pred_pairs else 1.0
    recall = correct / len(true_pairs) if true_pairs else 1.0
    return (bid, d, len(known), len(by_party), len(true_pairs), len(pred_pairs), correct, round(precision, 4),
            round(recall, 4), split, merged, C._now())


# ---------------------------------------------------------------- run


def run(spark, argv=None) -> dict:
    args = C.parse(C.base_parser(__doc__), argv)
    names = C.Names(args.db_prefix)
    d, bid = args.business_date, C.batch_id(args.business_date)
    C.ensure_databases(spark, names)
    C.require_completed(spark, names, "silver", d)
    audit = C.Audit(spark, names, "build_mdm", d, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED")
    t = lambda name: names.t(STAGE, name)  # noqa: E731
    silver = ",".join(names.t("silver", x) for x in ("cbs_customer", "lms_borrower", "crm_customer"))
    summary = {}
    try:
        cand = candidates(spark, names, d).localCheckpoint()
        n_cand = cand.count()
        before = C.snapshot_id(spark, t("party_xref"))
        C.replace_table(cand.withColumn("batch_id", F.lit(bid)), t("party_candidate"))
        audit.transform("candidates", "standardise", silver, t("party_candidate"), n_cand, n_cand,
                        "CBS customers, LMS borrowers, CRM profiles with silver's standardised attributes")

        pairs = match_pairs(cand)
        prior = prior_links(spark, names, cand, pairs)
        if prior is not None:
            pairs = pairs.unionByName(prior, allowMissingColumns=True)
        pairs = pairs.localCheckpoint()
        by_rule = {r["rule"]: r["count"] for r in pairs.groupBy("rule").count().collect()}
        labels = components(spark, cand, pairs.where("decision = 'MERGE'"))
        xref = assign_parties(spark, names, cand, labels, pairs, bid).localCheckpoint()
        party_of = xref.select(F.concat_ws(":", "src_system", "src_key").alias("cid"), "party_id")
        pairs_out = (pairs.join(party_of.withColumnRenamed("cid", "id_a").withColumnRenamed("party_id", "party_a"), "id_a")
                     .join(party_of.withColumnRenamed("cid", "id_b").withColumnRenamed("party_id", "party_b"), "id_b")
                     .withColumn("batch_id", F.lit(bid)))
        C.replace_table(pairs_out, t("match_pair"))
        audit.transform("match", "match", t("party_candidate"), t("match_pair"), n_cand, pairs.count(),
                        ", ".join(f"{k} {v}" for k, v in sorted(by_rule.items())))
        C.replace_table(xref, t("party_xref"))
        n_party = xref.select("party_id").distinct().count()
        audit.transform("cluster", "deduplicate", t("match_pair"), t("party_xref"), n_cand, n_party,
                        "connected components over MERGE pairs; party_id kept from the previous xref")

        docs = documents_to_party(spark, names, xref, d)
        if docs is not None:
            C.replace_table(docs.withColumn("batch_id", F.lit(bid)), t("document_party"))
            audit.transform("link documents", "enrich", names.t("silver", "doc_extract"), t("document_party"),
                            None, None, "by KYC customer id, else by account number -> CBS customer -> party")
        golden, golden_attr = survive(attribute_rows(cand, xref, docs))
        members = xref.where("is_active").groupBy("party_id").agg(
            F.array_sort(F.collect_set("src_system")).alias("source_systems"),
            F.count(F.lit(1)).alias("member_records"),
            F.array_sort(F.collect_list(F.when(F.col("src_system") == "cbs", F.col("src_key")))).alias("cbs_cust_ids"),
            F.array_sort(F.collect_list(F.when(F.col("src_system") == "lms", F.col("src_key")))).alias("lms_borrower_ids"),
            F.array_sort(F.collect_list(F.when(F.col("src_system") == "crm", F.col("src_key")))).alias("crm_ids"))
        pans = cand.where("is_active").join(xref, ["src_system", "src_key"]).groupBy("party_id").agg(
            (F.countDistinct("pan_std") > 1).alias("has_pan_conflict"))
        golden = (golden.join(members, "party_id").join(pans, "party_id", "left")
                  .withColumn("dob", F.col("dob").cast("date"))
                  .withColumn("record_hash", F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(a), F.lit(""))
                                                                           for a in SURVIVORSHIP]), 256))
                  .withColumn("batch_id", F.lit(bid)).withColumn("business_date", F.lit(d.isoformat()).cast("date")))
        C.replace_table(golden, t("golden_party"))
        C.replace_table(golden_attr.withColumn("batch_id", F.lit(bid)), t("golden_attribute"))
        n_golden = golden.count()
        audit.transform("survive", "survive", f"{t('party_xref')},{t('document_party')}", t("golden_party"),
                        n_cand, n_golden, "; ".join(f"{k}: {v}" for k, v in SURVIVORSHIP.items()))

        q = quality(spark, C.as_uri(args.landing), xref.collect(), bid, d)
        if q is not None:
            C.ensure_table(spark, t("match_quality"), QUALITY_SCHEMA)
            spark.sql(f"DELETE FROM {t('match_quality')} WHERE batch_id = '{bid}'")
            spark.createDataFrame([q], QUALITY_SCHEMA).writeTo(t("match_quality")).append()
            print(f"match quality vs truth: precision {q[7]:.3f}, recall {q[8]:.3f}, "
                  f"{q[9]} persons split, {q[10]} parties mixing persons", flush=True)
        review = pairs.where("decision = 'REVIEW'").count()
        msg = (f"{n_cand} records -> {n_party} parties ({n_golden} with an active record); pairs "
               + ", ".join(f"{k} {v}" for k, v in sorted(by_rule.items())) + f"; {review} for review")
        print(msg, flush=True)
        audit.load(STAGE, "party_xref", "COMMITTED", rows_in=n_cand, rows_out=n_party, snapshot_before=before,
                   snapshot_after=C.snapshot_id(spark, t("party_xref")), message=msg)
        for name in ("golden_party", "golden_attribute", "document_party", "match_pair"):
            if C.table_exists(spark, t(name)):
                audit.load(STAGE, name, "COMMITTED", snapshot_after=C.snapshot_id(spark, t(name)))
        audit.load(STAGE, "*", "COMPLETED", rows_in=n_cand, rows_out=n_golden)
        summary = {"candidates": n_cand, "parties": n_party, "golden": n_golden, "pairs": by_rule}
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return summary


def main() -> int:
    spark = C.get_spark("gdl-build-mdm")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
