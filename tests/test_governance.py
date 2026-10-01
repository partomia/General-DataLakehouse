import json
import re
import sys

from conftest import ROOT

sys.path.insert(0, str(ROOT / "scripts"))
import governance as G  # noqa: E402

GOV, KPI, _ = G.load_config()
CLEAR = {"pincode", "ifsc", "branch_name", "product_name", "file_name", "list_name"}   # not personal data, kept clear
PII = re.compile(r"name|pan|aadhaar|mobile|phone|email|dob|birth|address|passport|acct_no", re.I)


def test_every_personal_column_of_the_sources_is_classified():
    for f in (ROOT / "contracts").glob("*.json"):
        cols = [c["name"] if isinstance(c, dict) else c for c in json.loads(f.read_text()).get("columns", [])]
        for c in cols:
            if PII.search(c) and c not in CLEAR:
                assert c in GOV["columns"], f"{f.name}: {c} has no classification"


def test_rules_use_own_classifications_and_policies():
    tags = set(GOV["classifications"])
    assert all(t.startswith("GDL_PII_") for t in tags)
    for rule in GOV["columns"].values():
        assert set(rule.values() if isinstance(rule, dict) else [rule]) <= tags
    names = [G.policy_name(GOV, t) for t in tags]
    assert len(set(names)) == len(names) and all(n.startswith("rsingh-gdl-pii-") for n in names)
    assert G.policy_name(GOV, "GDL_PII_LAST_4") == "rsingh-gdl-pii-last-4"


def test_masking_policies_cover_only_the_masked_users():
    for tag in GOV["classifications"]:
        p = G.masking_policy(GOV, tag)
        assert p["service"] == "cm_tag" and p["policyType"] == 1
        assert p["resources"]["tag"]["values"] == [tag]
        (item,) = p["dataMaskPolicyItems"]
        assert item["users"] == ["federal01", "federal07"] and not item["groups"]
        assert item["dataMaskInfo"]["dataMaskType"].startswith("hive:")
        assert not G.policy_differs(p, json.loads(json.dumps(p)))


def test_date_of_birth_is_masked_by_type():
    assert G.column_tag(GOV, "dob", "date") == "GDL_PII_YEAR"
    assert G.column_tag(GOV, "DOB", "string") == "GDL_PII_REDACT"
    assert G.column_tag(GOV, "pan_std", "string") == "GDL_PII_LAST_4"
    assert G.column_tag(GOV, "party_sk", "bigint") is None


def test_changes_leave_other_projects_tags_alone():
    cols = [{"guid": "1", "qualifiedName": "d.t.pan@cm", "name": "pan", "type": "string", "tags": ["PII_LAST_4"]},
            {"guid": "2", "qualifiedName": "d.t.city@cm", "name": "city", "type": "string",
             "tags": ["GDL_PII_HASH", "PII_HASH"]},
            {"guid": "3", "qualifiedName": "d.t.email@cm", "name": "email", "type": "string",
             "tags": ["GDL_PII_HASH"]}]
    adds, removes = G.classification_changes(GOV, cols)
    assert adds == [("1", "d.t.pan@cm", "GDL_PII_LAST_4")]
    assert removes == [("2", "d.t.city@cm", "GDL_PII_HASH")]


def test_glossary_terms_go_to_the_views_that_read_the_certified_kpi():
    sql = "\n".join(f.read_text() for f in sorted((ROOT / "sql" / "semantic").glob("*.sql")))
    body = {}
    for m in re.finditer(r"(?:CREATE (?:OR REPLACE )?VIEW|INSERT OVERWRITE TABLE)\s+rsingh_gdl_semantic\.(\w+)"
                         r"(.*?)(?=;\s*$)", sql, flags=re.S | re.M):
        body[m.group(1)] = body.get(m.group(1), "") + m.group(2)
    for term in G.glossary_terms(GOV, KPI):
        k = next(x for x in KPI["kpis"] if x["glossary_term"] == term["name"])
        cert = k["certified_view"]
        assert cert in term["assigned_to"]
        for obj in term["assigned_to"]:
            reads = re.search(rf"rsingh_gdl_semantic\.{cert}\b", body[obj])
            read_by = re.search(rf"rsingh_gdl_semantic\.{obj}\b", body[cert])
            assert obj == cert or reads or read_by, f"{obj} neither reads nor feeds {cert}"
        assert k["definition"] in term["longDescription"]
