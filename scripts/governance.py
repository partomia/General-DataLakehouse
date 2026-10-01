"""
Governance of the rsingh_gdl_* databases, from config/governance.json and config/kpi.json:

  classifications  GDL_PII_* Atlas classifications on every PII column of bronze, silver,
                   MDM, gold and semantic (by column name; dob by type), not propagated, so
                   a derived column is never masked by a tag it did not ask for
  glossary         "GDL Banking KPIs": one term per certified KPI, assigned to the certified
                   view and to every MIS view and regulatory dataset that reads it
  masking          one Ranger tag masking policy per classification (rsingh-gdl-pii-*), for
                   the masked users; everyone else (rsingh) sees clear values

    python scripts/governance.py             # plan: what apply would change (read-only)
    python scripts/governance.py apply
    python scripts/governance.py verify      # exit 1 unless everything is in place

Talks to Atlas and Ranger through the data lake's Knox gateway (datalake_api in
config/pipeline.json) as GDL_WORKLOAD_USER / GDL_WORKLOAD_PASSWORD. Only GDL_* classifications,
the GDL glossary and rsingh-gdl-* policies are created or changed. Run it after the tables
exist (after the first CDE run and the semantic step); re-run it after a table is recreated.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TAG_PREFIX = "GDL_"
PAGE = 100
TABLE_TYPES = ("iceberg_table", "hive_table")      # Iceberg tables are iceberg_table in Atlas; views hive_table
COLUMN_TYPES = ("iceberg_column", "hive_column")


def load_config() -> tuple[dict, dict, str]:
    gov = json.loads((ROOT / "config" / "governance.json").read_text())
    kpi = json.loads((ROOT / "config" / "kpi.json").read_text())
    api = json.loads((ROOT / "config" / "pipeline.json").read_text())["datalake_api"]
    return gov, kpi, api


# ---------------------------------------------------------------- plan (pure)


def column_tag(gov: dict, name: str, col_type: str) -> str | None:
    rule = gov["columns"].get(name.lower())
    if isinstance(rule, dict):
        return rule["date" if (col_type or "").lower() == "date" else "string"]
    return rule


def classification_changes(gov: dict, columns: list[dict]) -> tuple[list, list]:
    """columns: {guid, qualifiedName, name, type, tags, entity} -> (adds, removes) of (guid, qualifiedName,
    tag). With profiler_tags_on_tables, a GDL tag on a table column that the name rules do not ask for is
    the profiler's (config/profiler_tag_rules.json) and is kept."""
    keep = gov.get("profiler_tags_on_tables", False)
    adds, removes = [], []
    for c in columns:
        want = column_tag(gov, c["name"], c["type"])
        have = {t for t in c["tags"] if t.startswith(TAG_PREFIX)}
        if want and want not in have:
            adds.append((c["guid"], c["qualifiedName"], want))
        if not (keep and c.get("entity") == "iceberg_column"):
            removes += [(c["guid"], c["qualifiedName"], t) for t in sorted(have - {want})]
    return adds, removes


def policy_name(gov: dict, tag: str) -> str:
    return gov["masking"]["policy_prefix"] + tag[len(TAG_PREFIX + "PII_"):].lower().replace("_", "-")


def masking_policy(gov: dict, tag: str) -> dict:
    spec = gov["classifications"][tag]
    return {
        "service": gov["masking"]["service"], "name": policy_name(gov, tag), "policyType": 1, "isEnabled": True,
        "description": f"General Data Lakehouse: {spec['description']}",
        "resources": {"tag": {"values": [tag], "isExcludes": False, "isRecursive": False}},
        "dataMaskPolicyItems": [{"users": sorted(gov["masking"]["masked_users"]), "groups": [], "roles": [],
                                 "accesses": [{"type": "hive:select", "isAllowed": True}],
                                 "dataMaskInfo": spec["mask"], "delegateAdmin": False}],
    }


def policy_differs(want: dict, have: dict) -> bool:
    def key(p):
        items = [(sorted(i.get("users", [])), i.get("dataMaskInfo", {}).get("dataMaskType"),
                  i.get("dataMaskInfo", {}).get("valueExpr"), sorted(a["type"] for a in i.get("accesses", [])))
                 for i in p.get("dataMaskPolicyItems", [])]
        return p.get("isEnabled"), p["resources"]["tag"]["values"], p.get("description"), items
    return key(want) != key(have)


def glossary_terms(gov: dict, kpi: dict) -> list[dict]:
    out = []
    for k in kpi["kpis"]:
        out.append({"name": k["glossary_term"], "abbreviation": k["kpi_code"], "shortDescription": k["name"],
                    "longDescription": f"{k['definition']}\n\nFormula: {k['formula']}\nGrain: {k['grain']}\n"
                                       f"Certified view: {k['certified_view']} (owner {k['owner']}, version "
                                       f"{k['version']}, certified {k['certified_on']})",
                    "assigned_to": gov["glossary"]["assigned_to"][k["kpi_code"]]})
    return out


# ---------------------------------------------------------------- REST


class Api:
    def __init__(self, base: str, user: str, password: str):
        self.base = base.rstrip("/")
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def call(self, method: str, path: str, body=None, ok404: bool = False):
        req = urllib.request.Request(self.base + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={"Authorization": self.auth, "Content-Type": "application/json",
                                              "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                text = r.read().decode()
        except urllib.error.HTTPError as e:
            if ok404 and e.code == 404:
                return None
            raise RuntimeError(f"{method} {path}: HTTP {e.code}: {e.read().decode()[:400]}") from None
        return json.loads(text) if text.strip() else None

    def atlas(self, method, path, body=None, **kw):
        return self.call(method, "/atlas/api/atlas/v2" + path, body, **kw)

    def ranger(self, method, path, body=None, **kw):
        return self.call(method, "/ranger/service/public/v2/api" + path, body, **kw)


def atlas_columns(api: Api, db: str) -> list[dict]:
    return [c for t in COLUMN_TYPES for c in _columns(api, db, t)]


def _columns(api: Api, db: str, type_name: str) -> list[dict]:
    out, offset = [], 0
    while True:
        r = api.atlas("POST", "/search/basic", {
            "typeName": type_name, "excludeDeletedEntities": True, "limit": PAGE, "offset": offset,
            "attributes": ["qualifiedName", "name", "type"],
            "entityFilters": {"attributeName": "qualifiedName", "operator": "startsWith", "attributeValue": f"{db}."}})
        page = r.get("entities") or []
        out += [{"guid": e["guid"], "qualifiedName": e["attributes"]["qualifiedName"],
                 "name": e["attributes"].get("name") or e["attributes"]["qualifiedName"].split("@")[0].split(".")[-1],
                 "type": e["attributes"].get("type") or "", "tags": e.get("classificationNames") or [],
                 "entity": type_name}
                for e in page]
        if len(page) < PAGE:
            return out
        offset += PAGE


def table_guid(api: Api, gov: dict, table: str) -> str | None:
    qn = urllib.parse.quote(f"{table}@{gov['atlas_cluster']}")
    for t in TABLE_TYPES:
        r = api.atlas("GET", f"/entity/uniqueAttribute/type/{t}?attr:qualifiedName={qn}&minExtInfo=true", ok404=True)
        if r and r["entity"].get("status") == "ACTIVE":
            return r["entity"]["guid"]
    return None


# ---------------------------------------------------------------- steps


class Run:
    def __init__(self, api: Api, gov: dict, kpi: dict, apply: bool):
        self.api, self.gov, self.kpi, self.apply, self.pending = api, gov, kpi, apply, 0

    def todo(self, what: str) -> None:
        self.pending += 1
        print(("APPLY " if self.apply else "PLAN  ") + what, flush=True)

    def typedefs(self) -> None:
        have = {t["name"] for t in self.api.atlas("GET", "/types/typedefs/headers?type=classification")}
        missing = [t for t in self.gov["classifications"] if t not in have]
        for t in missing:
            self.todo(f"create classification {t}")
        if missing and self.apply:
            self.api.atlas("POST", "/types/typedefs", {"classificationDefs": [
                {"name": t, "description": self.gov["classifications"][t]["description"], "superTypes": [],
                 "attributeDefs": [], "entityTypes": []} for t in missing]})

    def classifications(self) -> None:
        cols = [c for db in self.gov["databases"] for c in atlas_columns(self.api, db)]
        adds, removes = classification_changes(self.gov, cols)
        tagged = sum(1 for c in cols if column_tag(self.gov, c["name"], c["type"]))
        print(f"atlas: {len(cols)} columns in {len(self.gov['databases'])} databases, {tagged} PII "
              f"({len(adds)} to tag, {len(removes)} to untag)", flush=True)
        for tag in sorted({a[2] for a in adds}):
            guids = [g for g, _, t in adds if t == tag]
            for _, qn, _ in [a for a in adds if a[2] == tag]:
                self.todo(f"tag {qn} {tag}")
            if self.apply:
                for i in range(0, len(guids), 50):
                    self.api.atlas("POST", "/entity/bulk/classification", {
                        "classification": {"typeName": tag, "propagate": False}, "entityGuids": guids[i:i + 50]})
        for guid, qn, tag in removes:
            self.todo(f"untag {qn} {tag}")
            if self.apply:
                self.api.atlas("DELETE", f"/entity/guid/{guid}/classification/{tag}")

    def glossary(self) -> None:
        g = self.gov["glossary"]
        found = [x for x in self.api.atlas("GET", "/glossary?limit=-1") if x["name"] == g["name"]]
        if not found:
            self.todo(f"create glossary {g['name']}")
            if not self.apply:
                for t in glossary_terms(self.gov, self.kpi):
                    self.todo(f"create term {t['name']} -> {', '.join(t['assigned_to'])}")
                return
            found = [self.api.atlas("POST", "/glossary", {"name": g["name"], "shortDescription": g["name"],
                                                          "longDescription": g["description"]})]
        gguid = found[0]["guid"]
        terms = {t["name"]: t for t in self.api.atlas("GET", f"/glossary/{gguid}/terms?limit=-1") or []}
        for t in glossary_terms(self.gov, self.kpi):
            body = {k: t[k] for k in ("name", "abbreviation", "shortDescription", "longDescription")}
            have = terms.get(t["name"])
            if not have:
                self.todo(f"create term {t['name']}")
                if not self.apply:
                    continue
                have = self.api.atlas("POST", "/glossary/term", {**body, "anchor": {"glossaryGuid": gguid}})
            elif any(have.get(k) != v for k, v in body.items()):
                self.todo(f"update term {t['name']}")
                if self.apply:
                    self.api.atlas("PUT", f"/glossary/term/{have['guid']}",
                                   {**have, **body, "anchor": {"glossaryGuid": gguid}})
            assigned = {e["guid"] for e in self.api.atlas("GET", f"/glossary/terms/{have['guid']}/assignedEntities")
                        or []}
            semantic = self.gov["databases"][-1]
            for name in t["assigned_to"]:
                guid = table_guid(self.api, self.gov, f"{semantic}.{name}")
                if guid is None:
                    raise RuntimeError(f"{semantic}.{name} not in Atlas: run the semantic step first")
                if guid not in assigned:
                    self.todo(f"assign term {t['name']} to {semantic}.{name}")
                    if self.apply:
                        self.api.atlas("POST", f"/glossary/terms/{have['guid']}/assignedEntities", [{"guid": guid}])

    def masking(self) -> None:
        svc = self.gov["masking"]["service"]
        for tag in self.gov["classifications"]:
            want = masking_policy(self.gov, tag)
            found = self.api.ranger("GET", f"/service/{svc}/policy?policyName={urllib.parse.quote(want['name'])}")
            have = found[0] if found else None
            if have is None:
                self.todo(f"create Ranger policy {want['name']} ({tag}: {want['dataMaskPolicyItems'][0]['dataMaskInfo']['dataMaskType']})")
                if self.apply:
                    self.api.ranger("POST", "/policy", want)
            elif policy_differs(want, have):
                self.todo(f"update Ranger policy {want['name']}")
                if self.apply:
                    self.api.ranger("PUT", f"/policy/{have['id']}", {**have, **want, "id": have["id"]})


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", nargs="?", choices=("plan", "apply", "verify"), default="plan")
    p.add_argument("--only", choices=("typedefs", "classifications", "glossary", "masking"), action="append")
    args = p.parse_args(argv)
    user, password = os.environ.get("GDL_WORKLOAD_USER"), os.environ.get("GDL_WORKLOAD_PASSWORD")
    if not user or not password:
        raise SystemExit("set GDL_WORKLOAD_USER and GDL_WORKLOAD_PASSWORD (CDP workload user)")
    gov, kpi, base = load_config()
    run = Run(Api(base, user, password), gov, kpi, apply=args.command == "apply")
    for step in args.only or ("typedefs", "classifications", "glossary", "masking"):
        getattr(run, step)()
    if args.command == "verify":
        print(f"verify: {'OK' if not run.pending else f'{run.pending} change(s) outstanding'}")
        return 1 if run.pending else 0
    print(f"{run.pending} change(s) {'applied' if args.command == 'apply' else 'planned'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
