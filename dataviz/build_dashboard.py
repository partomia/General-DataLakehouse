#!/usr/bin/env python3
"""
The three General Data Lakehouse dashboards in Cloudera Data Visualization, as code:

  GDL Banking KPIs MIS             NPA exposure, CASA ratio and customer relationship value,
                                   from the MIS views (each aggregates one certified KPI view)
  GDL Reconciliation & Data Quality  every reconciliation and KPI consistency check, the load
                                   audit and the failed-batch trail (dash_recon, dash_load_audit)
  GDL MDM & Golden Record          golden records, match rules and decisions, the review
                                   queue and match quality against the generator truth

Datasets, visuals and sheets are declared below; this script turns them into one Data
Visualization export file (dataviz/gdl_dashboards.json) with fixed UUIDs and primary keys,
so an import updates the dashboards in place. Every dataset is a view in rsingh_gdl_semantic
(sql/semantic/20_mis_views.sql, 50_dashboard_views.sql).

  python dataviz/build_dashboard.py                          # write the file (column types from Impala)
  python dataviz/build_dashboard.py --import --connection X  # and import it into the CDW instance
  python dataviz/build_dashboard.py --check                  # every visual's query directly on Impala
  python dataviz/build_dashboard.py --verify                 # every visual through the Data API
  python dataviz/build_dashboard.py --list-connections

Column types come from Impala (GDL_IMPALA_USER / GDL_IMPALA_PASSWORD). The CDW Data
Visualization instance (config/pipeline.json dataviz_url) signs users in with SAML, so the API
calls (--import, --verify, --list-connections) need a Data Visualization API key in
GDL_VIZ_API_KEY. Without one, import the file in the UI: Data -> Import Visual Artifacts, and
pick a connection to the federal Impala warehouse.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

OUT = ROOT / "dataviz" / "gdl_dashboards.json"
DB = "rsingh_gdl_semantic"
NS = uuid.UUID("5b0e7d2a-3c61-4f8e-9a57-2d4c0b9e6f13")
DASHBOARD_PK0 = 9000
DATASET_PK0 = 9100
VISUAL_PK0 = 9200
# The export format version, used when the instance's own cannot be read (no API key).
DEFAULT_VERSION = {"Arcviz Version": "8.1.4.1000", "Description": "8.1.4.1000-4"}

DATASETS = {                          # key: (name, view, integer columns that are dimensions)
    "npa_trend": ("GDL - NPA trend", "mis_npa_trend", {"is_latest"}),
    "npa_breakdown": ("GDL - NPA breakdown", "mis_npa_breakdown", {"is_latest"}),
    "casa_trend": ("GDL - CASA trend", "mis_casa_trend", {"is_latest"}),
    "casa_breakdown": ("GDL - CASA breakdown", "mis_casa_breakdown", {"is_latest"}),
    "crv_segment": ("GDL - CRV by segment", "mis_crv_segment", {"is_latest"}),
    "crv_top": ("GDL - CRV top relationships", "mis_crv_top_relationships", {"is_latest", "crv_rank"}),
    "recon": ("GDL - Reconciliation", "dash_recon", {"is_latest"}),
    "audit": ("GDL - Load audit", "dash_load_audit", {"is_latest", "is_final"}),
    "match_quality": ("GDL - Match quality", "dash_match_quality", {"is_latest"}),
    "match_pair": ("GDL - Match pairs", "dash_match_pair", set()),
    "xref": ("GDL - Party cross-reference", "dash_party_xref", {"cluster_size"}),
    "golden": ("GDL - Golden parties", "dash_golden_party", {"member_records"}),
}

LATEST = "[is_latest] = 1"
LAKH = 100000.0
CRORE = 10000000.0
pct = lambda e: f"round(100 * {e}, 2)"  # noqa: E731

KPI_SHEETS = [
    ("NPA exposure", [
        dict(type="kpi", ds="npa_trend", title="Gross NPA ratio %", measures=[(pct("max([gross_npa_ratio])"),
             "Gross NPA %")], filters=[LATEST], pos=(1, 1, 13, 10)),
        dict(type="kpi", ds="npa_trend", title="Gross NPA (lakh)",
             measures=[(f"round(sum([gross_npa]) / {LAKH}, 2)", "Gross NPA lakh")], filters=[LATEST],
             pos=(14, 1, 13, 10)),
        dict(type="kpi", ds="npa_trend", title="Net NPA (lakh)",
             measures=[(f"round(sum([net_npa]) / {LAKH}, 2)", "Net NPA lakh")], filters=[LATEST], pos=(27, 1, 13, 10)),
        dict(type="kpi", ds="npa_trend", title="Provision coverage %",
             measures=[(pct("max([provision_coverage])"), "Coverage %")], filters=[LATEST], pos=(40, 1, 13, 10)),
        dict(type="kpi", ds="npa_trend", title="NPA loans", measures=[("sum([npa_loans])", "NPA loans")],
             filters=[LATEST], pos=(53, 1, 12, 10)),
        dict(type="trellis-lines", ds="npa_trend", title="Gross NPA ratio % by reporting date",
             x=[("reporting_date", "Reporting date")], measures=[(pct("max([gross_npa_ratio])"), "Gross NPA %")],
             pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="npa_breakdown", title="Gross NPA (lakh) by region and asset class",
             x=[("region", "Region")], measures=[(f"round(sum([gross_npa]) / {LAKH}, 2)", "Gross NPA lakh")],
             color=[("asset_class", "Asset class")], filters=[LATEST, "[asset_class] <> 'STANDARD'"],
             pos=(33, 11, 32, 22)),
        dict(type="trellis-bars", ds="npa_breakdown", title="Gross NPA ratio % by product",
             x=[("product_name", "Product")], measures=[(pct("sum([gross_npa]) / sum([gross_advances])"),
                                                         "Gross NPA %")],
             filters=[LATEST], sort_desc=True, pos=(1, 33, 32, 22)),
        dict(type="table", ds="npa_breakdown", title="NPA by branch (latest reporting date)",
             dims=[("region", "Region"), ("branch_name", "Branch")],
             measures=[("sum([loans])", "Loans"), (f"round(sum([gross_advances]) / {LAKH}, 2)", "Advances lakh"),
                       (f"round(sum([gross_npa]) / {LAKH}, 2)", "Gross NPA lakh"),
                       (pct("sum([gross_npa]) / sum([gross_advances])"), "Gross NPA %"),
                       (f"round(sum([provision_amount]) / {LAKH}, 2)", "Provision lakh")],
             filters=[LATEST], sort_dim="region", pos=(33, 33, 32, 22)),
    ]),
    ("CASA ratio", [
        dict(type="kpi", ds="casa_trend", title="CASA ratio %", measures=[(pct("max([casa_ratio])"), "CASA %")],
             filters=[LATEST], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="casa_trend", title="Total deposits (crore)",
             measures=[(f"round(sum([total_deposits]) / {CRORE}, 2)", "Deposits crore")], filters=[LATEST],
             pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="casa_trend", title="CASA balances (crore)",
             measures=[(f"round(sum([casa_balance]) / {CRORE}, 2)", "CASA crore")], filters=[LATEST],
             pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="casa_trend", title="Deposit accounts", measures=[("sum([accounts])", "Accounts")],
             filters=[LATEST], pos=(49, 1, 16, 10)),
        dict(type="trellis-lines", ds="casa_trend", title="CASA ratio % by reporting date",
             x=[("reporting_date", "Reporting date")], measures=[(pct("max([casa_ratio])"), "CASA %")],
             pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="casa_breakdown", title="CASA ratio % by region",
             x=[("region", "Region")], measures=[(pct("sum([casa_balance]) / sum([total_deposits])"), "CASA %")],
             filters=[LATEST], sort_desc=True, pos=(33, 11, 32, 22)),
        dict(type="trellis-bars", ds="casa_breakdown", title="Deposits (crore) by customer segment: CASA and term",
             x=[("segment", "Segment")], measures=[(f"round(sum([casa_balance]) / {CRORE}, 2)", "CASA crore"),
                                                   (f"round(sum([term_balance]) / {CRORE}, 2)", "Term crore")],
             filters=[LATEST], pos=(1, 33, 32, 22)),
        dict(type="table", ds="casa_breakdown", title="CASA ratio by branch (latest reporting date)",
             dims=[("region", "Region"), ("branch_name", "Branch")],
             measures=[("sum([accounts])", "Accounts"), (f"round(sum([total_deposits]) / {LAKH}, 2)", "Deposits lakh"),
                       (pct("sum([casa_balance]) / sum([total_deposits])"), "CASA %")],
             filters=[LATEST], sort_dim="region", pos=(33, 33, 32, 22)),
    ]),
    ("Customer relationship value", [
        dict(type="kpi", ds="crv_segment", title="Customers valued", measures=[("sum([parties])", "Parties")],
             filters=[LATEST], pos=(1, 1, 21, 10)),
        dict(type="kpi", ds="crv_segment", title="Total relationship value (lakh a year)",
             measures=[(f"round(sum([crv_total]) / {LAKH}, 2)", "CRV lakh")], filters=[LATEST], pos=(22, 1, 21, 10)),
        dict(type="kpi", ds="crv_segment", title="Average value per customer",
             measures=[("round(sum([crv_total]) / sum([parties]), 0)", "Average CRV")], filters=[LATEST],
             pos=(43, 1, 22, 10)),
        dict(type="trellis-bars", ds="crv_segment", title="Relationship value (lakh) by segment and component",
             x=[("segment", "Segment")],
             measures=[(f"round(sum([casa_value]) / {LAKH}, 2)", "CASA"), (f"round(sum([td_value]) / {LAKH}, 2)", "Term"),
                       (f"round(sum([lending_value]) / {LAKH}, 2)", "Lending"),
                       (f"round(sum([fee_value]) / {LAKH}, 2)", "Fees")],
             filters=[LATEST], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="crv_segment", title="Customers by value band",
             x=[("value_band", "Value band")], measures=[("sum([parties])", "Parties")],
             color=[("segment", "Segment")], filters=[LATEST], pos=(33, 11, 32, 22)),
        dict(type="table", ds="crv_top", title="The 25 most valuable relationships",
             dims=[("crv_rank", "Rank"), ("party_id", "Party"), ("full_name", "Name"), ("segment", "Segment"),
                   ("home_branch", "Home branch")],
             measures=[("sum([casa_value])", "CASA"), ("sum([td_value])", "Term"),
                       ("sum([lending_value])", "Lending"), ("sum([fee_value])", "Fees"), ("sum([crv])", "CRV")],
             filters=[LATEST], sort_dim="crv_rank", limit=25, pos=(1, 33, 64, 26)),
    ]),
    ("Trends", [
        dict(type="trellis-lines", ds="npa_trend", title="Gross and net NPA (lakh) by reporting date",
             x=[("reporting_date", "Reporting date")],
             measures=[(f"round(sum([gross_npa]) / {LAKH}, 2)", "Gross NPA"),
                       (f"round(sum([net_npa]) / {LAKH}, 2)", "Net NPA")], pos=(1, 1, 32, 22)),
        dict(type="trellis-lines", ds="casa_trend", title="CASA and term balances (crore) by reporting date",
             x=[("reporting_date", "Reporting date")],
             measures=[(f"round(sum([casa_balance]) / {CRORE}, 2)", "CASA"),
                       (f"round(sum([term_balance]) / {CRORE}, 2)", "Term")], pos=(33, 1, 32, 22)),
        dict(type="trellis-lines", ds="crv_segment", title="Relationship value (lakh) by reporting date and segment",
             x=[("reporting_date", "Reporting date")], measures=[(f"round(sum([crv_total]) / {LAKH}, 2)", "CRV lakh")],
             color=[("segment", "Segment")], pos=(1, 23, 64, 22)),
    ]),
]

RECON_SHEETS = [
    ("Latest batch", [
        dict(type="kpi", ds="recon", title="Checks on the latest batch", measures=[("sum(1)", "Checks")],
             filters=[LATEST], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="recon", title="Matched", measures=[("sum([is_matched])", "Matched")],
             filters=[LATEST], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="recon", title="Explained (difference with a known cause)",
             measures=[("sum([is_explained])", "Explained")], filters=[LATEST], pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="recon", title="Mismatches", measures=[("sum([is_mismatch])", "Mismatches")],
             filters=[LATEST], pos=(49, 1, 16, 10)),
        dict(type="trellis-bars", ds="recon", title="Checks by layer",
             x=[("layer_label", "Layer")], measures=[("sum([is_matched])", "Matched"),
                                                     ("sum([is_explained])", "Explained"),
                                                     ("sum([is_mismatch])", "Mismatch")],
             filters=[LATEST], pos=(1, 11, 24, 22)),
        dict(type="table", ds="recon", title="Explained and mismatched checks, with the reason",
             dims=[("layer_label", "Layer"), ("entity", "Entity"), ("check_name", "Check"), ("status", "Status"),
                   ("detail", "Detail")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([difference])", "Difference")],
             filters=[LATEST, "[status] <> 'MATCHED'"], may_be_empty=True, sort_dim="layer_label",
             pos=(25, 11, 40, 22)),
        dict(type="table", ds="recon", title="KPI consistency: every consumer against the certified view",
             dims=[("entity", "KPI"), ("check_name", "Consumer"), ("status", "Status"), ("detail", "What")],
             measures=[("sum([expected])", "Certified"), ("sum([actual])", "Consumer"),
                       ("sum([difference])", "Difference")],
             filters=[LATEST, "[layer] = 'semantic'"], sort_dim="entity", pos=(1, 33, 64, 26)),
    ]),
    ("By business date", [
        dict(type="trellis-bars", ds="recon", title="Checks per business date",
             x=[("business_date", "Business date")], measures=[("sum([is_matched])", "Matched"),
                                                               ("sum([is_explained])", "Explained"),
                                                               ("sum([is_mismatch])", "Mismatch")],
             pos=(1, 1, 32, 22)),
        dict(type="trellis-bars", ds="recon", title="Mismatches per business date and layer",
             x=[("business_date", "Business date")], measures=[("sum([is_mismatch])", "Mismatches")],
             color=[("layer_label", "Layer")], pos=(33, 1, 32, 22)),
        dict(type="table", ds="recon", title="Every mismatch (the planted trailer error and any failed batch)",
             dims=[("business_date", "Business date"), ("layer_label", "Layer"), ("entity", "Entity"),
                   ("check_name", "Check"), ("detail", "Detail")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([difference])", "Difference")],
             filters=["[status] = 'MISMATCH'"], may_be_empty=True, sort_dim="business_date", sort_asc=False,
             pos=(1, 23, 64, 22)),
    ]),
    ("Load audit", [
        dict(type="kpi", ds="audit", title="Failed attempts (all batches)", measures=[("sum([failed])", "Failed")],
             pos=(1, 1, 21, 10)),
        dict(type="kpi", ds="audit", title="Entities committed on the latest batch",
             measures=[("sum(case when [status] = 'COMMITTED' then 1 else 0 end)", "Committed")],
             filters=[LATEST, "[is_final] = 1"], pos=(22, 1, 21, 10)),
        dict(type="kpi", ds="audit", title="Rows rejected to quarantine (latest batch)",
             measures=[("sum([rows_rejected])", "Rejected")],
             filters=[LATEST, "[stage] = 'bronze'", "[entity] <> '*'"], pos=(43, 1, 22, 10)),
        dict(type="trellis-bars", ds="audit", title="Rows written per business date and stage",
             x=[("business_date", "Business date")], measures=[("sum([rows_out])", "Rows")],
             color=[("stage_label", "Stage")], filters=["[is_final] = 1", "[entity] <> '*'"], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="audit", title="Bronze rows rejected per entity (all batches)",
             x=[("entity", "Entity")], measures=[("sum([rows_rejected])", "Rejected")],
             filters=["[stage] = 'bronze'", "[entity] <> '*'", "[is_final] = 1"], sort_desc=True, pos=(33, 11, 32, 22)),
        dict(type="table", ds="audit", title="The failed-batch trail: every failed attempt and its error",
             dims=[("business_date", "Business date"), ("stage_label", "Stage"), ("entity", "Entity"),
                   ("logged_at", "Logged at"), ("message", "Error")],
             measures=[("sum([rows_out])", "Rows")], filters=["[status] = 'FAILED'"], may_be_empty=True,
             sort_dim="logged_at", sort_asc=False, pos=(1, 33, 64, 18)),
        dict(type="table", ds="audit", title="Stage results of the latest batch",
             dims=[("stage_label", "Stage"), ("entity", "Entity"), ("status", "Status"), ("logged_at", "Logged at")],
             measures=[("sum([rows_in])", "Rows in"), ("sum([rows_out])", "Rows out"),
                       ("sum([rows_rejected])", "Rejected")],
             filters=[LATEST, "[is_final] = 1"], sort_dim="stage_label", pos=(1, 51, 64, 26)),
    ]),
]

MDM_SHEETS = [
    ("Golden records", [
        dict(type="kpi", ds="golden", title="Golden parties", measures=[("sum(1)", "Parties")], pos=(1, 1, 13, 10)),
        dict(type="kpi", ds="golden", title="Parties merged from 2+ records", measures=[("sum([is_merged])", "Merged")],
             pos=(14, 1, 13, 10)),
        dict(type="kpi", ds="xref", title="Active source records", measures=[("sum([is_active])", "Records")],
             pos=(27, 1, 13, 10)),
        dict(type="kpi", ds="match_quality", title="Match precision (vs generator truth)",
             measures=[("max([match_precision])", "Precision")], filters=[LATEST], pos=(40, 1, 13, 10)),
        dict(type="kpi", ds="match_quality", title="Match recall (vs generator truth)",
             measures=[("max([match_recall])", "Recall")], filters=[LATEST], pos=(53, 1, 12, 10)),
        dict(type="trellis-bars", ds="xref", title="Source records by resolving rule and system",
             x=[("match_rule", "Rule")], measures=[("sum([is_active])", "Records")],
             color=[("src_system", "Source")], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="golden", title="Golden parties by segment and KYC status",
             x=[("segment", "Segment")], measures=[("sum(1)", "Parties")], color=[("kyc_status", "KYC")],
             pos=(33, 11, 32, 22)),
        dict(type="trellis-bars", ds="golden", title="Golden parties by number of source records",
             x=[("member_records", "Source records")], measures=[("sum(1)", "Parties")], pos=(1, 33, 32, 22)),
        dict(type="table", ds="golden", title="Parties with conflicting PANs across sources",
             dims=[("party_id", "Party"), ("full_name", "Name"), ("segment", "Segment"), ("home_branch", "Branch")],
             measures=[("sum([member_records])", "Records")], filters=["[has_pan_conflict] = 1"], may_be_empty=True,
             sort_dim="party_id", pos=(33, 33, 32, 22)),
    ]),
    ("Matching", [
        dict(type="trellis-bars", ds="match_pair", title="Candidate pairs by rule and decision",
             x=[("rule", "Rule")], measures=[("sum(1)", "Pairs")], color=[("decision", "Decision")],
             pos=(1, 1, 32, 22)),
        dict(type="trellis-lines", ds="match_quality", title="Precision and recall per batch",
             x=[("business_date", "Business date")],
             measures=[("max([match_precision])", "Precision"), ("max([match_recall])", "Recall")], pos=(33, 1, 32, 22)),
        dict(type="table", ds="match_pair", title="Review queue: similar name and DOB, kept apart",
             dims=[("id_a", "Record A"), ("name_a", "Name A"), ("id_b", "Record B"), ("name_b", "Name B"),
                   ("rule", "Rule")],
             measures=[("max([score])", "Score"), ("max([name_similarity])", "Name similarity")],
             filters=["[for_review] = 1"], may_be_empty=True, sort_dim="id_a", pos=(1, 23, 64, 18)),
        dict(type="table", ds="match_pair", title="Same person, different PANs: not merged",
             dims=[("id_a", "Record A"), ("name_a", "Name A"), ("id_b", "Record B"), ("name_b", "Name B")],
             measures=[("max([name_similarity])", "Name similarity"), ("sum([same_dob])", "Same DOB"),
                       ("sum([same_mobile])", "Same mobile")],
             filters=["[rule] = 'PAN_CONFLICT'"], may_be_empty=True, sort_dim="id_a", pos=(1, 41, 64, 18)),
        dict(type="table", ds="match_quality", title="Match quality per batch",
             dims=[("business_date", "Business date")],
             measures=[("sum([source_records])", "Source records"), ("sum([parties])", "Parties"),
                       ("sum([true_pairs])", "True pairs"), ("sum([predicted_pairs])", "Predicted pairs"),
                       ("sum([correct_pairs])", "Correct"), ("max([match_precision])", "Precision"),
                       ("max([match_recall])", "Recall"), ("sum([split_persons])", "Split persons"),
                       ("sum([merged_persons])", "Merged persons")],
             sort_dim="business_date", pos=(1, 59, 64, 16)),
    ]),
]

DASHBOARDS = [
    dict(title="GDL Banking KPIs MIS", pk=DASHBOARD_PK0, key="kpi-mis", sheets=KPI_SHEETS, main_ds="npa_trend",
         subtitle="NPA exposure, CASA ratio and customer relationship value from the certified KPI views"),
    dict(title="GDL Reconciliation & Data Quality", pk=DASHBOARD_PK0 + 1, key="reconciliation", sheets=RECON_SHEETS,
         main_ds="recon", subtitle="Reconciliation per layer and batch, KPI consistency, load audit and failed batches"),
    dict(title="GDL MDM & Golden Record", pk=DASHBOARD_PK0 + 2, key="mdm", sheets=MDM_SHEETS, main_ds="golden",
         subtitle="Golden records, match rules and decisions, review queue, match quality"),
]

SHELVES = {
    "kpi": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("compare_shelf", 1, 2), ("label_shelf", 1, 2),
            ("tooltip_shelf", 1, 2), ("x_shelf", 1, 1), ("y_shelf", 1, 1), ("filters_shelf", 2, 3)],
    "table": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-bars": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                     ("drill_shelf", 1, 1), ("label_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-lines": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                      ("filters_shelf", 2, 3)],
}


def visuals_of(d: dict):
    for sheet, items in d["sheets"]:
        for v in items:
            yield sheet, v


def uid(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def impala():
    import run_semantic as S

    return S.ImpalaEngine(json.loads((ROOT / "config" / "pipeline.json").read_text())["impala"])


def column_types(engine, ds_key: str) -> dict[str, str]:
    cols, rows = engine.query(f"DESCRIBE {DB}.{DATASETS[ds_key][1]}")
    return {r[cols.index("name")]: str(r[cols.index("type")]).upper() for r in rows}


def is_dim(ds_key: str, col: str, typ: str) -> bool:
    return col in DATASETS[ds_key][2] or not any(t in typ for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL"))


def dataset_record(key: str, pk: int, types: dict[str, str], conn_id: int, dashboards: list[int]) -> dict:
    name, view, _ = DATASETS[key]
    table = f"{DB}.{view}"
    cols = [{"alias": c, "type": t, "name": c, "isdim": is_dim(key, c, t)} for c, t in types.items()]
    return {"model": "datasets.dataset", "pk": pk, "fields": {
        "dataconnection": conn_id, "dataset_name": name, "dataset_type": "singletable", "dataset_detail": table,
        "dataset_description": f"{table} (sql/semantic)",
        "dataset_info": json.dumps([{"tablename": table, "columns": cols}]),
        "dataset_tablenames": json.dumps([table]), "uuid": uid("dataset", key), "imported_uuid": None,
        "cache_sequence": 0, "dataset_settings": "{}", "search_enabled": False, "dashboards": dashboards,
        "version_id": pk, "version_group_id": pk, "is_active_version": True,
        "version_name": "general-datalakehouse", "is_named_version": False}}


def dim_item(col: str, alias: str, typ: str) -> dict:
    return {"dataset_colname": col, "dataset_coltype": typ, "expression_for_trigger": f"[{col}]", "col_alias": alias}


def measure_item(expr: str, alias: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "expr_hasagg": True, "col_alias": alias,
            "dataset_colname": alias, "dataset_coltype": "DOUBLE"}


def filter_item(expr: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "filter_input": {}, "filter_data": [],
            "dataset_colname": "", "dataset_coltype": "STRING", "filter_column": ""}


def visual_record(v: dict, pk: int, sheet: str, types: dict[str, str], dataset_pk: int, dash: dict) -> dict:
    kind = v["type"]
    shelves = {name: [] for name, _, _ in SHELVES[kind]}
    sources = {}

    def add_dims(shelf, pairs):
        for col, alias in pairs:
            shelves[shelf].append(dim_item(col, alias, types[col]))
            sources[f"[{col}] as 'sub:{alias}'"] = shelf

    def add_measures(shelf, pairs):
        for expr, alias in pairs:
            shelves[shelf].append(measure_item(expr, alias))
            sources[f"{expr} as 'sub:{alias}'"] = shelf

    if kind in ("kpi", "table"):
        add_dims("dimensions_shelf", v.get("dims", []))
        add_measures("aggregates_shelf", v["measures"])
    else:
        add_dims("x_shelf", v["x"])
        add_measures("y_shelf", v["measures"])
        add_dims("color_shelf", v.get("color", []))
    for expr in v.get("filters", []):
        shelves["filters_shelf"].append(filter_item(expr))
        sources[expr] = "filters_shelf"
    if v.get("sort_desc"):
        shelves["y_shelf"][0]["order"] = {"priority": 1, "ascending": False}
    if v.get("sort_dim"):
        shelf = "dimensions_shelf" if kind == "table" else "x_shelf"
        item = next(i for i in shelves[shelf] if i["dataset_colname"] == v["sort_dim"])
        item["order"] = {"priority": 1, "ascending": v.get("sort_asc", True)}
    report = {
        "report_title": v["title"], "report_subtitle": "", "dashboard_id": dash["pk"],
        "limit": v.get("limit", 1000), "sample_pct": "Off", "selected_segments": [], "report_derived_data": [],
        "click_behaviors": {}, "sort_orders_asc": {}, "user_settings": {}, **shelves,
        "core": {"viz_type": kind, "saved_shelf_sources": sources,
                 "shelves": [{"name": n, "shelf_type": s, "column_type": c} for n, s, c in SHELVES[kind]]},
    }
    return {"model": "reports.report", "pk": pk, "fields": {
        "report_name": "", "report_description": f"{dash['title']} / {sheet}", "dataset": dataset_pk,
        "workspace": 1, "report_type": kind, "report_mode": "", "dashboard_url_name": "",
        "report_data": json.dumps({"report_data": report, "report_type": kind}), "shared_visual_dashboards": None,
        "parent_report": None, "uuid": uid("visual", dash["key"], sheet, v["title"]), "imported_uuid": None,
        "has_css_styles": False, "report_search_text": ""}}


def dashboard_record(d: dict, pk0: int, visuals: list[dict], ds_pk: dict[str, int], types: dict) -> dict:
    sheets, pk = [], pk0
    for order, (sheet, items) in enumerate(d["sheets"], 1):
        placed = []
        for v in items:
            pk += 1
            visuals.append(visual_record(v, pk, sheet, types[v["ds"]], ds_pk[v["ds"]], d))
            placed.append((pk, v["pos"]))
        sheets.append({"sheet_id": order, "order": order, "sheet_handle_title": sheet, "behaviors": {},
                       "visual_widgets": [{"col": c, "row": r, "size_x": w, "size_y": h, "id": f"uri-{i}-widget-{p}"}
                                          for i, (p, (c, r, w, h)) in enumerate(placed, 1)],
                       "control_widgets": []})
    dash = {"report_title": d["title"], "numColumns": 64, "report_subtitle": d["subtitle"],
            "dashboard_widgets": sheets[0]["visual_widgets"], "dashboard_sheets": sheets,
            "user_settings": {"dashboard_width": "1280", "display_filters": "true",
                              "permit_csv_download_dashboard": "true"},
            "global_control_widgets": [], "control_widgets": [], "click_behavior": {}}
    return {"model": "reports.report", "pk": d["pk"], "fields": {
        "report_name": d["title"], "report_description": "docs/DATAVIZ.md",
        "dataset": ds_pk[d["main_ds"]], "workspace": 1, "report_type": "dashboard", "report_mode": None,
        "dashboard_url_name": "", "report_data": json.dumps(dash), "shared_visual_dashboards": "[]",
        "parent_report": None, "uuid": uid("dashboard", d["key"]), "imported_uuid": None, "has_css_styles": False,
        "report_search_text": None}}


def missing_columns(types: dict[str, dict[str, str]]) -> list[str]:
    out = []
    for d in DASHBOARDS:
        for sheet, v in visuals_of(d):
            exprs = [e for e, _ in v["measures"]] + v.get("filters", [])
            cols = {c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])}
            cols |= {c for e in exprs for c in re.findall(r"\[(\w+)\]", e)}
            out += [f"{d['title']} / {sheet} / {v['title']}: {c}" for c in sorted(cols - set(types[v["ds"]]))]
    return out


def build(types: dict[str, dict[str, str]], conn_id: int, version: dict) -> dict:
    missing = missing_columns(types)
    if missing:
        raise SystemExit("columns not in the dataset's view:\n  " + "\n  ".join(missing))
    ds_pk = {k: DATASET_PK0 + i for i, k in enumerate(DATASETS)}
    visuals, dashboards, pk0 = [], [], VISUAL_PK0
    for d in DASHBOARDS:
        dashboards.append(dashboard_record(d, pk0, visuals, ds_pk, types))
        pk0 += 100
    used_by = {k: [d["pk"] for d in DASHBOARDS if any(v["ds"] == k for _, v in visuals_of(d))] for k in DATASETS}
    return {"segments": [], "staticasset": [], "dashboards": dashboards, "appgroupmembership": [],
            "reportannotation": [], "events": [], "customcss": [], "reportimage": [], "dateranges": [],
            "visuals": visuals, "colorpalette": [], "appgroups": [],
            "datasets": [dataset_record(k, ds_pk[k], types[k], conn_id, used_by[k]) for k in DATASETS],
            "version": version}


def _strip(e: str) -> str:
    return re.sub(r"\[(\w+)\]", r"\1", e)


def tile_sql(v: dict) -> str:
    """A KPI tile's number as plain Impala SQL on its view ([col] -> col)."""
    where = " AND ".join(_strip(f) for f in v.get("filters", [])) or "TRUE"
    return f"SELECT {_strip(v['measures'][0][0])} FROM {DB}.{DATASETS[v['ds']][1]} WHERE {where}"


def visual_sql(v: dict) -> str:
    """The query a visual sends, as plain Impala SQL."""
    dims = [c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])]
    where = " AND ".join(_strip(f) for f in v.get("filters", [])) or "TRUE"
    group = f" GROUP BY {', '.join(dims)}" if dims else ""
    return (f"SELECT {', '.join(dims + [_strip(e) for e, _ in v['measures']])} FROM {DB}.{DATASETS[v['ds']][1]} "
            f"WHERE {where}{group} LIMIT {v.get('limit', 1000)}")


def check(engine) -> int:
    """Every visual's query straight on Impala: the expressions parse and the visual has rows."""
    failed = 0
    for d in DASHBOARDS:
        for sheet, v in visuals_of(d):
            try:
                rows = engine.query(visual_sql(v))[1]
                ok, note = bool(rows) or v.get("may_be_empty", False), f"{len(rows)} rows"
                if rows and v["type"] == "kpi":
                    note = f"{rows[0][-1]}"
            except Exception as e:  # noqa: BLE001
                ok, note = False, str(e).strip().splitlines()[-1][:200]
            failed += not ok
            print(f"{'ok  ' if ok else 'FAIL'} {d['title']} / {sheet} / {v['title']}: {note}", flush=True)
    return failed


class DataViz:
    """The CDW Data Visualization instance, authenticated with a Data Visualization API key."""

    def __init__(self):
        import requests

        key = os.environ.get("GDL_VIZ_API_KEY")
        if not key:
            raise SystemExit("set GDL_VIZ_API_KEY (Data Visualization: Site Administration -> Manage API Keys), "
                             "or import dataviz/gdl_dashboards.json in the UI")
        self.url = json.loads((ROOT / "config" / "pipeline.json").read_text())["dataviz_url"].rstrip("/")
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"apikey {key}"

    def get(self, path: str, **params):
        r = self.s.get(self.url + path, params=params, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"GET {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def connections(self) -> list[dict]:
        return self.get("/arc/adminapi/v1/connections")

    def connection_id(self, name: str) -> int:
        found = next((c for c in self.connections() if c["name"] == name), None)
        if found is None:
            raise SystemExit(f"no connection {name!r}; --list-connections shows them")
        return found["id"]

    def version(self) -> dict:
        return self.get("/arc/migration/api/export/", dashboards="[]", filename="version", dry_run="False")["version"]

    def import_file(self, path: Path, connection: str) -> None:
        with path.open("rb") as f:
            r = self.s.post(self.url + "/arc/migration/api/import/", files={"import_file": f},
                            data={"dry_run": "False", "dataconnection_name": connection}, timeout=300)
        print(f"import: HTTP {r.status_code} {r.text[:500]}")
        if r.status_code != 200:
            raise SystemExit(1)

    def verify(self, engine) -> int:
        """Every visual's query through the Data API (Data Visualization -> its connection -> Impala);
        each KPI tile's number is also computed directly in Impala and compared."""
        ids = {d["name"]: d["id"] for d in self.get("/arc/adminapi/v1/datasets")}
        failed = 0
        for d in DASHBOARDS:
            for sheet, v in visuals_of(d):
                failed += not self.verify_visual(v, f"{d['title']} / {sheet}", ids, engine)
        return failed

    def verify_visual(self, v: dict, where: str, ids: dict, engine) -> bool:
        dims = v.get("dims", []) + v.get("x", []) + v.get("color", [])
        dsreq = {"version": 1, "type": "SQL", "limit": v.get("limit", 1000),
                 "dimensions": [{"type": "SIMPLE", "expr": f"[{c}] as '{a}'"} for c, a in dims],
                 "aggregates": [{"expr": f"{e} as '{a}'"} for e, a in v["measures"]],
                 "filters": v.get("filters", []), "dataset_id": ids[DATASETS[v["ds"]][0]]}
        r = self.s.post(self.url + "/arc/api/data", data={"version": 1, "dsreq": json.dumps(dsreq)}, timeout=300)
        rows = json.loads(r.json()["rows"]) if r.status_code == 200 else None
        ok = bool(rows) or (rows is not None and v.get("may_be_empty", False))
        note = f"{len(rows)} rows" if rows is not None else f"HTTP {r.status_code} {r.text[:200]}"
        if ok and v["type"] == "kpi":
            want = engine.query(tile_sql(v))[1][0][0]
            got = rows[0][-1] if isinstance(rows[0], list) else list(rows[0].values())[-1]
            ok = abs(float(got) - float(want)) < 1e-6
            note = f"{got} (Impala {want})"
        print(f"{'ok  ' if ok else 'FAIL'} {where} / {v['title']}: {note}")
        return ok


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--import", dest="do_import", action="store_true", help="import the file (needs GDL_VIZ_API_KEY)")
    p.add_argument("--connection", help="Data Visualization connection to the Impala warehouse (with --import)")
    p.add_argument("--verify", action="store_true", help="run every visual's query through the Data API")
    p.add_argument("--check", action="store_true", help="run every visual's query directly on Impala")
    p.add_argument("--list-connections", action="store_true")
    args = p.parse_args()
    if args.list_connections:
        for c in DataViz().connections():
            print(c["id"], c["name"], c.get("type"))
        return 0
    engine = impala()
    if args.check:
        return 1 if check(engine) else 0
    if args.verify:
        return 1 if DataViz().verify(engine) else 0
    viz = DataViz() if args.do_import else None
    if viz and not args.connection:
        p.error("--import needs --connection (see --list-connections)")
    types = {k: column_types(engine, k) for k in DATASETS}
    conn_id = viz.connection_id(args.connection) if viz else 1
    doc = build(types, conn_id, viz.version() if viz else DEFAULT_VERSION)
    OUT.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(doc['dashboards'])} dashboards, {len(doc['datasets'])} datasets, "
          f"{len(doc['visuals'])} visuals, {sum(len(d['sheets']) for d in DASHBOARDS)} sheets")
    if viz:
        viz.import_file(OUT, args.connection)
        print(f"open {viz.url}/arc/apps/ -> Dashboards -> " + " / ".join(d["title"] for d in DASHBOARDS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
