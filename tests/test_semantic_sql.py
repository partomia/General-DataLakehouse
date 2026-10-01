"""Static checks on the semantic SQL: it parses into statements, stays inside the dialect both
engines share, and consumers read only the certified KPI views."""

import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import run_semantic as S  # noqa: E402

SQL_FILES = sorted((ROOT / "sql" / "semantic").glob("*.sql")) + [ROOT / "sql" / "adhoc.sql",
                                                                  ROOT / "sql" / "time_travel.sql"]
CONSUMERS = [ROOT / "sql" / "semantic" / "20_mis_views.sql", ROOT / "sql" / "semantic" / "40_regulatory_load.sql",
             ROOT / "sql" / "adhoc.sql"]
NOT_PORTABLE = [r"\bCREATE OR REPLACE\b", r"\bIF\s*\(", r"\bEXCEPT\b", r"\bMINUS\b", r"`", r"\bNVL2?\s*\(",
                r"\barray_contains\b", r"\bdate_add\s*\(", r"\bLATERAL VIEW\b", r"\bQUALIFY\b"]


class FakeEngine:
    name = "fake"

    def dialect(self, sql):
        return sql


def code(path: Path) -> str:
    return "\n".join(line for line in path.read_text().splitlines() if not line.strip().startswith("--"))


def test_statements_split_on_line_end_semicolons():
    text = "-- a comment; not a statement\nSELECT 1;\nSELECT 'x;y'\nFROM t;\n"
    assert S.statements(text) == ["SELECT 1", "SELECT 'x;y'\nFROM t"]


def test_named_blocks():
    assert set(S.named_blocks(ROOT / "sql" / "adhoc.sql")) == {"npa_borrowers", "casa_branch_change",
                                                               "party_value_by_product"}
    tt = S.named_blocks(ROOT / "sql" / "time_travel.sql")
    assert {"golden_record_as_of_batch", "snapshot_diff", "table_history", "scd2_versus_time_travel"} <= set(tt)


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_sql_is_portable(path):
    text = code(path)
    for pattern in NOT_PORTABLE:
        assert not re.search(pattern, text, re.I), f"{path.name}: {pattern} is not in the Impala/Spark common dialect"


@pytest.mark.parametrize("path", CONSUMERS, ids=lambda p: p.name)
def test_consumers_read_only_certified_views(path):
    text = code(path)
    assert not re.search(r"\bfact_\w+", text), f"{path.name} reads a gold fact instead of a certified KPI view"
    assert re.search(r"rsingh_gdl_semantic\.kpi_\w+", text)


def test_every_certified_view_is_defined():
    kpis = json.loads((ROOT / "config" / "kpi.json").read_text())["kpis"]
    defined = set(re.findall(r"CREATE VIEW rsingh_gdl_semantic\.(\w+)",
                             "\n".join(code(p) for p in (ROOT / "sql" / "semantic").glob("*.sql"))))
    assert {k["certified_view"] for k in kpis} <= defined


def test_render_prefix_params_and_spark_dialect():
    spark = S.SparkEngine.__new__(S.SparkEngine)
    r = S.Renderer(spark, "demo")
    ddl = "CREATE TABLE rsingh_gdl_semantic.x (d DATE)\nPARTITIONED BY SPEC (d)\nSTORED AS ICEBERG\nTBLPROPERTIES ('a' = 'b')"
    assert r(ddl) == "CREATE TABLE demo_semantic.x (d DATE)\nUSING iceberg PARTITIONED BY (d)\nTBLPROPERTIES ('a' = 'b')"
    assert "FROM demo_mdm.golden_party.history" in r("DESCRIBE HISTORY rsingh_gdl_mdm.golden_party")
    assert r("SELECT DATE '${reporting_date}'", {"reporting_date": "2026-09-21"}) == "SELECT DATE '2026-09-21'"
    with pytest.raises(ValueError, match="party_id"):
        S.Renderer(FakeEngine(), S.DEFAULT_PREFIX)("SELECT '${party_id}'")


def test_every_regulatory_load_has_a_table():
    tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS rsingh_gdl_semantic\.(\w+)",
                            code(ROOT / "sql" / "semantic" / "30_regulatory_tables.sql")))
    loads = set(re.findall(r"INSERT OVERWRITE TABLE rsingh_gdl_semantic\.(\w+)",
                           code(ROOT / "sql" / "semantic" / "40_regulatory_load.sql")))
    assert loads == tables == {"reg_asset_classification", "reg_deposit_composition", "rpt_customer_profitability"}


def test_literals_cannot_break_out_of_quotes():
    assert S.lit("it's") == "'its'"
    assert S.lit(None) == "NULL" and S.lit(3) == "3" and S.lit(0.5) == "0.5"
