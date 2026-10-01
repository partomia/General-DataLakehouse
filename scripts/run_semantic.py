"""
Semantic layer: certified KPI views, MIS views, regulatory datasets and the KPI consistency
check, from the SQL in sql/semantic/, sql/adhoc.sql and sql/time_travel.sql. One set of SQL
runs on two engines:

  --engine impala   CDW Impala (Hue's engine; Atlas records column lineage for the views and
                    the INSERT OVERWRITE loads). Needs GDL_IMPALA_USER and GDL_IMPALA_PASSWORD.
  --engine spark    local Spark + Iceberg (scripts/run_local.py semantic, and CI)

Steps, for each --dates entry in order:
  views        (re)create the KPI and MIS views and the regulatory tables (once per run)
  load         INSERT OVERWRITE the reporting date's partition of each regulatory dataset
  check        every consumer (MIS views, regulatory datasets, ad-hoc queries) must give the
               certified view's figure; results go to ref.recon_results (layer 'semantic')
  adhoc        run sql/adhoc.sql and print the answers
  time-travel  run sql/time_travel.sql, with the snapshot ids taken from ref.load_audit

SQL is written with the default database names (rsingh_gdl_*) so it pastes into Hue as is;
--db-prefix rewrites them. ${...} variables are filled per reporting date.

Usage:
  python scripts/run_semantic.py --engine spark --warehouse /tmp/w --dates 2026-09-21,2026-09-22
  python scripts/run_semantic.py --engine impala --steps views,load,check --dates 2026-09-25
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"
DEFAULT_PREFIX = "rsingh_gdl"
STEPS = ("views", "load", "check", "adhoc", "time-travel")
TOLERANCE = 0.005
TOP_N = 25


# ---------------------------------------------------------------- engines


class SparkEngine:
    name = "spark"
    now = "current_timestamp()"

    def __init__(self, spark):
        self.spark = spark

    def execute(self, sql: str) -> None:
        self.spark.sql(sql)

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        df = self.spark.sql(sql)
        return df.columns, [tuple(r) for r in df.collect()]

    def snapshot_id(self, table: str):
        rows = self.query(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")[1]
        return rows[0][0] if rows else None

    def dialect(self, sql: str) -> str:
        sql = re.sub(r"PARTITIONED BY SPEC \(([^)]*)\)\s*STORED AS ICEBERG", r"USING iceberg PARTITIONED BY (\1)", sql)
        return re.sub(r"DESCRIBE HISTORY\s+([\w.]+)",
                      r"SELECT made_current_at AS creation_time, snapshot_id, parent_id, is_current_ancestor "
                      r"FROM \1.history ORDER BY made_current_at", sql)


class ImpalaEngine:
    name = "impala"
    now = "utc_timestamp()"

    def __init__(self, cfg: dict):
        from impala.dbapi import connect

        user, password = os.environ.get("GDL_IMPALA_USER"), os.environ.get("GDL_IMPALA_PASSWORD")
        if not user or not password:
            raise SystemExit("set GDL_IMPALA_USER and GDL_IMPALA_PASSWORD (CDP workload user) for --engine impala")
        self.conn = connect(host=os.environ.get("GDL_IMPALA_HOST", cfg["host"]), port=int(cfg["port"]),
                            use_ssl=True, use_http_transport=True, http_path=cfg["http_path"],
                            auth_mechanism=cfg["auth_mechanism"], user=user, password=password)

    def execute(self, sql: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            cols = [d[0].split(".")[-1] for d in cur.description or []]
            return cols, [tuple(r) for r in cur.fetchall()] if cur.description else []
        finally:
            cur.close()

    def snapshot_id(self, table: str):
        cols, rows = self.query(f"DESCRIBE HISTORY {table}")
        current = [r for r in rows if str(r[cols.index("is_current_ancestor")]).lower() == "true"]
        return int(current[-1][cols.index("snapshot_id")]) if current else None

    def dialect(self, sql: str) -> str:
        return sql


# ---------------------------------------------------------------- SQL files


def statements(text: str) -> list[str]:
    """Split a SQL file into statements: comment lines dropped, ';' at a line end ends one."""
    out, cur = [], []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        cur.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(cur).strip().rstrip(";").strip())
            cur = []
    rest = "\n".join(cur).strip()
    return [s for s in out + ([rest] if rest else []) if s]


def named_blocks(path: Path) -> dict[str, str]:
    """'-- name: x' blocks of a file -> their single statement."""
    blocks, name, lines = {}, None, []
    for line in path.read_text().splitlines():
        m = re.match(r"--\s*name:\s*(\w+)", line)
        if m:
            if name:
                blocks[name] = statements("\n".join(lines))[0]
            name, lines = m.group(1), []
        elif name:
            lines.append(line)
    if name:
        blocks[name] = statements("\n".join(lines))[0]
    return blocks


class Renderer:
    def __init__(self, engine, prefix: str):
        self.engine, self.prefix = engine, prefix

    def __call__(self, sql: str, params: dict | None = None) -> str:
        if self.prefix != DEFAULT_PREFIX:
            sql = sql.replace(f"{DEFAULT_PREFIX}_", f"{self.prefix}_")
        for k, v in (params or {}).items():
            sql = sql.replace("${" + k + "}", str(v))
        left = sorted(set(re.findall(r"\$\{(\w+)\}", sql)))
        if left:
            raise ValueError(f"no value for {left}")
        return self.engine.dialect(sql)


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(float(v)) if isinstance(v, float) else str(v)
    return "'" + str(v).replace("\\", "").replace("'", "") + "'"


# ---------------------------------------------------------------- runner


class Semantic:
    def __init__(self, engine, prefix: str = DEFAULT_PREFIX, pipeline_run: str | None = None):
        self.e, self.prefix = engine, prefix
        self.r = Renderer(engine, prefix)
        self.run_id = f"semantic-{uuid.uuid4().hex[:12]}"
        self.pipeline_run = pipeline_run or self.run_id
        self.bank = json.loads((ROOT / "config" / "pipeline.json").read_text())["bank"]["name"]

    def t(self, layer: str, name: str) -> str:
        return f"{self.prefix}_{layer}.{name}"

    def scalar(self, sql: str, params: dict | None = None):
        rows = self.e.query(self.r(sql, params))[1]
        return rows[0][0] if rows and rows[0] else None

    def row(self, sql: str, params: dict | None = None) -> tuple:
        rows = self.e.query(self.r(sql, params))[1]
        return rows[0] if rows else ()

    # -------------------------------------------------- audit (plain SQL, so both engines write it)

    def audit(self, d: date, entity: str, status: str, rows_out=None, snapshot=None, message="") -> None:
        bid = "B" + d.strftime("%Y%m%d")
        self.e.execute(
            f"INSERT INTO {self.t('ref', 'load_audit')} VALUES ({lit(self.run_id)}, {lit(self.pipeline_run)}, "
            f"{lit(bid)}, DATE '{d.isoformat()}', 'semantic', {lit(entity)}, {lit(status)}, CAST(NULL AS BIGINT), "
            f"CAST({lit(rows_out)} AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), "
            f"CAST({lit(snapshot)} AS BIGINT), {self.e.now}, {self.e.now}, {lit(message)})")

    def transform(self, d: date, step: str, kind: str, sources: str, target: str, rows_out=None, snapshot=None,
                  details="") -> None:
        bid = "B" + d.strftime("%Y%m%d")
        self.e.execute(
            f"INSERT INTO {self.t('ref', 'transform_log')} VALUES ({lit(self.run_id)}, {lit(self.pipeline_run)}, "
            f"{lit(bid)}, DATE '{d.isoformat()}', 'run_semantic.{self.e.name}', {lit(step)}, {lit(kind)}, "
            f"{lit(sources)}, {lit(target)}, CAST(NULL AS BIGINT), CAST({lit(rows_out)} AS BIGINT), "
            f"CAST({lit(snapshot)} AS BIGINT), {self.e.now}, {lit(details)})")

    # -------------------------------------------------- steps

    def views(self) -> None:
        for f in sorted((SQL / "semantic").glob("*.sql")):
            if f.name.startswith("40_"):
                continue
            for s in statements(f.read_text()):
                self.e.execute(self.r(s))
            print(f"semantic: {f.name}", flush=True)

    def params(self, d: date) -> dict:
        return {"reporting_date": d.isoformat(), "batch_id": "B" + d.strftime("%Y%m%d"), "bank_name": self.bank}

    def load(self, d: date) -> None:
        gold = self.scalar(f"SELECT COUNT(*) FROM {self.t('gold', 'fact_deposit_balance_daily')} "
                           f"WHERE business_date = DATE '{d.isoformat()}'")
        if not gold:
            raise RuntimeError(f"no gold facts for {d}; run gold first")
        sources = {"reg_asset_classification": "semantic.kpi_npa_exposure,gold.dim_party,gold.dim_loan",
                   "reg_deposit_composition": "semantic.kpi_casa_ratio",
                   "rpt_customer_profitability": "semantic.kpi_customer_relationship_value,gold.dim_party"}
        for s in statements((SQL / "semantic" / "40_regulatory_load.sql").read_text()):
            target = re.search(r"INSERT OVERWRITE TABLE\s+\w+\.(\w+)", s).group(1)
            self.e.execute(self.r(s, self.params(d)))
            t = self.t("semantic", target)
            n = self.scalar(f"SELECT COUNT(*) FROM {t} WHERE reporting_date = DATE '{d.isoformat()}'")
            snap = self.e.snapshot_id(t)
            self.transform(d, f"load {target}", "consume", sources[target], t, n, snap,
                           "regulatory dataset for the reporting date, from the certified KPI view")
            self.audit(d, target, "COMMITTED", n, snap)
            print(f"{target}: {n} rows for {d}", flush=True)

    def previous_date(self, d: date):
        v = self.scalar(f"SELECT MAX(reporting_date) FROM {self.t('semantic', 'kpi_casa_ratio')} "
                        f"WHERE reporting_date < DATE '{d.isoformat()}'")
        return v if v is None or isinstance(v, date) else date.fromisoformat(str(v)[:10])

    def adhoc_total(self, name: str, columns: str, params: dict) -> tuple:
        q = named_blocks(SQL / "adhoc.sql")[name]
        q = re.sub(r"\s+ORDER BY[^()]*$", "", q, flags=re.S)
        return self.row(f"SELECT {columns} FROM ({q}) adhoc", params)

    def check(self, d: date) -> list[tuple]:
        p = self.params(d)
        prev = self.previous_date(d) or d
        on = f"WHERE reporting_date = DATE '{d.isoformat()}'"
        sem = lambda n: self.t("semantic", n)  # noqa: E731
        rows: list[tuple] = []

        def add(entity, check, expected, actual, detail, scale=1.0):
            e = None if expected is None else round(float(expected) * scale, 6)
            a = None if actual is None else round(float(actual) * scale, 6)
            diff = None if e is None or a is None else round(a - e, 6)
            status = "MATCHED" if diff is not None and abs(diff) <= TOLERANCE else "MISMATCH"
            rows.append((entity, check, e, a, diff, status, detail))

        # NPA exposure
        npa, adv, prov, npa_prov, npa_loans = self.row(
            f"SELECT SUM(gross_npa), SUM(gross_advance), SUM(provision_amount), SUM(npa_provision), "
            f"SUM(CASE WHEN is_npa THEN 1 ELSE 0 END) FROM {sem('kpi_npa_exposure')} {on}")
        ratio = float(npa) / float(adv)
        t = self.row(f"SELECT gross_npa, gross_npa_ratio, net_npa FROM {sem('mis_npa_trend')} {on}")
        add("NPA", "mis_npa_trend.gross_npa", npa, t[0], "MIS trend vs certified gross NPA")
        add("NPA", "mis_npa_trend.gross_npa_ratio_pct", ratio, t[1], "gross NPA ratio, percent", 100)
        add("NPA", "mis_npa_trend.net_npa", float(npa) - float(npa_prov), t[2], "gross NPA less NPA provisions")
        add("NPA", "mis_npa_breakdown.gross_npa", npa,
            self.scalar(f"SELECT SUM(gross_npa) FROM {sem('mis_npa_breakdown')} {on}"), "sum over branch x product x class")
        r = self.row(f"SELECT SUM(outstanding), SUM(CASE WHEN is_npa THEN outstanding ELSE 0 END), SUM(provision_amount) "
                     f"FROM {sem('reg_asset_classification')} {on}")
        add("NPA", "reg_asset_classification.outstanding", adv, r[0], "regulatory return vs certified gross advances")
        add("NPA", "reg_asset_classification.npa_outstanding", npa, r[1], "regulatory return vs certified gross NPA")
        add("NPA", "reg_asset_classification.provision", prov, r[2], "regulatory return vs certified provision")
        a = self.adhoc_total("npa_borrowers", "SUM(npa_outstanding), COUNT(*)", p)
        add("NPA", "adhoc.npa_borrowers.npa_outstanding", npa, a[0], "ad-hoc NPA borrower list vs certified gross NPA")
        add("NPA", "adhoc.npa_borrowers.loans", npa_loans, a[1], "one row per NPA loan")
        add("NPA", "gold.fact_loan_position_daily.provision", prov,
            self.scalar(f"SELECT SUM(provision_amount) FROM {self.t('gold', 'fact_loan_position_daily')} "
                        f"WHERE business_date = DATE '{d.isoformat()}'"),
            "provision from ref.kpi_parameter (certified) vs the rate gold applied")

        # CASA ratio
        casa, total = self.row(f"SELECT SUM(casa_balance), SUM(deposit_balance) FROM {sem('kpi_casa_ratio')} {on}")
        casa_ratio = float(casa) / float(total)
        t = self.row(f"SELECT casa_ratio, total_deposits FROM {sem('mis_casa_trend')} {on}")
        add("CASA", "mis_casa_trend.casa_ratio_pct", casa_ratio, t[0], "CASA ratio, percent", 100)
        add("CASA", "mis_casa_trend.total_deposits", total, t[1], "total deposits")
        b = self.row(f"SELECT SUM(casa_balance), SUM(total_deposits) FROM {sem('mis_casa_breakdown')} {on}")
        add("CASA", "mis_casa_breakdown.casa_ratio_pct", casa_ratio, float(b[0]) / float(b[1]),
            "ratio of the summed branch x segment rows, percent", 100)
        g = self.row(f"SELECT SUM(CASE WHEN is_casa THEN balance_inr ELSE 0 END), SUM(balance_inr) "
                     f"FROM {sem('reg_deposit_composition')} {on}")
        add("CASA", "reg_deposit_composition.casa_ratio_pct", casa_ratio, float(g[0]) / float(g[1]),
            "regulatory deposit composition, percent", 100)
        add("CASA", "reg_deposit_composition.total_deposits", total, g[1], "regulatory deposit composition")
        a = self.adhoc_total("casa_branch_change", "SUM(casa_balance), SUM(total_deposits)",
                             {**p, "previous_date": prev.isoformat()})
        add("CASA", "adhoc.casa_branch_change.casa_ratio_pct", casa_ratio, float(a[0]) / float(a[1]),
            f"ad-hoc branch comparison with {prev}, percent", 100)

        # customer relationship value
        crv, parties = self.row(f"SELECT SUM(crv), COUNT(*) FROM {sem('kpi_customer_relationship_value')} {on}")
        top = self.row(f"SELECT party_id, crv FROM {sem('kpi_customer_relationship_value')} {on} "
                       f"ORDER BY crv DESC, party_id LIMIT 1")
        top_n = self.scalar(f"SELECT SUM(crv) FROM (SELECT crv FROM {sem('kpi_customer_relationship_value')} {on} "
                            f"ORDER BY crv DESC, party_id LIMIT {TOP_N}) t")
        add("CRV", "kpi_crv_component.annual_value", crv,
            self.scalar(f"SELECT SUM(annual_value) FROM {sem('kpi_crv_component')} {on}"),
            "finest-grain components vs the party-level certified view")
        s = self.row(f"SELECT SUM(crv_total), SUM(parties) FROM {sem('mis_crv_segment')} {on}")
        add("CRV", "mis_crv_segment.crv_total", crv, s[0], "sum over segment x branch x value band")
        add("CRV", "mis_crv_segment.parties", parties, s[1], "parties")
        add("CRV", "mis_crv_top_relationships.crv", top_n,
            self.scalar(f"SELECT SUM(crv) FROM {sem('mis_crv_top_relationships')} {on}"), f"top {TOP_N} relationships")
        r = self.row(f"SELECT SUM(crv), COUNT(*) FROM {sem('rpt_customer_profitability')} {on}")
        add("CRV", "rpt_customer_profitability.crv", crv, r[0], "profitability extract vs certified total")
        add("CRV", "rpt_customer_profitability.parties", parties, r[1], "parties")
        a = self.adhoc_total("party_value_by_product", "SUM(annual_value)", {**p, "party_id": top[0]})
        add("CRV", "adhoc.party_value_by_product.crv", top[1], a[0], f"product breakdown of the top party {top[0]}")

        self.write_results(d, rows)
        bad = [x for x in rows if x[5] != "MATCHED"]
        print(f"kpi consistency {p['batch_id']}: {len(rows) - len(bad)} MATCHED, {len(bad)} MISMATCH "
              f"(gross NPA {float(npa):,.2f} = {ratio:.2%} of advances; CASA {casa_ratio:.2%}; "
              f"CRV {float(crv):,.2f} over {parties} parties)", flush=True)
        for x in bad:
            print(f"  MISMATCH {x[0]} {x[1]}: expected {x[2]}, got {x[3]} ({x[6]})", flush=True)
        self.transform(d, "kpi consistency", "validate", "semantic.kpi_*,semantic.mis_*,semantic.reg_*,sql/adhoc.sql",
                       self.t("ref", "recon_results"), len(rows), None,
                       f"{len(rows) - len(bad)} MATCHED, {len(bad)} MISMATCH")
        return bad

    def write_results(self, d: date, rows: list[tuple]) -> None:
        """Replace the date's 'semantic' rows in ref.recon_results in one commit (no DELETE, which
        Impala only allows on merge-on-read tables): overwrite the partition with its other rows
        plus the new ones."""
        t, bid = self.t("ref", "recon_results"), "B" + d.strftime("%Y%m%d")
        new = " UNION ALL ".join(
            f"SELECT {lit(self.run_id)}, {lit(bid)}, DATE '{d.isoformat()}', 'semantic', {lit(e)}, {lit(c)}, "
            f"CAST({lit(x)} AS DOUBLE), CAST({lit(a)} AS DOUBLE), CAST({lit(df)} AS DOUBLE), {lit(s)}, {lit(det)}, "
            f"{self.e.now}" for e, c, x, a, df, s, det in rows)
        self.e.execute(f"INSERT OVERWRITE TABLE {t} SELECT * FROM {t} WHERE business_date = DATE '{d.isoformat()}' "
                       f"AND layer <> 'semantic' UNION ALL {new}")

    def adhoc(self, d: date) -> None:
        prev = self.previous_date(d) or d
        top = self.scalar(f"SELECT party_id FROM {self.t('semantic', 'kpi_customer_relationship_value')} "
                          f"WHERE reporting_date = DATE '{d.isoformat()}' ORDER BY crv DESC, party_id LIMIT 1")
        params = {**self.params(d), "previous_date": prev.isoformat(), "party_id": top}
        for name, q in named_blocks(SQL / "adhoc.sql").items():
            show(name, *self.e.query(self.r(q, params)))

    def time_travel_params(self, d: date) -> dict:
        audit = self.t("ref", "load_audit")
        party = self.scalar(
            f"SELECT party_id FROM {self.t('gold', 'dim_party')} WHERE version > 1 AND effective_from <= DATE "
            f"'{d.isoformat()}' AND src_record_ids LIKE '%\"address\":\"doc:%' ORDER BY effective_from DESC, party_id LIMIT 1")
        changed = self.scalar(f"SELECT MAX(effective_from) FROM {self.t('gold', 'dim_party')} "
                              f"WHERE party_id = '{party}' AND version > 1")
        changed = changed if isinstance(changed, date) else date.fromisoformat(str(changed)[:10])
        snap = lambda op, b: self.row(  # noqa: E731
            f"SELECT snapshot_after, CAST(ended_at AS STRING) FROM {audit} WHERE stage = 'mdm' AND entity = "
            f"'golden_party' AND status = 'COMMITTED' AND batch_id {op} '{b}' ORDER BY batch_id DESC, ended_at DESC LIMIT 1")
        after = snap("=", "B" + changed.strftime("%Y%m%d"))
        before = snap("<", "B" + changed.strftime("%Y%m%d"))
        return {"entity": "golden_party", "party_id": party, "snapshot_after": after[0], "snapshot_before": before[0],
                "as_of_time": after[1][:19], "reg_snapshot": self.e.snapshot_id(self.t("semantic", "reg_deposit_composition"))}

    def time_travel(self, d: date) -> None:
        params = self.time_travel_params(d)
        print(f"time travel: party {params['party_id']}, golden_party snapshot {params['snapshot_before']} -> "
              f"{params['snapshot_after']}", flush=True)
        for name, q in named_blocks(SQL / "time_travel.sql").items():
            show(name, *self.e.query(self.r(q, params)))


def show(name: str, cols: list[str], rows: list[tuple], limit: int = 12) -> None:
    print(f"\n-- {name}: {len(rows)} row(s)")
    if not rows:
        return
    cells = [[("" if v is None else str(v))[:40] for v in r] for r in rows[:limit]]
    widths = [max(len(c), *(len(r[i]) for r in cells)) for i, c in enumerate(cols)]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    for r in cells:
        print("  " + " | ".join(v.ljust(w) for v, w in zip(r, widths)))
    if len(rows) > limit:
        print(f"  ... {len(rows) - limit} more")


def run(engine, prefix: str, dates: list[date], steps=("views", "load", "check"), pipeline_run=None,
        fail_on_mismatch: bool = False) -> int:
    sem = Semantic(engine, prefix, pipeline_run)
    if "views" in steps:
        sem.views()
    bad = 0
    for d in dates:
        if "load" in steps:
            sem.load(d)
        if "check" in steps:
            bad += len(sem.check(d))
    if "adhoc" in steps:
        sem.adhoc(dates[-1])
    if "time-travel" in steps:
        sem.time_travel(dates[-1])
    if bad and fail_on_mismatch:
        raise RuntimeError(f"{bad} KPI consistency mismatch(es)")
    return bad


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--engine", choices=("spark", "impala"), default="spark")
    p.add_argument("--dates", default=None, help="comma-separated reporting dates (default: the pipeline's)")
    p.add_argument("--steps", default="views,load,check")
    p.add_argument("--db-prefix", default=DEFAULT_PREFIX)
    p.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse"), help="local Iceberg warehouse (spark)")
    p.add_argument("--pipeline-run", default=None)
    p.add_argument("--fail-on-mismatch", action="store_true")
    args = p.parse_args(argv)
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        p.error(f"unknown steps {unknown}; choose from {', '.join(STEPS)}")
    cfg = json.loads((ROOT / "config" / "pipeline.json").read_text())
    dates = [date.fromisoformat(x) for x in (args.dates.split(",") if args.dates else cfg["business_dates"])]
    if args.engine == "spark":
        sys.path.insert(0, str(ROOT / "scripts"))
        from run_local import local_spark

        engine = SparkEngine(local_spark(Path(args.warehouse)))
    else:
        engine = ImpalaEngine(cfg["impala"])
    run(engine, args.db_prefix, dates, steps, args.pipeline_run, args.fail_on_mismatch)
    return 0


if __name__ == "__main__":
    sys.exit(main())
