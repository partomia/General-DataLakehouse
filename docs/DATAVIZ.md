# Dashboards in Cloudera Data Visualization

Four dashboards on the existing CDW Data Visualization instance
(<https://viz-indianbank-spend-analytics.dw-federal-cdp-env.dp5i-5vkq.cloudera.site/arc/apps/>),
over its `federal-impala-1` connection to the CDW Impala warehouse. They are code:
`dataviz/build_dashboard.py` declares datasets, visuals and sheets and writes one export file,
`dataviz/gdl_dashboards.json`, with fixed UUIDs and primary keys (dashboards 9000-9003, datasets
from 9100, visuals from 9200), so importing it again updates the dashboards in place. Nothing
else on the instance is touched.

| Dashboard | Sheets | Datasets (`rsingh_gdl_semantic`) |
|---|---|---|
| GDL Banking KPIs MIS | NPA exposure, CASA ratio, Customer relationship value, Trends | `mis_npa_trend`, `mis_npa_breakdown`, `mis_casa_trend`, `mis_casa_breakdown`, `mis_crv_segment`, `mis_crv_top_relationships` |
| GDL Reconciliation & Data Quality | Latest batch, By business date, Load audit | `dash_recon`, `dash_load_audit` |
| GDL MDM & Golden Record | Golden records, Matching | `dash_golden_party`, `dash_party_xref`, `dash_match_pair`, `dash_match_quality` |
| GDL AML Alerts | Alerts, New alerts by date | `dash_aml_alert` |

The MIS datasets only aggregate the certified KPI views (`sql/semantic/20_mis_views.sql`), so a
dashboard figure is the certified figure; the KPI consistency check proves it every batch, and
its result is the "KPI consistency" table on the Reconciliation dashboard. Every dataset has
`is_latest = 1` on the latest business date, which the KPI tiles filter on.

Personal columns shown on a dashboard (names on the top relationships, the golden records and the
alert queue) carry the project's PII classifications, so federal01 and federal07 see them masked
here too ([GOVERNANCE.md](GOVERNANCE.md)).

## Build, import, verify

```bash
export GDL_IMPALA_USER=... GDL_IMPALA_PASSWORD=...   # CDP workload user, for column types and --check
export GDL_VIZ_API_KEY=...                           # Data Visualization: Site Administration -> Manage API Keys

python dataviz/build_dashboard.py --check            # every visual's query directly on Impala
python dataviz/build_dashboard.py --list-connections
python dataviz/build_dashboard.py --import --connection federal-impala-1
python dataviz/build_dashboard.py --verify           # every visual through Data Visualization's data API
```

`--import` rebuilds the file with the instance's own export version and the connection's id
before importing. `--verify` runs each visual's query through Data Visualization (so through its
connection to Impala) and compares every KPI tile with the same figure computed directly in
Impala. The instance signs users in with SAML, so the API needs the key; without one, import
`dataviz/gdl_dashboards.json` in the UI (Data, then Import Visual Artifacts) and pick the
`federal-impala-1` connection.

Column types come from `DESCRIBE` on Impala, and the build fails if a visual names a column its
view does not have. Re-run the build and import after a view's columns change.
