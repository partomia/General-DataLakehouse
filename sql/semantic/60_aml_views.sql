-- The AML extension's consumer: the dataset of the "GDL AML Alerts" dashboard in Data
-- Visualization (dataviz/build_dashboard.py), and the alert queue an analyst works from in Hue.
-- gold.fact_aml_alert joined to its rule and to the conformed party and branch dimensions;
-- 0/1 flags a visual can sum, and is_latest = 1 on the latest business date.

DROP VIEW IF EXISTS rsingh_gdl_semantic.dash_aml_alert;

CREATE VIEW rsingh_gdl_semantic.dash_aml_alert
COMMENT 'Dashboard: AML alerts per business date (gold.fact_aml_alert) with the rule, party and branch'
AS
SELECT a.business_date, a.alert_id, a.rule_code, r.rule_name, a.severity, a.subject_type,
       a.party_id, p.full_name, p.segment, a.account_key, a.branch_code, b.branch_name, b.region,
       a.window_from, a.window_to, a.txn_count, a.amount_inr, a.entry_id, a.list_name, a.match_basis,
       a.evidence,
       1 AS alerts,
       CASE WHEN a.is_new THEN 1 ELSE 0 END AS is_new,
       CASE WHEN a.severity = 'CRITICAL' THEN 1 ELSE 0 END AS is_critical,
       CASE WHEN a.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM rsingh_gdl_gold.fact_aml_alert a
JOIN rsingh_gdl_gold.dim_aml_rule r ON r.rule_code = a.rule_code
LEFT JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = a.party_sk
LEFT JOIN rsingh_gdl_gold.dim_branch b ON b.branch_code = a.branch_code
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_gdl_gold.fact_aml_alert) l;
