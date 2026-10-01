-- Datasets of the "Reconciliation & Data Quality" and "MDM & Golden Record" dashboards in
-- Data Visualization (dataviz/build_dashboard.py). Flat views over ref and mdm: labels, 0/1
-- flags a visual can sum, and is_latest = 1 on the latest business date. No KPI figure is
-- computed here; the KPI dashboard reads the MIS views (20_mis_views.sql).

DROP VIEW IF EXISTS rsingh_gdl_semantic.dash_recon;

CREATE VIEW rsingh_gdl_semantic.dash_recon
COMMENT 'Dashboard: every reconciliation and KPI consistency check per business date (ref.recon_results)'
AS
SELECT r.business_date, r.batch_id, r.layer,
       CASE r.layer WHEN 'bronze' THEN '1 bronze' WHEN 'silver' THEN '2 silver' WHEN 'gold' THEN '3 gold'
                    WHEN 'semantic' THEN '4 semantic (KPI consumers)' ELSE r.layer END AS layer_label,
       r.entity, r.check_name, r.expected, r.actual, r.difference, r.status, r.detail,
       CASE WHEN r.status = 'MATCHED' THEN 1 ELSE 0 END   AS is_matched,
       CASE WHEN r.status = 'EXPLAINED' THEN 1 ELSE 0 END AS is_explained,
       CASE WHEN r.status = 'MISMATCH' THEN 1 ELSE 0 END  AS is_mismatch,
       CASE WHEN r.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM rsingh_gdl_ref.recon_results r
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_gdl_ref.recon_results) l;

DROP VIEW IF EXISTS rsingh_gdl_semantic.dash_load_audit;

CREATE VIEW rsingh_gdl_semantic.dash_load_audit
COMMENT 'Dashboard: every stage and entity commit or failure per batch (ref.load_audit); is_final marks the last attempt'
AS
SELECT a.business_date, a.batch_id, a.pipeline_run, a.stage,
       CASE a.stage WHEN 'bronze' THEN '1 bronze' WHEN 'silver' THEN '2 silver' WHEN 'mdm' THEN '3 mdm'
                    WHEN 'gold' THEN '4 gold' WHEN 'reconcile' THEN '5 reconcile'
                    WHEN 'semantic' THEN '6 semantic' ELSE a.stage END AS stage_label,
       a.entity, a.status, a.rows_in, a.rows_out, a.rows_rejected,
       a.logged_at, a.message,
       CASE WHEN a.attempt_desc = 1 THEN 1 ELSE 0 END AS is_final,
       CASE WHEN a.status = 'FAILED' THEN 1 ELSE 0 END AS failed,
       CASE WHEN a.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM (
    SELECT business_date, batch_id, pipeline_run, stage, entity, status, rows_in, rows_out, rows_rejected,
           ended_at AS logged_at, message,
           ROW_NUMBER() OVER (PARTITION BY batch_id, stage, entity ORDER BY ended_at DESC) AS attempt_desc
    FROM rsingh_gdl_ref.load_audit
    WHERE status <> 'STARTED'
) a
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_gdl_ref.load_audit) l;

DROP VIEW IF EXISTS rsingh_gdl_semantic.dash_match_quality;

CREATE VIEW rsingh_gdl_semantic.dash_match_quality
COMMENT 'Dashboard: entity resolution measured against the generator truth per batch (mdm.match_quality)'
AS
SELECT q.business_date, q.batch_id, q.source_records, q.parties, q.true_pairs, q.predicted_pairs,
       q.correct_pairs, q.precision AS match_precision, q.recall AS match_recall,
       q.split_persons, q.merged_persons,
       CASE WHEN q.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM rsingh_gdl_mdm.match_quality q
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_gdl_mdm.match_quality) l;

DROP VIEW IF EXISTS rsingh_gdl_semantic.dash_match_pair;

CREATE VIEW rsingh_gdl_semantic.dash_match_pair
COMMENT 'Dashboard: every candidate pair the matcher scored, with its rule and decision (mdm.match_pair)'
AS
SELECT batch_id, rule, decision, score, name_similarity,
       CASE WHEN same_pan THEN 1 ELSE 0 END     AS same_pan,
       CASE WHEN same_mobile THEN 1 ELSE 0 END  AS same_mobile,
       CASE WHEN same_dob THEN 1 ELSE 0 END     AS same_dob,
       CASE WHEN same_pincode THEN 1 ELSE 0 END AS same_pincode,
       CASE WHEN decision = 'REVIEW' THEN 1 ELSE 0 END AS for_review,
       id_a, id_b, name_a, name_b, party_a, party_b
FROM rsingh_gdl_mdm.match_pair;

DROP VIEW IF EXISTS rsingh_gdl_semantic.dash_party_xref;

CREATE VIEW rsingh_gdl_semantic.dash_party_xref
COMMENT 'Dashboard: every source record and the golden party it resolved to (mdm.party_xref)'
AS
SELECT src_system, src_key, party_id, match_rule, match_confidence, cluster_size,
       CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
       CASE WHEN previous_party_id IS NOT NULL AND previous_party_id <> party_id THEN 1 ELSE 0 END AS moved_party,
       first_batch_id, batch_id
FROM rsingh_gdl_mdm.party_xref;

DROP VIEW IF EXISTS rsingh_gdl_semantic.dash_golden_party;

CREATE VIEW rsingh_gdl_semantic.dash_golden_party
COMMENT 'Dashboard: the golden records as of the latest batch (mdm.golden_party), without the source id lists'
AS
SELECT party_id, full_name, segment, home_branch, kyc_status, address_city, member_records,
       CASE WHEN has_pan_conflict THEN 1 ELSE 0 END AS has_pan_conflict,
       CASE WHEN member_records > 1 THEN 1 ELSE 0 END AS is_merged,
       batch_id, business_date
FROM rsingh_gdl_mdm.golden_party;
