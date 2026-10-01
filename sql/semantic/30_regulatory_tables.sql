-- Regulatory / external datasets: Iceberg tables with one partition per reporting date,
-- written by 40_regulatory_load.sql. They are tables, not views, because a submitted return
-- must not change when the data behind it is restated; Iceberg keeps every submitted version.
-- Written in Impala DDL; scripts/run_semantic.py rewrites the storage clause for Spark.

CREATE TABLE IF NOT EXISTS rsingh_gdl_semantic.reg_asset_classification (
    reporting_date    DATE,
    bank_name         STRING,
    branch_ifsc       STRING,
    borrower_id       STRING,
    borrower_name     STRING,
    borrower_pan      STRING,
    facility_id       STRING,
    product_name      STRING,
    sanction_amount   DECIMAL(18,2),
    outstanding       DECIMAL(18,2),
    overdue           DECIMAL(18,2),
    dpd               INT,
    asset_class       STRING,
    is_npa            BOOLEAN,
    npa_since         DATE,
    provision_rate    DOUBLE,
    provision_amount  DECIMAL(18,2),
    batch_id          STRING
)
PARTITIONED BY SPEC (reporting_date)
STORED AS ICEBERG
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS rsingh_gdl_semantic.reg_deposit_composition (
    reporting_date    DATE,
    bank_name         STRING,
    branch_ifsc       STRING,
    region            STRING,
    deposit_type      STRING,
    is_casa           BOOLEAN,
    size_band         STRING,
    accounts          BIGINT,
    balance_inr       DECIMAL(18,2),
    batch_id          STRING
)
PARTITIONED BY SPEC (reporting_date)
STORED AS ICEBERG
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS rsingh_gdl_semantic.rpt_customer_profitability (
    reporting_date    DATE,
    party_id          STRING,
    customer_name     STRING,
    segment           STRING,
    home_branch       STRING,
    casa_value        DECIMAL(18,2),
    td_value          DECIMAL(18,2),
    lending_value     DECIMAL(18,2),
    fee_value         DECIMAL(18,2),
    crv               DECIMAL(18,2),
    crv_rank          INT,
    batch_id          STRING
)
PARTITIONED BY SPEC (reporting_date)
STORED AS ICEBERG
TBLPROPERTIES ('format-version' = '2');
