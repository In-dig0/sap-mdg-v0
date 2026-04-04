/* @bruin
name: stg.chk06_customer_contacts_orphans
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK06_CUST — CROSS_TABLE: verifica che ogni KUNNR presente in
  S_CUST_CONT#ZBP-AddInterlocutore esista anche in
  S_CUST_GEN#ZBP-DatiGenerali.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_CONT#ZBP-AddInterlocutore'          AS source_table,
    'BP'                                         AS category,
    cont."KUNNR(k/*)"                            AS object_key,
    'CHK06_CUST'                                 AS check_id,
    'KUNNR [' || cont."KUNNR(k/*)" || '] presente in ZBP_AddInterlocutore '
    || 'ma assente in ZBP_DatiGenerali'          AS message,
    'Error'                                      AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    cont."_zip_source"                           AS zip_source,
    NOW()                                        AS created_at
FROM (
    SELECT DISTINCT "KUNNR(k/*)", "_zip_source"
    FROM raw."S_CUST_CONT#ZBP-AddInterlocutore"
) cont
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_CUST_GEN#ZBP-DatiGenerali" gen
    WHERE gen."KUNNR(k/*)" = cont."KUNNR(k/*)"
)
;
