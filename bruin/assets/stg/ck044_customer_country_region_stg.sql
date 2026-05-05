/* @bruin
name: stg.ck044_customer_country_region_stg
type: pg.sql
depends:
  - stg.clean_check_results
  - stg.detect_new_records  
description: >
  CK044 — SAP_REF: Clienti STG: coppia COUNTRY(*)+REGION presente in T005S.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'stg.S_CUST_GEN#ZBP_DatiGenerali_STG'       AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                             AS object_key,
    'CK044'                                      AS check_id,
    CASE
        WHEN raw."REGION" IS NULL OR raw."REGION" = ''
            THEN 'REGION obbligatoria mancante (COUNTRY(*)=' || COALESCE(raw."COUNTRY(*)", 'NULL') || ')'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY(*)"
              AND ref."BLAND" = raw."REGION"
        )
            THEN 'Coppia paese/regione [' || raw."COUNTRY(*)" || '/' || raw."REGION" || '] non presente in SAP (T005S)'
        ELSE 'Coppia paese/regione [' || raw."COUNTRY(*)" || '/' || raw."REGION" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."REGION" IS NULL OR raw."REGION" = ''  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK044')
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY(*)"
              AND ref."BLAND" = raw."REGION"
        )                                               THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK044')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM stg."S_CUST_GEN#ZBP_DatiGenerali_STG" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK044'
)
;
