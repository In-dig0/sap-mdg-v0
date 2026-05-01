/* @bruin
name: stg.ck041_supplier_country_stg
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK041 — SAP_REF: Fornitori STG: codice paese (COUNTRY) valorizzato
  e presente nella tabella di riferimento SAP T005S (LAND1).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'stg.S_SUPPL_GEN#ZBP_DatiGenerali_STG'      AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CK041'                                      AS check_id,
    CASE
        WHEN raw."COUNTRY" IS NULL OR raw."COUNTRY" = ''
            THEN 'COUNTRY obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
        )
            THEN 'Codice paese [' || raw."COUNTRY" || '] non presente in SAP (T005S.LAND1)'
        ELSE 'Codice paese [' || raw."COUNTRY" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."COUNTRY" IS NULL OR raw."COUNTRY" = ''   THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
        )                                                   THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM stg."S_SUPPL_GEN#ZBP_DatiGenerali_STG" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK041'
)
;
