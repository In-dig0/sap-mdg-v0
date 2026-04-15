/* @bruin
name: stg.ck015_mara_matkl
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK015 — SAP_REF: Materiali (S_MARA): campo MATKL (Gruppo merci)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_T023 (MATKL).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARA'                                     AS source_table,
    'MAT'                                        AS category,
    raw."PRODUCT(k/*)"                           AS object_key,
    'CK015'                                      AS check_id,
    CASE
        WHEN raw."MATKL" IS NULL OR raw."MATKL" = ''
            THEN 'MATKL obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T023" ref
            WHERE ref."MATKL" = raw."MATKL"
        )
            THEN 'Gruppo merci [' || raw."MATKL" || '] non presente in SAP (SAP_EXPORT_T023.MATKL)'
        ELSE 'Gruppo merci [' || raw."MATKL" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."MATKL" IS NULL OR raw."MATKL" = ''  THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T023" ref
            WHERE ref."MATKL" = raw."MATKL"
        )                                              THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_MARA" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK015'
)
;
