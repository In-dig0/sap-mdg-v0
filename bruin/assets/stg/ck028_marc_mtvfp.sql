/* @bruin
name: stg.ck028_marc_mtvfp
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK028 — SAP_REF: Materiali (S_MARC): campo MTVFP (Tipo fabbisogno)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_TMVF (MTVFP).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARC'                                             AS source_table,
    'MAT'                                                AS category,
    raw."PRODUCT(k/*)" || '/' || raw."WERKS(k/*)"       AS object_key,
    'CK028'                                              AS check_id,
    CASE
        WHEN raw."MTVFP" IS NULL OR raw."MTVFP" = ''
            THEN 'MTVFP obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TMVF" ref
            WHERE ref."MTVFP" = raw."MTVFP"
        )
            THEN 'Tipo fabbisogno [' || raw."MTVFP" || '] non presente in SAP (SAP_EXPORT_TMVF.MTVFP)'
        ELSE 'Tipo fabbisogno [' || raw."MTVFP" || '] valido'
    END                                                  AS message,
    CASE
        WHEN raw."MTVFP" IS NULL OR raw."MTVFP" = ''   THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TMVF" ref
            WHERE ref."MTVFP" = raw."MTVFP"
        )                                               THEN 'Error'
        ELSE 'Ok'
    END                                                  AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                   AS run_id,
    raw."_source"                                    AS zip_source,
    NOW()                                                AS created_at
FROM raw."S_MARC" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK028'
)
;
