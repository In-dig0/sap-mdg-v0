/* @bruin
name: stg.ck026_marc_beskz
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK026 — SAP_REF: Materiali (S_MARC): campo BESKZ (Tipo approvvigionamento)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_T460A (BESKZ).
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
    'CK026'                                              AS check_id,
    CASE
        WHEN raw."BESKZ" IS NULL OR raw."BESKZ" = ''
            THEN 'BESKZ obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T460A" ref
            WHERE ref."BESKZ" = raw."BESKZ"
        )
            THEN 'Tipo approvvigionamento [' || raw."BESKZ" || '] non presente in SAP (SAP_EXPORT_T460A.BESKZ)'
        ELSE 'Tipo approvvigionamento [' || raw."BESKZ" || '] valido'
    END                                                  AS message,
    CASE
        WHEN raw."BESKZ" IS NULL OR raw."BESKZ" = ''    THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T460A" ref
            WHERE ref."BESKZ" = raw."BESKZ"
        )                                               THEN 'Error'
        ELSE 'Ok'
    END                                                  AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                   AS run_id,
    raw."_zip_source"                                    AS zip_source,
    NOW()                                                AS created_at
FROM raw."S_MARC" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK026'
)
;
