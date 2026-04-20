/* @bruin
name: stg.ck027_marc_sobsl
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK027 — SAP_REF: Materiali (S_MARC): campo SOBSL (Tipo approvvigionamento speciale)
  se valorizzato, deve essere presente in SAP_EXPORT_T460A (SOBSL).
  Se il campo è vuoto o NULL, il record viene ignorato.
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
    'CK027'                                              AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T460A" ref
            WHERE ref."SOBSL" = raw."SOBSL"
        )
            THEN 'Tipo approvvigionamento speciale [' || raw."SOBSL" || '] non presente in SAP (SAP_EXPORT_T460A.SOBSL)'
        ELSE 'Tipo approvvigionamento speciale [' || raw."SOBSL" || '] valido'
    END                                                  AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T460A" ref
            WHERE ref."SOBSL" = raw."SOBSL"
        )                                               THEN 'Error'
        ELSE 'Ok'
    END                                                  AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                   AS run_id,
    raw."_zip_source"                                    AS zip_source,
    NOW()                                                AS created_at
FROM raw."S_MARC" raw
WHERE
    raw."SOBSL" IS NOT NULL AND raw."SOBSL" <> ''
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK027'
    )
;
