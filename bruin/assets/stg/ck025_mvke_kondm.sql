/* @bruin
name: stg.ck025_mvke_kondm
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK025 — SAP_REF: Materiali (S_MVKE): campo KONDM (Gruppo prezzi)
  se valorizzato, deve essere presente in SAP_EXPORT_T178 (KONDM).
  Se il campo è vuoto o NULL, il record viene ignorato.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MVKE'                                                                         AS source_table,
    'MAT'                                                                            AS category,
    raw."PRODUCT(k/*)" || '/' || raw."VKORG(k/*)" || '/' || raw."VTWEG(k/*)"       AS object_key,
    'CK025'                                                                          AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T178" ref
            WHERE ref."KONDM" = raw."KONDM"
        )
            THEN 'Gruppo prezzi [' || raw."KONDM" || '] non presente in SAP (SAP_EXPORT_T178.KONDM)'
        ELSE 'Gruppo prezzi [' || raw."KONDM" || '] valido'
    END                                                                              AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T178" ref
            WHERE ref."KONDM" = raw."KONDM"
        )                                                                            THEN 'Error'
        ELSE 'Ok'
    END                                                                              AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                                               AS run_id,
    raw."_source"                                                                AS zip_source,
    NOW()                                                                            AS created_at
FROM raw."S_MVKE" raw
WHERE
    raw."KONDM" IS NOT NULL AND raw."KONDM" <> ''
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK025'
    )
;
