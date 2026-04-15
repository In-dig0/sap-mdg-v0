/* @bruin
name: stg.ck014_mara_extwg
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK014 — SAP_REF: Materiali (S_MARA): campo EXTWG (Gruppo merci esterno)
  se valorizzato, deve essere presente in SAP_EXPORT_TWEW (EXTWG).
  Se il campo è vuoto o NULL, il record viene ignorato.
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
    'CK014'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TWEW" ref
            WHERE ref."EXTWG" = raw."EXTWG"
        )
            THEN 'Gruppo merci esterno [' || raw."EXTWG" || '] non presente in SAP (SAP_EXPORT_TWEW.EXTWG)'
        ELSE 'Gruppo merci esterno [' || raw."EXTWG" || '] valido'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TWEW" ref
            WHERE ref."EXTWG" = raw."EXTWG"
        )                                        THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_MARA" raw
WHERE
    -- Processa solo se EXTWG è valorizzato
    raw."EXTWG" IS NOT NULL AND raw."EXTWG" <> ''
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK014'
    )
;
