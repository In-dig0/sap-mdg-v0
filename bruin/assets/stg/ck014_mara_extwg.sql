/* @bruin
name: stg.ck014_mara_extwg
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK014 — SAP_REF: Materiali (S_MARA): campo EXTWG (Gruppo merci esterno)
  se valorizzato, deve essere presente in SAP_EXPORT_TWEW (EXTWG).
  Se il campo è vuoto o NULL, il record viene ignorato.
  Il messaggio include il WERKS ricavato da S_MARC a parità di PRODUCT(k/*).
  In caso di più plant per lo stesso articolo, viene preso il primo in ordine alfabetico.
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
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Gruppo merci esterno [' || raw."EXTWG" || '] non presente in SAP (SAP_EXPORT_TWEW.EXTWG)'
        ELSE '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Gruppo merci esterno [' || raw."EXTWG" || '] valido'
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
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_MARA" raw
LEFT JOIN LATERAL (
    SELECT "WERKS(k/*)"
    FROM raw."S_MARC"
    WHERE "PRODUCT(k/*)" = raw."PRODUCT(k/*)"
    ORDER BY "WERKS(k/*)"
    LIMIT 1
) marc ON TRUE
WHERE
    -- Processa solo se EXTWG è valorizzato
    raw."EXTWG" IS NOT NULL AND raw."EXTWG" <> ''
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK014'
    )
;
