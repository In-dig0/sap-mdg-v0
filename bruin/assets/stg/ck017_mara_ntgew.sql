/* @bruin
name: stg.ck017_mara_ntgew
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK017 — EXISTENCE: Materiali (S_MARA): campo NTGEW (Peso netto)
  obbligatorio e diverso da zero.
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
    'CK017'                                      AS check_id,
    CASE
        WHEN raw."NTGEW" IS NULL OR raw."NTGEW" = ''
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] NTGEW obbligatorio mancante'
        WHEN REPLACE(raw."NTGEW", ',', '.')::NUMERIC = 0
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] NTGEW non può essere zero'
        ELSE '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Peso netto [' || raw."NTGEW" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."NTGEW" IS NULL OR raw."NTGEW" = ''        THEN 'Error'
        WHEN REPLACE(raw."NTGEW", ',', '.')::NUMERIC = 0   THEN 'Error'
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
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK017'
)
;
