/* @bruin
name: stg.ck016_mara_mtart
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK016 — SAP_REF: Materiali (S_MARA): campo MTART(*) (Tipo materiale)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_T134 (MTART).
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
    'CK016'                                      AS check_id,
    CASE
        WHEN raw."MTART(*)" IS NULL OR raw."MTART(*)" = ''
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] MTART(*) obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T134" ref
            WHERE ref."MTART" = raw."MTART(*)"
        )
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Tipo materiale [' || raw."MTART(*)" || '] non presente in SAP (SAP_EXPORT_T134.MTART)'
        ELSE '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Tipo materiale [' || raw."MTART(*)" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."MTART(*)" IS NULL OR raw."MTART(*)" = ''  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK016')
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T134" ref
            WHERE ref."MTART" = raw."MTART(*)"
        )                                                    THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK016')
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
    FROM stg.check_catalog WHERE check_id = 'CK016'
)
;
