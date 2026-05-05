/* @bruin
name: stg.ck022_mbew_bklas
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK022 — SAP_REF: Materiali (S_MBEW): campo BKLAS(*) (Classe di valorizzazione)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_T025 (BKLAS).
  Il messaggio include il WERKS ricavato da S_MARC a parità di PRODUCT(k/*).
  In caso di più plant per lo stesso articolo, viene preso il primo in ordine alfabetico.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MBEW'                                             AS source_table,
    'MAT'                                                AS category,
    raw."PRODUCT(k/*)"                                   AS object_key,
    'CK022'                                              AS check_id,
    CASE
        WHEN raw."BKLAS(*)" IS NULL OR raw."BKLAS(*)" = ''
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] BKLAS(*) obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T025" ref
            WHERE ref."BKLAS" = raw."BKLAS(*)"
        )
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Classe di valorizzazione [' || raw."BKLAS(*)" || '] non presente in SAP (SAP_EXPORT_T025.BKLAS)'
        ELSE '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Classe di valorizzazione [' || raw."BKLAS(*)" || '] valida'
    END                                                  AS message,
    CASE
        WHEN raw."BKLAS(*)" IS NULL OR raw."BKLAS(*)" = ''  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK022')
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T025" ref
            WHERE ref."BKLAS" = raw."BKLAS(*)"
        )                                                    THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK022')
        ELSE 'Ok'
    END                                                  AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                   AS run_id,
    raw."_source"                                        AS zip_source,
    NOW()                                                AS created_at
FROM raw."S_MBEW" raw
LEFT JOIN LATERAL (
    SELECT "WERKS(k/*)"
    FROM raw."S_MARC"
    WHERE "PRODUCT(k/*)" = raw."PRODUCT(k/*)"
    ORDER BY "WERKS(k/*)"
    LIMIT 1
) marc ON TRUE
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK022'
)
;
