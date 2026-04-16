/* @bruin
name: stg.ck022_mbew_bklas
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK022 — SAP_REF: Materiali (S_MBEW): campo BKLAS(*) (Classe di valorizzazione)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_T025 (BKLAS).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MBEW'                                             AS source_table,
    'MAT'                                                AS category,
    raw."PRODUCT(k/*)" || '/' || raw."BWKEY(k/*)"       AS object_key,
    'CK022'                                              AS check_id,
    CASE
        WHEN raw."BKLAS(*)" IS NULL OR raw."BKLAS(*)" = ''
            THEN 'BKLAS(*) obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T025" ref
            WHERE ref."BKLAS" = raw."BKLAS(*)"
        )
            THEN 'Classe di valorizzazione [' || raw."BKLAS(*)" || '] non presente in SAP (SAP_EXPORT_T025.BKLAS)'
        ELSE 'Classe di valorizzazione [' || raw."BKLAS(*)" || '] valida'
    END                                                  AS message,
    CASE
        WHEN raw."BKLAS(*)" IS NULL OR raw."BKLAS(*)" = ''  THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T025" ref
            WHERE ref."BKLAS" = raw."BKLAS(*)"
        )                                                    THEN 'Error'
        ELSE 'Ok'
    END                                                  AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                   AS run_id,
    raw."_zip_source"                                    AS zip_source,
    NOW()                                                AS created_at
FROM raw."S_MBEW" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK022'
)
;
