/* @bruin
name: stg.ck023_mara_mstae
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK023 — SAP_REF: Materiali (S_MARA): campo MSTAE (Stato materiale)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_T141 (MMSTA).
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
    'CK023'                                      AS check_id,
    CASE
        WHEN raw."MSTAE" IS NULL OR raw."MSTAE" = ''
            THEN 'MSTAE obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T141" ref
            WHERE ref."MMSTA" = raw."MSTAE"
        )
            THEN 'Stato materiale [' || raw."MSTAE" || '] non presente in SAP (SAP_EXPORT_T141.MMSTA)'
        ELSE 'Stato materiale [' || raw."MSTAE" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."MSTAE" IS NULL OR raw."MSTAE" = ''  THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T141" ref
            WHERE ref."MMSTA" = raw."MSTAE"
        )                                              THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_MARA" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK023'
)
;
