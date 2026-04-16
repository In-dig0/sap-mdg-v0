/* @bruin
name: stg.ck021_marc_dispo
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK021 — SAP_REF: Materiali (S_MARC): la coppia WERKS+DISPO
  deve essere presente nella tabella SAP_EXPORT_T024D (WERKS+DISPO).
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
    'CK021'                                              AS check_id,
    CASE
        WHEN raw."DISPO" IS NULL OR raw."DISPO" = ''
            THEN 'DISPO obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T024D" ref
            WHERE ref."WERKS" = raw."WERKS(k/*)"
              AND ref."DISPO" = raw."DISPO"
        )
            THEN 'Coppia WERKS/DISPO [' || raw."WERKS(k/*)" || '/' || raw."DISPO" || '] non presente in SAP (SAP_EXPORT_T024D)'
        ELSE 'Coppia WERKS/DISPO [' || raw."WERKS(k/*)" || '/' || raw."DISPO" || '] valida'
    END                                                  AS message,
    CASE
        WHEN raw."DISPO" IS NULL OR raw."DISPO" = ''    THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T024D" ref
            WHERE ref."WERKS" = raw."WERKS(k/*)"
              AND ref."DISPO" = raw."DISPO"
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
    FROM stg.check_catalog WHERE check_id = 'CK021'
)
;
