/* @bruin
name: stg.chk02_supplier_country_region
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK02 — Verifica che la coppia COUNTRY+REGION della tabella
  S_SUPPL_GEN#ZBP_DatiGenerali esista in ref.EXPORT_T005S.
  COUNTRY e REGION sono entrambi obbligatori per SAP.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'              AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CHK02_SUPPL'                                      AS check_id,
    CASE
        WHEN raw."REGION" IS NULL OR raw."REGION" = ''
            THEN 'REGION obbligatoria mancante (COUNTRY=' || COALESCE(raw."COUNTRY", 'NULL') || ')'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
              AND ref."BLAND" = raw."REGION"
        )
            THEN 'Coppia paese/regione [' || raw."COUNTRY" || '/' || raw."REGION" || '] non presente in SAP (T005S)'
        ELSE
            'Coppia paese/regione [' || raw."COUNTRY" || '/' || raw."REGION" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."REGION" IS NULL OR raw."REGION" = ''
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
              AND ref."BLAND" = raw."REGION"
        )
            THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog
    WHERE check_id = 'CHK02_SUPPL'
)
;