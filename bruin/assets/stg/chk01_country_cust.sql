/* @bruin
name: stg.chk01_country_cust
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK01 — Clienti: verifica che il campo COUNTRY della tabella
  S_CUST_GEN#ZBP_DatiGenerali sia valorizzato e presente
  nella tabella di controllo SAP ref.EXPORT_T005S (colonna LAND1).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_GEN#ZBP_DatiGenerali'               AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                             AS object_key,
    'CHK01'                                      AS check_id,
    CASE
        WHEN raw."COUNTRY" IS NULL OR raw."COUNTRY" = ''
            THEN 'COUNTRY obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
        )
            THEN 'Codice paese [' || raw."COUNTRY" || '] non presente in SAP (T005S.LAND1)'
        ELSE
            'Codice paese [' || raw."COUNTRY" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."COUNTRY" IS NULL OR raw."COUNTRY" = ''
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
        )
            THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_GEN#ZBP_DatiGenerali" raw
;
