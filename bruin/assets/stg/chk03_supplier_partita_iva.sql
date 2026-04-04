/* @bruin
name: stg.chk03_supplier_partita_iva
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK03 — Partita IVA mancante per soggetti UE/ExtraUE.
  Verifica che ogni BP abbia almeno un TAXNUM(*) valorizzato
  in S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'              AS source_table,
    'BP'                                         AS category,
    gen."LIFNR(k/*)"                             AS object_key,
    'CHK03_SUPPL'                                      AS check_id,
    CASE
        WHEN tax."LIFNR(k/*)" IS NULL
            THEN 'Nessun codice fiscale presente in ZBP_CodiciFisc'
        ELSE
            'Almeno un codice fiscale valorizzato presente'
    END                                          AS message,
    CASE
        WHEN tax."LIFNR(k/*)" IS NULL
            THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    gen."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" gen
LEFT JOIN (
    SELECT DISTINCT "LIFNR(k/*)"
    FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"
    WHERE "TAXNUM(*)" IS NOT NULL
      AND "TAXNUM(*)" <> ''
) tax ON tax."LIFNR(k/*)" = gen."LIFNR(k/*)"
;
