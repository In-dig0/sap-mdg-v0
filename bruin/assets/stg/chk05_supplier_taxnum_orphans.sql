/* @bruin
name: stg.chk05_supplier_taxnum_orphans
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK05_SUPPL — CROSS_TABLE: verifica che ogni LIFNR presente in
  S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc esista anche in
  S_SUPPL_GEN#ZBP_DatiGenerali. Record orfani = dati inconsistenti
  che causerebbero errori di caricamento in SAP.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc'         AS source_table,
    'BP'                                         AS category,
    tax."LIFNR(k/*)"                             AS object_key,
    'CHK05_SUPPL'                                AS check_id,
    'LIFNR [' || tax."LIFNR(k/*)" || '] presente in ZBP_CodiciFisc '
    || 'ma assente in ZBP_DatiGenerali'          AS message,
    'Error'                                      AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    tax."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM (
    SELECT DISTINCT "LIFNR(k/*)", "_zip_source"
    FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"
) tax
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" gen
    WHERE gen."LIFNR(k/*)" = tax."LIFNR(k/*)"
)
;
