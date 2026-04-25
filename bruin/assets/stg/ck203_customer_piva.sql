/* @bruin
name: stg.ck203_customer_piva
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK203 — EXISTENCE: Clienti: partita IVA mancante.
  Verifica che ogni KUNNR abbia almeno un TAXNUM(*) valorizzato
  in S_CUST_TAXNUMBERS#ZBP-CodiciFisc.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_GEN#ZBP-DatiGenerali'               AS source_table,
    'BP'                                         AS category,
    gen."KUNNR(k/*)"                             AS object_key,
    'CK203'                                      AS check_id,
    CASE
        WHEN tax."KUNNR(k/*)" IS NULL
            THEN 'Nessun codice fiscale presente in ZBP_CodiciFisc'
        ELSE 'Almeno un codice fiscale valorizzato presente'
    END                                          AS message,
    CASE
        WHEN tax."KUNNR(k/*)" IS NULL THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    gen."_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_GEN#ZBP-DatiGenerali" gen
LEFT JOIN (
    SELECT DISTINCT "KUNNR(k/*)"
    FROM raw."S_CUST_TAXNUMBERS#ZBP-CodiciFisc"
    WHERE "TAXNUM(*)" IS NOT NULL AND "TAXNUM(*)" <> ''
) tax ON tax."KUNNR(k/*)" = gen."KUNNR(k/*)"
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK203'
)
;
