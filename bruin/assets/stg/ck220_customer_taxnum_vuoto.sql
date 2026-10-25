/* @bruin
name: stg.ck220_customer_taxnum_vuoto
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK220 — EXISTENCE: Clienti (ZBP-CodiciFisc): ogni codice fiscale TAXTYPE(k/*) deve avere
  l'identificativo TAXNUM(*) valorizzato (le stringhe di soli spazi valgono come vuote).
  Le righe senza identificativo sono escluse dallo ZIP clienti (in SAP danno errore)
  e scritte negli scarti. Un esito per riga (object_key = KUNNR, tipo nel messaggio).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_TAXNUMBERS#ZBP-CodiciFisc'           AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                                  AS object_key,
    'CK220'                                      AS check_id,
    CASE
        WHEN COALESCE(TRIM(raw."TAXNUM(*)"), '') = ''
            THEN 'Codice fiscale [' || raw."TAXTYPE(k/*)" || '] senza identificativo (TAXNUM vuoto): riga esclusa dallo ZIP clienti'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN COALESCE(TRIM(raw."TAXNUM(*)"), '') = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK220')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_TAXNUMBERS#ZBP-CodiciFisc" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK220'
)
;
