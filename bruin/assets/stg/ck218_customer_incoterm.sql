/* @bruin
name: stg.ck218_customer_incoterm
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK218 — EXISTENCE: Clienti (DatiVendite): gli Incoterm INCO1 e INCO2 devono essere
  valorizzati (le stringhe di soli spazi valgono come vuote). Nell'output per SAP
  prd.merge_dati_vendite imposta i default INCO1 = 'EXW' e INCO2 = '.'.
  Un esito per cliente.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_SALES_DATA#ZBP-DatiVendite'          AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                                  AS object_key,
    'CK218'                                      AS check_id,
    CASE
        WHEN m.manca_inco1 AND m.manca_inco2
            THEN 'Incoterm mancanti: INCO1 e INCO2 vuoti (in output INCO1 = EXW, INCO2 = .)'
        WHEN m.manca_inco1
            THEN 'Incoterm mancante: INCO1 vuoto (in output INCO1 = EXW)'
        WHEN m.manca_inco2
            THEN 'Incoterm mancante: INCO2 vuoto (in output INCO2 = .)'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN m.manca_inco1 OR m.manca_inco2
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK218')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_SALES_DATA#ZBP-DatiVendite" raw
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(raw."INCO1"), '') = '' AS manca_inco1,
           COALESCE(TRIM(raw."INCO2"), '') = '' AS manca_inco2
) m
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK218'
)
;
