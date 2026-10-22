/* @bruin
name: stg.ck411_zdm_partner_we
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK411 — CROSS_TABLE: Destinatari merci (flusso ZDM-Clienti): ogni KUNNR(k/*)
  di S_CUST_GEN#ZDM-DatiGenerali deve avere in S_CUST_SALES_PARTNER#ZDM-Partner
  almeno un record con funzione partner PARVW(k/*) = 'WE' (destinatario merci)
  a parità di KUNNR(k/*). Un esito per codice DM.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_GEN#ZDM-DatiGenerali'                AS source_table,
    'BP'                                         AS category,
    g."KUNNR(k/*)"                               AS object_key,
    'CK411'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_SALES_PARTNER#ZDM-Partner" p
            WHERE p."KUNNR(k/*)" = g."KUNNR(k/*)"
              AND p."PARVW(k/*)" = 'WE'
        )
            THEN 'Destinatario merci [' || g."KUNNR(k/*)" || '] senza partner WE in S_CUST_SALES_PARTNER#ZDM-Partner'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_SALES_PARTNER#ZDM-Partner" p
            WHERE p."KUNNR(k/*)" = g."KUNNR(k/*)"
              AND p."PARVW(k/*)" = 'WE'
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK411')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    g."_source"                                  AS zip_source,
    NOW()                                        AS created_at
FROM (
    SELECT DISTINCT "KUNNR(k/*)", "_source"
    FROM raw."S_CUST_GEN#ZDM-DatiGenerali"
    WHERE COALESCE(TRIM("KUNNR(k/*)"), '') <> ''
) g
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK411'
)
;
