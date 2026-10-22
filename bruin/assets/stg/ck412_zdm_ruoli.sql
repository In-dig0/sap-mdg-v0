/* @bruin
name: stg.ck412_zdm_ruoli
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK412 — CROSS_TABLE: Destinatari merci (flusso ZDM-Clienti): ogni KUNNR(k/*)
  di S_CUST_GEN#ZDM-DatiGenerali deve avere almeno un ruolo BP_ROLE(k/*) in
  S_ROLES#ZBP-RuoliDM a parità di KUNNR(k/*). Un esito per codice DM.
  La direzione opposta (ruoli senza dati generali) è coperta da CK403.
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
    'CK412'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_ROLES#ZBP-RuoliDM" r
            WHERE r."KUNNR(k/*)" = g."KUNNR(k/*)"
        )
            THEN 'Destinatario merci [' || g."KUNNR(k/*)" || '] senza ruolo in S_ROLES#ZBP-RuoliDM'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_ROLES#ZBP-RuoliDM" r
            WHERE r."KUNNR(k/*)" = g."KUNNR(k/*)"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK412')
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
    FROM stg.check_catalog WHERE check_id = 'CK412'
)
;
