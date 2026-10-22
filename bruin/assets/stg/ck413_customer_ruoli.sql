/* @bruin
name: stg.ck413_customer_ruoli
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK413 — CROSS_TABLE: Clienti (flusso ZBP-Clienti): ogni KUNNR(k/*)
  di S_CUST_GEN#ZBP-DatiGenerali deve avere almeno un ruolo BP_ROLE(k/*) in
  S_ROLES#ZBP-RuoliClienti a parità di KUNNR(k/*). Un esito per cliente.
  La direzione opposta (ruoli senza dati generali) è coperta da CK404.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_GEN#ZBP-DatiGenerali'                AS source_table,
    'BP'                                         AS category,
    g."KUNNR(k/*)"                               AS object_key,
    'CK413'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_ROLES#ZBP-RuoliClienti" r
            WHERE r."KUNNR(k/*)" = g."KUNNR(k/*)"
        )
            THEN 'Cliente [' || g."KUNNR(k/*)" || '] senza ruolo in S_ROLES#ZBP-RuoliClienti'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_ROLES#ZBP-RuoliClienti" r
            WHERE r."KUNNR(k/*)" = g."KUNNR(k/*)"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK413')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    g."_source"                                  AS zip_source,
    NOW()                                        AS created_at
FROM (
    SELECT DISTINCT "KUNNR(k/*)", "_source"
    FROM raw."S_CUST_GEN#ZBP-DatiGenerali"
    WHERE COALESCE(TRIM("KUNNR(k/*)"), '') <> ''
) g
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK413'
)
;
