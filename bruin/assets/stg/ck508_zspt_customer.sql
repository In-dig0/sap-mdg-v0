/* @bruin
name: stg.ck508_zspt_customer
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK508 — CROSS_SOURCE: Pricing vendite (FA-ZSPT-ScontoTestataCliente):
  il campo Customer deve essere presente in almeno una delle tabelle clienti
  raw.S_CUST_GEN#ZBP-DatiGenerali (KUNNR(k/*)) oppure
  raw.S_CUST_GEN#ZDM-DatiGenerali (KUNNR(k/*)).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'FA-ZSPT-ScontoTestataCliente'               AS source_table,
    'MAT'                                        AS category,
    raw."Customer"                               AS object_key,
    'CK508'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZBP-DatiGenerali" zbp
            WHERE zbp."KUNNR(k/*)" = raw."Customer"
        )
        AND NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZDM-DatiGenerali" zdm
            WHERE zdm."KUNNR(k/*)" = raw."Customer"
        )
            THEN 'Cliente [' || raw."Customer" || '] non presente in S_CUST_GEN#ZBP-DatiGenerali né in S_CUST_GEN#ZDM-DatiGenerali'
        ELSE 'Cliente [' || raw."Customer" || '] valido'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZBP-DatiGenerali" zbp
            WHERE zbp."KUNNR(k/*)" = raw."Customer"
        )
        AND NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZDM-DatiGenerali" zdm
            WHERE zdm."KUNNR(k/*)" = raw."Customer"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK508')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."FA-ZSPT-ScontoTestataCliente" raw
WHERE
    raw."Customer" IS NOT NULL AND raw."Customer" <> ''
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK508'
    )
;
