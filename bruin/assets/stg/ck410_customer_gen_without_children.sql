/* @bruin
name: stg.ck410_customer_gen_without_children
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK410 — CROSS_TABLE: Clienti: ogni KUNNR(k/*) presente in
  S_CUST_GEN#ZBP-DatiGenerali deve avere anche i dati societari
  (S_CUST_COMPANY#ZBP-DatiSocieta) e i dati area vendite
  (S_CUST_SALES_DATA#ZBP-DatiVendite). Un esito per cliente e per tabella figlia.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    figlio.source_table                             AS source_table,
    'BP'                                            AS category,
    gen."KUNNR(k/*)"                                AS object_key,
    'CK410'                                         AS check_id,
    CASE
        WHEN NOT figlio.presente
            THEN 'Cliente [' || gen."KUNNR(k/*)" || '] presente in S_CUST_GEN#ZBP-DatiGenerali '
                 || 'ma assente in ' || figlio.source_table
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN NOT figlio.presente
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK410')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    gen."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_CUST_GEN#ZBP-DatiGenerali" gen
CROSS JOIN LATERAL (
    VALUES
        ('S_CUST_COMPANY#ZBP-DatiSocieta',
         EXISTS (SELECT 1 FROM raw."S_CUST_COMPANY#ZBP-DatiSocieta" c
                 WHERE c."KUNNR(k/*)" = gen."KUNNR(k/*)")),
        ('S_CUST_SALES_DATA#ZBP-DatiVendite',
         EXISTS (SELECT 1 FROM raw."S_CUST_SALES_DATA#ZBP-DatiVendite" s
                 WHERE s."KUNNR(k/*)" = gen."KUNNR(k/*)"))
) AS figlio(source_table, presente)
WHERE gen."KUNNR(k/*)" IS NOT NULL
  AND gen."KUNNR(k/*)" <> ''
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK410'
)
;
