/* @bruin
name: stg.ck409_supplier_gen_without_children
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK409 — CROSS_TABLE: Fornitori: ogni LIFNR(k/*) presente in
  S_SUPPL_GEN#ZBP_DatiGenerali deve avere anche i dati societari
  (S_SUPPL_COMPANY#ZBP_DatiSocieta) e i dati di organizzazione acquisti
  (S_SUPPL_PURCHASING#ZBP_OrganAcq). Un esito per fornitore e per tabella figlia.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    figlio.source_table                             AS source_table,
    'BP'                                            AS category,
    gen."LIFNR(k/*)"                                AS object_key,
    'CK409'                                         AS check_id,
    CASE
        WHEN NOT figlio.presente
            THEN 'Fornitore [' || gen."LIFNR(k/*)" || '] presente in S_SUPPL_GEN#ZBP_DatiGenerali '
                 || 'ma assente in ' || figlio.source_table
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN NOT figlio.presente
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK409')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    gen."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" gen
CROSS JOIN LATERAL (
    VALUES
        ('S_SUPPL_COMPANY#ZBP_DatiSocieta',
         EXISTS (SELECT 1 FROM raw."S_SUPPL_COMPANY#ZBP_DatiSocieta" c
                 WHERE c."LIFNR(k/*)" = gen."LIFNR(k/*)")),
        ('S_SUPPL_PURCHASING#ZBP_OrganAcq',
         EXISTS (SELECT 1 FROM raw."S_SUPPL_PURCHASING#ZBP_OrganAcq" p
                 WHERE p."LIFNR(k/*)" = gen."LIFNR(k/*)"))
) AS figlio(source_table, presente)
WHERE gen."LIFNR(k/*)" IS NOT NULL
  AND gen."LIFNR(k/*)" <> ''
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK409'
)
;
