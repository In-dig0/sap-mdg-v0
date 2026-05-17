/* @bruin
name: stg.ck049_supplier_withholding_tax
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK049 — SAP_REF: Fornitori: coppia WITHT+WT_WITHCD presente in T059Z.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_WITH_TAX#ZBP_RitenAcco'            AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CK049'                                      AS check_id,
    CASE
        WHEN raw."WITHT(k/*)" IS NULL OR raw."WITHT(k/*)" = ''
            THEN 'WITHT obbligatorio mancante'
        WHEN raw."WT_WITHCD" IS NULL OR raw."WT_WITHCD" = ''
            THEN 'WT_WITHCD obbligatorio mancante (WITHT=' || raw."WITHT(k/*)" || ')'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T059Z" ref
            WHERE ref."WITHT"     = raw."WITHT(k/*)"
              AND ref."WT_WITHCD" = raw."WT_WITHCD"
        )
            THEN 'Coppia ritenuta [' || raw."WITHT(k/*)" || '/' || raw."WT_WITHCD" || '] non presente in SAP (T059Z)'
        ELSE 'Coppia ritenuta [' || raw."WITHT(k/*)" || '/' || raw."WT_WITHCD" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."WITHT(k/*)" IS NULL OR raw."WITHT(k/*)" = ''  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK049')
        WHEN raw."WT_WITHCD"  IS NULL OR raw."WT_WITHCD"  = ''  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK049')
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T059Z" ref
            WHERE ref."WITHT"     = raw."WITHT(k/*)"
              AND ref."WT_WITHCD" = raw."WT_WITHCD"
        )                                                        THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK049')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_WITH_TAX#ZBP_RitenAcco" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK049'
)
;
