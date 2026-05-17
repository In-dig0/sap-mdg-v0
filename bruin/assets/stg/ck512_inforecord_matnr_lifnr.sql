/* @bruin
name: stg.ck512_inforecord_matnr_lifnr
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK512 — CROSS_SOURCE: Inforecord acquisti:
  - MATNR deve essere presente in S_MARA come PRODUCT(k/*)
  - LIFNR(*) deve essere presente in S_SUPPL_GEN#ZBP_DatiGenerali
    oppure in S_SUPPL_GEN#AddDatiGenerali come LIFNR(k/*)
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_EINA#INFORMATFOR'                            AS source_table,
    'MAT'                                           AS category,
    raw."INFNR(k/*)"                                AS object_key,
    'CK512'                                         AS check_id,
    CASE
        -- MATNR mancante o non in S_MARA
        WHEN raw."MATNR" IS NULL OR raw."MATNR" = ''
            THEN 'MATNR obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARA" mara
            WHERE mara."PRODUCT(k/*)" = raw."MATNR"
        )
            THEN 'MATNR [' || raw."MATNR" || '] non presente in S_MARA'
        -- LIFNR mancante o non in nessuna delle due master fornitori
        WHEN raw."LIFNR(*)" IS NULL OR raw."LIFNR(*)" = ''
            THEN 'LIFNR obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" sup
            WHERE sup."LIFNR(k/*)" = raw."LIFNR(*)"
        ) AND NOT EXISTS (
            SELECT 1 FROM raw."S_SUPPL_GEN#AddDatiGenerali" sup
            WHERE sup."LIFNR(k/*)" = raw."LIFNR(*)"
        )
            THEN 'LIFNR [' || raw."LIFNR(*)" || '] non presente in '
                 || 'S_SUPPL_GEN#ZBP_DatiGenerali né in S_SUPPL_GEN#AddDatiGenerali'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN raw."MATNR" IS NULL OR raw."MATNR" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK512')
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARA" mara
            WHERE mara."PRODUCT(k/*)" = raw."MATNR"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK512')
        WHEN raw."LIFNR(*)" IS NULL OR raw."LIFNR(*)" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK512')
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" sup
            WHERE sup."LIFNR(k/*)" = raw."LIFNR(*)"
        ) AND NOT EXISTS (
            SELECT 1 FROM raw."S_SUPPL_GEN#AddDatiGenerali" sup
            WHERE sup."LIFNR(k/*)" = raw."LIFNR(*)"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK512')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    raw."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_EINA#INFORMATFOR" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK512'
)
;
