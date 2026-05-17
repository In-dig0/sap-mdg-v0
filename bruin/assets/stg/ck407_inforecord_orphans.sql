/* @bruin
name: stg.ck407_inforecord_orphans
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK407 — CROSS_TABLE: Inforecord acquisti: ogni INFNR(k/*) nelle tabelle
  secondarie (S_EINE#INFORDATIACQ, S_SCALES#INFORSCALES, S_COND#INFORCOND)
  deve essere presente nella master S_EINA#INFORMATFOR.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)

-- S_EINE#INFORDATIACQ
SELECT
    'S_EINE#INFORDATIACQ'                           AS source_table,
    'MAT'                                           AS category,
    sec."INFNR(k/*)"                                AS object_key,
    'CK407'                                         AS check_id,
    'INFNR [' || sec."INFNR(k/*)" || '] '
        || 'presente in S_EINE#INFORDATIACQ ma assente nella master S_EINA#INFORMATFOR'
                                                    AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK407')
                                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_EINE#INFORDATIACQ" sec
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_EINA#INFORMATFOR" mst
    WHERE mst."INFNR(k/*)" = sec."INFNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK407')

UNION ALL

-- S_SCALES#INFORSCALES
SELECT
    'S_SCALES#INFORSCALES'                          AS source_table,
    'MAT'                                           AS category,
    sec."INFNR(k/*)"                                AS object_key,
    'CK407'                                         AS check_id,
    'INFNR [' || sec."INFNR(k/*)" || '] '
        || 'presente in S_SCALES#INFORSCALES ma assente nella master S_EINA#INFORMATFOR'
                                                    AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK407')
                                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_SCALES#INFORSCALES" sec
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_EINA#INFORMATFOR" mst
    WHERE mst."INFNR(k/*)" = sec."INFNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK407')

UNION ALL

-- S_COND#INFORCOND
SELECT
    'S_COND#INFORCOND'                              AS source_table,
    'MAT'                                           AS category,
    sec."INFNR(k/*)"                                AS object_key,
    'CK407'                                         AS check_id,
    'INFNR [' || sec."INFNR(k/*)" || '] '
        || 'presente in S_COND#INFORCOND ma assente nella master S_EINA#INFORMATFOR'
                                                    AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK407')
                                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_COND#INFORCOND" sec
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_EINA#INFORMATFOR" mst
    WHERE mst."INFNR(k/*)" = sec."INFNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK407')

-- Record Ok: INFNR presenti nella master (uno per tabella secondaria distinta)
UNION ALL
SELECT DISTINCT
    'S_EINE#INFORDATIACQ'                           AS source_table,
    'MAT'                                           AS category,
    sec."INFNR(k/*)"                                AS object_key,
    'CK407'                                         AS check_id,
    'Ok'                                            AS message,
    'Ok'                                            AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_EINE#INFORDATIACQ" sec
WHERE EXISTS (
    SELECT 1 FROM raw."S_EINA#INFORMATFOR" mst
    WHERE mst."INFNR(k/*)" = sec."INFNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK407')
;
