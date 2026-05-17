/* @bruin
name: stg.ck408_cicli_lavoro_orphans
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK408 — CROSS_TABLE: Cicli di lavoro: ogni PLNNR(k/*) nelle tabelle
  secondarie (S_MAPL, S_GROUP, S_PLKO) deve essere presente nella master
  S_OPERATION.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)

-- S_MAPL
SELECT
    'S_MAPL'                                        AS source_table,
    'MAT'                                           AS category,
    sec."PLNNR(k/*)"                                AS object_key,
    'CK408'                                         AS check_id,
    'PLNNR [' || sec."PLNNR(k/*)" || '] '
        || 'presente in S_MAPL ma assente nella master S_OPERATION'
                                                    AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK408')
                                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_MAPL" sec
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_OPERATION" mst
    WHERE mst."PLNNR(k/*)" = sec."PLNNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK408')

UNION ALL

-- S_GROUP
SELECT
    'S_GROUP'                                       AS source_table,
    'MAT'                                           AS category,
    sec."PLNNR(k/*)"                                AS object_key,
    'CK408'                                         AS check_id,
    'PLNNR [' || sec."PLNNR(k/*)" || '] '
        || 'presente in S_GROUP ma assente nella master S_OPERATION'
                                                    AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK408')
                                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_GROUP" sec
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_OPERATION" mst
    WHERE mst."PLNNR(k/*)" = sec."PLNNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK408')

UNION ALL

-- S_PLKO
SELECT
    'S_PLKO'                                        AS source_table,
    'MAT'                                           AS category,
    sec."PLNNR(k/*)"                                AS object_key,
    'CK408'                                         AS check_id,
    'PLNNR [' || sec."PLNNR(k/*)" || '] '
        || 'presente in S_PLKO ma assente nella master S_OPERATION'
                                                    AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK408')
                                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_PLKO" sec
WHERE NOT EXISTS (
    SELECT 1 FROM raw."S_OPERATION" mst
    WHERE mst."PLNNR(k/*)" = sec."PLNNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK408')

UNION ALL

-- Ok: PLNNR presenti nella master (campione da S_MAPL come riferimento)
SELECT DISTINCT
    'S_MAPL'                                        AS source_table,
    'MAT'                                           AS category,
    sec."PLNNR(k/*)"                                AS object_key,
    'CK408'                                         AS check_id,
    'Ok'                                            AS message,
    'Ok'                                            AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    sec."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_MAPL" sec
WHERE EXISTS (
    SELECT 1 FROM raw."S_OPERATION" mst
    WHERE mst."PLNNR(k/*)" = sec."PLNNR(k/*)"
)
AND (SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = 'CK408')
;
