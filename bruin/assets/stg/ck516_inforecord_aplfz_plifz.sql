/* @bruin
name: stg.ck516_inforecord_aplfz_plifz
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK516 — CROSS_SOURCE: Inforecord acquisti (S_EINE#INFORDATIACQ):
  il campo APLFZ può essere vuoto/nullo solo se il corrispondente campo
  PLIFZ in S_MARC è valorizzato. Se entrambi sono vuoti/nulli si segnala errore.
  Join chain: S_EINE -> S_EINA (INFNR) -> S_MARC (MATNR + WERKS).
  I record senza corrispondenza in S_EINA o S_MARC sono esclusi
  (già coperti da CK407 e CK512).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_EINE#INFORDATIACQ'                           AS source_table,
    'MAT'                                           AS category,
    eine."INFNR(k/*)"                               AS object_key,
    'CK516'                                         AS check_id,
    CASE
        WHEN (eine."APLFZ" IS NULL OR eine."APLFZ" = '')
         AND (marc."PLIFZ"  IS NULL OR marc."PLIFZ"  = '')
            THEN 'APLFZ vuoto in S_EINE e PLIFZ vuoto in S_MARC'
                 || ' per INFNR [' || eine."INFNR(k/*)" || ']'
                 || ', materiale [' || eina."MATNR" || ']'
                 || ', plant ['     || eine."WERKS(k)"  || ']'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN (eine."APLFZ" IS NULL OR eine."APLFZ" = '')
         AND (marc."PLIFZ"  IS NULL OR marc."PLIFZ"  = '')
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK516')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    eine."_source"                                  AS zip_source,
    NOW()                                           AS created_at
FROM      raw."S_EINE#INFORDATIACQ"  eine
JOIN      raw."S_EINA#INFORMATFOR"   eina
       ON eina."INFNR(k/*)" = eine."INFNR(k/*)"
JOIN      raw."S_MARC"               marc
       ON marc."PRODUCT(k/*)" = eina."MATNR"
      AND marc."WERKS(k/*)"   = eine."WERKS(k)"
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK516'
)
;
