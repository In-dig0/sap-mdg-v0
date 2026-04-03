/* @bruin
name: stg.chk02_country_region
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK02 — Verifica che la coppia COUNTRY+REGION della tabella
  S_SUPPL_GEN#ZBP_DatiGenerali esista nella tabella di
  controllo SAP ref.EXPORT_T005S (colonne LAND1+BLAND).
  COUNTRY e REGION sono entrambi obbligatori per SAP.
  Segnala: REGION vuota/nulla E coppia non presente in T005S.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table,
    category,
    object_key,
    check_id,
    message,
    status,
    run_id,
    created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'              AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CHK02'                                      AS check_id,
    CASE
        WHEN raw."REGION" IS NULL OR raw."REGION" = ''
            THEN 'REGION obbligatoria mancante (COUNTRY=' || COALESCE(raw."COUNTRY", 'NULL') || ')'
        ELSE
            'Coppia paese/regione [' || raw."COUNTRY" || '/' || raw."REGION" || '] non presente in SAP (T005S)'
    END                                          AS message,
    'Error'                                      AS status,
    TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS')          AS run_id,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
WHERE
    (raw."REGION" IS NULL OR raw."REGION" = '')
    OR
    (
        raw."REGION" IS NOT NULL
        AND raw."REGION" <> ''
        AND NOT EXISTS (
            SELECT 1
            FROM ref."EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
              AND ref."BLAND" = raw."REGION"
        )
    )
;
