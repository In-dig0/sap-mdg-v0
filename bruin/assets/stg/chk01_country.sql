/* @bruin
name: stg.chk01_country
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK01 — Verifica che il campo COUNTRY della tabella
  S_SUPPL_GEN#ZBP_DatiGenerali sia valorizzato e presente
  nella tabella di controllo SAP ref.EXPORT_T005S (colonna LAND1).
  COUNTRY è obbligatorio per SAP.
  Segnala: COUNTRY vuoto/nullo E valore non presente in T005S.
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
    'CHK01'                                      AS check_id,
    CASE
        WHEN raw."COUNTRY" IS NULL OR raw."COUNTRY" = ''
            THEN 'COUNTRY obbligatorio mancante'
        ELSE
            'Codice paese [' || raw."COUNTRY" || '] non presente in SAP (T005S.LAND1)'
    END                                          AS message,
    'Error'                                      AS status,
    TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS')          AS run_id,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
WHERE
    (raw."COUNTRY" IS NULL OR raw."COUNTRY" = '')
    OR
    (
        raw."COUNTRY" IS NOT NULL
        AND raw."COUNTRY" <> ''
        AND NOT EXISTS (
            SELECT 1
            FROM ref."EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
        )
    )
;
