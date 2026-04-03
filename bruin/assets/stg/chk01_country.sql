/* @bruin
name: stg.chk01_country
type: pg.sql
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
description: >
  CHK01 — Verifica che il campo COUNTRY della tabella
  S_SUPPL_GEN#ZBP_DatiGenerali sia valorizzato e presente
  nella tabella di controllo SAP ref.EXPORT_T005S (colonna LAND1).
  COUNTRY è obbligatorio per SAP.
  Segnala: COUNTRY vuoto/nullo E valore non presente in T005S.
connection: mdg_postgres
@bruin */

-- Pulizia esecuzioni precedenti (idempotente)
DELETE FROM stg.check_results
WHERE check_id = 'CHK01'
  AND source_table = 'S_SUPPL_GEN#ZBP_DatiGenerali';

INSERT INTO stg.check_results (
    source_table,
    category,
    object_key,
    check_id,
    message,
    status,
    created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'  AS source_table,
    'BP'                             AS category,
    raw."LIFNR(k/*)"                 AS object_key,
    'CHK01'                          AS check_id,
    CASE
        WHEN raw."COUNTRY" IS NULL OR raw."COUNTRY" = ''
            THEN 'COUNTRY obbligatorio mancante'
        ELSE
            'Codice paese [' || raw."COUNTRY" || '] non presente in SAP (T005S.LAND1)'
    END                              AS message,
    'Error'                          AS status,
    NOW()                            AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
WHERE
    -- Caso 1: COUNTRY vuoto o nullo (campo obbligatorio)
    (raw."COUNTRY" IS NULL OR raw."COUNTRY" = '')
    OR
    -- Caso 2: valore COUNTRY non presente in T005S.LAND1
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
