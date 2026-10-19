/* @bruin
name: stg.ck207_bp_taxnum_duplicati_cross
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK207 — EXISTENCE: Business Partner (Fornitori + Clienti): identità fiscale
  duplicata tra BP diversi, anche tra fornitori e clienti.
  Due BP sono duplicati se condividono la stessa combinazione di:
    - IT1 (codice fiscale) + IT2 (partita IVA), oppure
    - IT0 (partita IVA UE) + IT1 (codice fiscale).
  La sola partita IVA uguale non basta (es. soggetti diversi con stessa P.IVA
  ma codice fiscale diverso). IT4 (codice SDI) non è considerato.
  BP esteri (senza IT1): duplicati se hanno lo stesso identificativo fiscale
  estero, a parità di TAXTYPE (UE come AT0/DE0 ed extra-UE come CH1/TR2/TN1),
  normalizzato come in CK801/CK803 (CC + numero, senza spazi, punti e trattini).
  Tabelle:
    - S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc
    - S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc
    - S_CUST_TAXNUMBERS#ZBP-CodiciFisc
  Un esito per BP, con le combinazioni condivise e i BP coinvolti.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
WITH all_taxnums AS (
    SELECT 'Fornitore' AS ruolo, 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc' AS source_table,
           "LIFNR(k/*)" AS bp_key, "TAXTYPE(k/*)" AS taxtype, "TAXNUM(*)" AS taxnum, "_source" AS zip_source
    FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"
    UNION ALL
    SELECT 'Fornitore', 'S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc',
           "LIFNR(k/*)", "TAXTYPE(k/*)", "TAXNUM(*)", "_source"
    FROM raw."S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc"
    UNION ALL
    SELECT 'Cliente', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc',
           "KUNNR(k/*)", "TAXTYPE(k/*)", "TAXNUM(*)", "_source"
    FROM raw."S_CUST_TAXNUMBERS#ZBP-CodiciFisc"
),
-- Una riga per BP (ruolo + codice) con i suoi identificativi fiscali IT0 / IT1 / IT2
-- (anche i BP esteri, che hanno solo la P.IVA del proprio paese)
bp AS (
    SELECT
        ruolo,
        bp_key,
        MIN(source_table)                                              AS source_table,
        MIN(zip_source)                                                AS zip_source,
        MAX(TRIM(taxnum)) FILTER (WHERE taxtype = 'IT0')               AS it0,
        MAX(TRIM(taxnum)) FILTER (WHERE taxtype = 'IT1')               AS it1,
        MAX(TRIM(taxnum)) FILTER (WHERE taxtype = 'IT2')               AS it2
    FROM all_taxnums
    WHERE taxnum IS NOT NULL
      AND TRIM(taxnum) <> ''
    GROUP BY ruolo, bp_key
),
-- Combinazioni che identificano il soggetto fiscale
identita AS (
    SELECT ruolo, bp_key, source_table, zip_source,
           'IT1=' || it1 || ' / IT2=' || it2 AS combinazione
    FROM bp WHERE it1 IS NOT NULL AND it2 IS NOT NULL
    UNION ALL
    SELECT ruolo, bp_key, source_table, zip_source,
           'IT0=' || it0 || ' / IT1=' || it1
    FROM bp WHERE it0 IS NOT NULL AND it1 IS NOT NULL
    UNION ALL
    -- BP esteri (senza codice fiscale italiano): identificativo fiscale estero (UE ed extra-UE)
    SELECT a.ruolo, a.bp_key, b.source_table, b.zip_source,
           a.taxtype || '=' || CASE
               WHEN regexp_replace(UPPER(a.taxnum), '[[:space:].-]', '', 'g') LIKE UPPER(LEFT(a.taxtype, 2)) || '%'
                   THEN regexp_replace(UPPER(a.taxnum), '[[:space:].-]', '', 'g')
               ELSE UPPER(LEFT(a.taxtype, 2)) || regexp_replace(UPPER(a.taxnum), '[[:space:].-]', '', 'g')
           END
    FROM all_taxnums a
    JOIN bp b ON b.ruolo = a.ruolo AND b.bp_key = a.bp_key
    WHERE b.it1 IS NULL
      AND a.taxtype NOT LIKE 'IT%'
      AND a.taxnum IS NOT NULL
      AND TRIM(a.taxnum) <> ''
),
dup AS (
    SELECT
        combinazione,
        COUNT(DISTINCT ruolo || ':' || bp_key)                              AS cnt,
        STRING_AGG(DISTINCT ruolo || ' ' || bp_key, ', '
                   ORDER BY ruolo || ' ' || bp_key)                        AS bp_coinvolti
    FROM identita
    GROUP BY combinazione
    HAVING COUNT(DISTINCT ruolo || ':' || bp_key) > 1
)
-- Un esito per BP: le combinazioni condivise (IT1+IT2, IT0+IT1, identificativo estero) in un'unica riga
SELECT
    i.source_table                               AS source_table,
    'BP'                                         AS category,
    i.bp_key                                     AS object_key,
    'CK207'                                      AS check_id,
    'Identità fiscale condivisa [' || STRING_AGG(i.combinazione, ' ; ' ORDER BY i.combinazione)
        || '] — BP coinvolti: ' || MIN(d.bp_coinvolti)
                                                 AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK207') AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    i.zip_source                                 AS zip_source,
    NOW()                                        AS created_at
FROM identita i
JOIN dup d ON d.combinazione = i.combinazione
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK207'
)
GROUP BY i.ruolo, i.bp_key, i.source_table, i.zip_source
;
