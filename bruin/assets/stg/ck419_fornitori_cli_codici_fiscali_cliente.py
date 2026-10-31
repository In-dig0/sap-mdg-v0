""" @bruin
name: stg.ck419_fornitori_cli_codici_fiscali_cliente
type: python
depends:
  - stg.clean_check_results
  - setup.fn_fornitori_cli
  - prd.merge_taxnumbers_clienti
  - stg.ck801_customer_vat_vies     # esiti VIES dei clienti
description: >
  CK419 — CROSS_TABLE: Clienti estesi a fornitori (flusso DM06-05a): identificativi fiscali del
  fornitore (S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc) confrontati con quelli del cliente di
  riferimento già caricato in SAP (prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc", tramite REF_KUNNR).
  Il file non è esportato nello ZIP 05a: i codici fiscali sono quelli caricati con il cliente.
    - stesso TAXTYPE con lo stesso identificativo (normalizzato, stg.fn_taxnum_uguale_cliente): Ok;
    - P.IVA UE (TAXTYPE ..0) risultata non valida su VIES per il cliente (ultimo esito di
      CK801 in stg.check_vat_vies) e quindi esclusa anche dal cliente: Ok;
    - identificativo diverso, TAXTYPE assente nel cliente o TAXTYPE vuoto: Warning, il dato
      del fornitore non arriva in SAP con questo flusso (da verificare sul cliente).
  Un esito per riga. Saltato se il flusso 05a non è caricato.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc', a."LIFNR(k/*)",
       CASE
           WHEN stg.fn_taxnum_uguale_cliente(a."LIFNR(k/*)", a."TAXTYPE(k/*)", a."TAXNUM(*)") THEN NULL
           WHEN vies.entity_id IS NOT NULL THEN NULL
           WHEN COALESCE(TRIM(a."TAXTYPE(k/*)"), '') = ''
               THEN 'TAXTYPE vuoto per l''identificativo [' || COALESCE(TRIM(a."TAXNUM(*)"), '')
                    || ']: non confrontabile con il cliente [' || fc.kunnr || '], non caricato in SAP'
           WHEN cli.taxnum IS NULL
               THEN TRIM(a."TAXTYPE(k/*)") || ' [' || COALESCE(TRIM(a."TAXNUM(*)"), '') || '] non caricato in SAP: il cliente ['
                    || fc.kunnr || '] non ha questo tipo di identificativo'
           ELSE TRIM(a."TAXTYPE(k/*)") || ' [' || COALESCE(TRIM(a."TAXNUM(*)"), '') || '] non caricato in SAP: diverso da quello del cliente ['
                || fc.kunnr || '] (' || cli.taxnum || ')'
       END,
       a."_source",
       CASE
           WHEN vies.entity_id IS NOT NULL AND NOT stg.fn_taxnum_uguale_cliente(a."LIFNR(k/*)", a."TAXTYPE(k/*)", a."TAXNUM(*)")
               THEN TRIM(a."TAXTYPE(k/*)") || ' [' || COALESCE(TRIM(a."TAXNUM(*)"), '')
                    || '] non valida su VIES (esclusa anche dal cliente)'
           ELSE TRIM(a."TAXTYPE(k/*)") || ' [' || COALESCE(TRIM(a."TAXNUM(*)"), '') || '] uguale a quello del cliente ['
                || fc.kunnr || ']'
       END
FROM raw."S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc" a
JOIN stg.fn_fornitori_da_clienti() fc ON fc.lifnr = TRIM(a."LIFNR(k/*)")
LEFT JOIN (
    SELECT TRIM("KUNNR(k/*)") AS kunnr, "TAXTYPE(k/*)" AS taxtype,
           string_agg(TRIM("TAXNUM(*)"), ', ' ORDER BY "TAXNUM(*)") AS taxnum
    FROM prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc"
    WHERE COALESCE(TRIM("TAXNUM(*)"), '') <> ''
    GROUP BY 1, 2
) cli ON cli.kunnr = fc.kunnr AND cli.taxtype = TRIM(a."TAXTYPE(k/*)")
-- Ultimo esito VIES del cliente per la stessa P.IVA: INVALID = esclusa anche dal cliente
LEFT JOIN (
    SELECT DISTINCT ON (entity_id, vat_number) entity_id, vat_number, check_status
    FROM stg.check_vat_vies
    WHERE entity_type = 'customer' AND check_status NOT IN ('CACHED', 'PENDING')
    ORDER BY entity_id, vat_number, created_at DESC
) vies ON vies.entity_id = fc.kunnr
      AND TRIM(a."TAXTYPE(k/*)") LIKE '%0'
      AND vies.vat_number = stg.fn_norm_taxnum(a."TAXTYPE(k/*)", a."TAXNUM(*)")
      AND vies.check_status = 'INVALID'
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK419",
        sql=SQL,
        required=[("raw", "S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc"), ("raw", "S_SUPPL_GEN#AddDatiGenerali"),
                  ("prd", "S_CUST_TAXNUMBERS#ZBP-CodiciFisc")],
        category="BP",
    )
