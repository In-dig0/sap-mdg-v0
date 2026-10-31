""" @bruin
name: stg.ck419_fornitori_cli_codici_fiscali_cliente
type: python
depends:
  - stg.clean_check_results
  - setup.fn_fornitori_cli
  - prd.merge_taxnumbers_clienti
description: >
  CK419 — CROSS_TABLE: Clienti estesi a fornitori (flusso DM06-05a): identificativi fiscali del
  fornitore (S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc) confrontati con quelli del cliente di
  riferimento già caricato in SAP (prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc", tramite REF_KUNNR).
    - stesso TAXTYPE con lo stesso identificativo (normalizzato, stg.fn_taxnum_uguale_cliente):
      Ok, la riga non è esportata nello ZIP (il dato è già sul BP in SAP);
    - identificativo diverso, TAXTYPE assente nel cliente o TAXTYPE vuoto: Warning,
      la riga è esportata.
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
           WHEN COALESCE(TRIM(a."TAXTYPE(k/*)"), '') = ''
               THEN 'TAXTYPE vuoto per l''identificativo [' || COALESCE(TRIM(a."TAXNUM(*)"), '')
                    || ']: riga esportata, non confrontabile con il cliente [' || fc.kunnr || ']'
           WHEN cli.taxnum IS NULL
               THEN TRIM(a."TAXTYPE(k/*)") || ' [' || COALESCE(TRIM(a."TAXNUM(*)"), '') || '] esportato: il cliente ['
                    || fc.kunnr || '] non ha questo tipo di identificativo in SAP'
           ELSE TRIM(a."TAXTYPE(k/*)") || ' [' || COALESCE(TRIM(a."TAXNUM(*)"), '') || '] esportato: diverso da quello del cliente ['
                || fc.kunnr || '] (' || cli.taxnum || ')'
       END,
       a."_source",
       TRIM(a."TAXTYPE(k/*)") || ' [' || COALESCE(TRIM(a."TAXNUM(*)"), '') || '] uguale a quello del cliente ['
           || fc.kunnr || ']: riga non esportata'
FROM raw."S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc" a
JOIN stg.fn_fornitori_da_clienti() fc ON fc.lifnr = TRIM(a."LIFNR(k/*)")
LEFT JOIN (
    SELECT TRIM("KUNNR(k/*)") AS kunnr, "TAXTYPE(k/*)" AS taxtype,
           string_agg(TRIM("TAXNUM(*)"), ', ' ORDER BY "TAXNUM(*)") AS taxnum
    FROM prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc"
    WHERE COALESCE(TRIM("TAXNUM(*)"), '') <> ''
    GROUP BY 1, 2
) cli ON cli.kunnr = fc.kunnr AND cli.taxtype = TRIM(a."TAXTYPE(k/*)")
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK419",
        sql=SQL,
        required=[("raw", "S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc"), ("raw", "S_SUPPL_GEN#AddDatiGenerali"),
                  ("prd", "S_CUST_TAXNUMBERS#ZBP-CodiciFisc")],
        category="BP",
    )
