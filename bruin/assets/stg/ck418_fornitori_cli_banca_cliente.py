""" @bruin
name: stg.ck418_fornitori_cli_banca_cliente
type: python
depends:
  - stg.clean_check_results
  - setup.fn_fornitori_cli
  - prd.merge_banche_clienti
description: >
  CK418 — CROSS_TABLE: Clienti estesi a fornitori (flusso DM06-05a): coordinate bancarie del
  fornitore (S_SUPP_BANK#ZBP_AddAppoggioBanca) confrontate con quelle del cliente di
  riferimento già caricato in SAP (prd."S_CUST_BANK_DATA#ZBP-AppoggioBanca", tramite REF_KUNNR).
    - IBAN uguale a uno del cliente (stg.fn_iban_uguale_cliente): Ok, la riga non è
      esportata nello ZIP (la coordinata è già sul BP in SAP);
    - IBAN diverso, o cliente senza coordinate bancarie: Warning, la riga è esportata.
  Un esito per riga. Saltato se il flusso 05a non è caricato.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_SUPP_BANK#ZBP_AddAppoggioBanca', a."LIFNR(k/*)",
       CASE
           WHEN stg.fn_iban_uguale_cliente(a."LIFNR(k/*)", a."IBAN(k)") THEN NULL
           WHEN COALESCE(TRIM(a."IBAN(k)"), '') = ''
               THEN 'IBAN vuoto: non confrontabile con le coordinate del cliente [' || fc.kunnr || ']'
           WHEN cli.iban IS NULL
               THEN 'IBAN [' || TRIM(a."IBAN(k)") || '] esportato: il cliente [' || fc.kunnr
                    || '] non ha coordinate bancarie in SAP'
           ELSE 'IBAN [' || TRIM(a."IBAN(k)") || '] esportato: diverso da quello del cliente ['
                || fc.kunnr || '] (' || cli.iban || ')'
       END,
       a."_source",
       'IBAN [' || TRIM(a."IBAN(k)") || '] uguale a quello del cliente [' || fc.kunnr
           || ']: riga non esportata'
FROM raw."S_SUPP_BANK#ZBP_AddAppoggioBanca" a
JOIN stg.fn_fornitori_da_clienti() fc ON fc.lifnr = TRIM(a."LIFNR(k/*)")
LEFT JOIN (
    SELECT TRIM("KUNNR(k/*)") AS kunnr, string_agg(TRIM("IBAN(k)"), ', ' ORDER BY "IBAN(k)") AS iban
    FROM prd."S_CUST_BANK_DATA#ZBP-AppoggioBanca"
    WHERE COALESCE(TRIM("IBAN(k)"), '') <> ''
    GROUP BY 1
) cli ON cli.kunnr = fc.kunnr
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK418",
        sql=SQL,
        required=[("raw", "S_SUPP_BANK#ZBP_AddAppoggioBanca"), ("raw", "S_SUPPL_GEN#AddDatiGenerali"),
                  ("prd", "S_CUST_BANK_DATA#ZBP-AppoggioBanca")],
        category="BP",
    )
