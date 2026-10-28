""" @bruin
name: stg.ck524_po_fornitore_anagrafica
type: python
depends:
  - stg.clean_check_results
description: >
  CK524 — CROSS_SOURCE: Ordini di acquisto (S_EKKO): il codice legacy del fornitore VERKF deve
  essere presente nell'anagrafica fornitori (S_SUPPL_GEN#ZBP_DatiGenerali, LIFNR(k/*)).
  Se l'anagrafica fornitori non è caricata il controllo è saltato. Un esito per ordine.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKKO', k."EBELN(k/*)",
       CASE
           WHEN COALESCE(TRIM(k."VERKF"), '') = '' THEN 'Codice legacy del fornitore VERKF vuoto'
           WHEN NOT EXISTS (SELECT 1 FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" s
                            WHERE TRIM(s."LIFNR(k/*)") = TRIM(k."VERKF"))
               THEN 'Fornitore VERKF [' || TRIM(k."VERKF") || '] (LIFNR SAP ' || COALESCE(TRIM(k."LIFNR(*)"), '')
                    || ') non presente nell''anagrafica fornitori S_SUPPL_GEN#ZBP_DatiGenerali'
       END,
       k."_source"
FROM raw."S_EKKO" k
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK524",
        sql=SQL,
        required=[("raw", "S_EKKO"), ("raw", "S_SUPPL_GEN#ZBP_DatiGenerali")],
    )
