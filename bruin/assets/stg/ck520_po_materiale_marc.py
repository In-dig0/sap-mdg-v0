""" @bruin
name: stg.ck520_po_materiale_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK520 — CROSS_SOURCE: Ordini di acquisto (S_EKPO): il materiale MATNR deve essere presente
  nell'anagrafica materiali (S_MARA) ed esteso al plant WERKS della posizione (S_MARC).
  Le posizioni senza materiale non sono controllate. Un esito per posizione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKPO', p."EBELN(k/*)" || '/' || p."EBELP(k/*)",
       CASE
           WHEN NOT EXISTS (SELECT 1 FROM raw."S_MARA" a WHERE TRIM(a."PRODUCT(k/*)") = TRIM(p."MATNR"))
               THEN 'Materiale MATNR [' || TRIM(p."MATNR") || '] non presente in S_MARA'
           WHEN NOT EXISTS (SELECT 1 FROM raw."S_MARC" c WHERE TRIM(c."PRODUCT(k/*)") = TRIM(p."MATNR")
                              AND TRIM(c."WERKS(k/*)") = TRIM(p."WERKS"))
               THEN 'Materiale MATNR [' || TRIM(p."MATNR") || '] non esteso al plant WERKS ['
                    || COALESCE(TRIM(p."WERKS"), '') || '] in S_MARC'
       END,
       p."_source"
FROM raw."S_EKPO" p
WHERE COALESCE(TRIM(p."MATNR"), '') <> ''
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK520",
        sql=SQL,
        required=[("raw", "S_EKPO"), ("raw", "S_MARA"), ("raw", "S_MARC")],
    )
