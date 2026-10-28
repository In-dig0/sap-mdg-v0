""" @bruin
name: stg.ck522_po_materiale_stato
type: python
depends:
  - stg.clean_check_results
description: >
  CK522 — CROSS_SOURCE: Ordini di acquisto (S_EKPO): il materiale MATNR non deve avere in S_MARA
  lo stato MSTAE 07 (obsoleto) o 08 (incompleto). Le posizioni con materiale vuoto o assente
  da S_MARA non sono controllate (vedi CK520). Un esito per posizione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKPO', p."EBELN(k/*)" || '/' || p."EBELP(k/*)",
       CASE a.mstae
           WHEN '07' THEN 'Materiale ' || a.matnr || ' in stato MSTAE 07 (obsoleto) in S_MARA'
           WHEN '08' THEN 'Materiale ' || a.matnr || ' in stato MSTAE 08 (incompleto) in S_MARA'
       END,
       p."_source"
FROM raw."S_EKPO" p
JOIN (
    SELECT DISTINCT ON (TRIM("PRODUCT(k/*)")) TRIM("PRODUCT(k/*)") AS matnr, COALESCE(TRIM("MSTAE"), '') AS mstae
    FROM raw."S_MARA"
    ORDER BY TRIM("PRODUCT(k/*)")
) a ON a.matnr = TRIM(p."MATNR")
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK522",
        sql=SQL,
        required=[("raw", "S_EKPO"), ("raw", "S_MARA")],
    )
