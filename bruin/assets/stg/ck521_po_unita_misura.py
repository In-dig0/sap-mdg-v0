""" @bruin
name: stg.ck521_po_unita_misura
type: python
depends:
  - stg.clean_check_results
description: >
  CK521 — CROSS_SOURCE: Ordini di acquisto (S_EKPO): l'unità di misura dell'ordine MEINS deve essere
  l'unità base del materiale (S_MARA.MEINS(*)) o avere una conversione in S_MARM (MEINH(k/*)).
  Le posizioni con materiale vuoto o assente da S_MARA non sono controllate (vedi CK520).
  Un esito per posizione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKPO', p."EBELN(k/*)" || '/' || p."EBELP(k/*)",
       CASE
           WHEN COALESCE(TRIM(p."MEINS"), '') = '' THEN 'Unità di misura MEINS vuota'
           WHEN TRIM(p."MEINS") <> a.meins
            AND NOT EXISTS (SELECT 1 FROM raw."S_MARM" m WHERE TRIM(m."PRODUCT(k/*)") = a.matnr
                              AND TRIM(m."MEINH(k/*)") = TRIM(p."MEINS"))
               THEN 'Unità di misura MEINS [' || TRIM(p."MEINS") || '] diversa dall''unità base ['
                    || a.meins || '] del materiale ' || a.matnr || ' e senza conversione in S_MARM'
       END,
       p."_source"
FROM raw."S_EKPO" p
JOIN (
    SELECT DISTINCT ON (TRIM("PRODUCT(k/*)")) TRIM("PRODUCT(k/*)") AS matnr, COALESCE(TRIM("MEINS(*)"), '') AS meins
    FROM raw."S_MARA"
    ORDER BY TRIM("PRODUCT(k/*)")
) a ON a.matnr = TRIM(p."MATNR")
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK521",
        sql=SQL,
        required=[("raw", "S_EKPO"), ("raw", "S_MARA"), ("raw", "S_MARM")],
    )
