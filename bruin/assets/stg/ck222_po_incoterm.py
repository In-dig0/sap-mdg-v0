""" @bruin
name: stg.ck222_po_incoterm
type: python
depends:
  - stg.clean_check_results
description: >
  CK222 — EXISTENCE: Ordini di acquisto (S_EKKO): gli Incoterm INCO1 e INCO2_L devono essere
  valorizzati (le stringhe di soli spazi valgono come vuote). Un esito per ordine.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKKO', k."EBELN(k/*)",
       CASE
           WHEN m.i1 AND m.i2 THEN 'Incoterm mancanti: INCO1 e INCO2_L vuoti'
           WHEN m.i1 THEN 'Incoterm mancante: INCO1 vuoto'
           WHEN m.i2 THEN 'Incoterm mancante: INCO2_L vuoto'
       END,
       k."_source"
FROM raw."S_EKKO" k
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(k."INCO1"), '') = '' AS i1,
           COALESCE(TRIM(k."INCO2_L"), '') = '' AS i2
) m
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK222",
        sql=SQL,
        required=[("raw", "S_EKKO")],
    )
