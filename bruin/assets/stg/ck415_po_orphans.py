""" @bruin
name: stg.ck415_po_orphans
type: python
depends:
  - stg.clean_check_results
description: >
  CK415 — CROSS_TABLE: Ordini di acquisto: ogni posizione (S_EKPO) deve avere la testata (S_EKKO),
  ogni schedulazione (S_EKET) la posizione, ogni testata almeno una posizione e ogni posizione
  almeno una schedulazione. Un esito per testata, posizione e schedulazione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKKO', k."EBELN(k/*)",
       CASE WHEN NOT EXISTS (SELECT 1 FROM raw."S_EKPO" p WHERE TRIM(p."EBELN(k/*)") = TRIM(k."EBELN(k/*)"))
            THEN 'Ordine senza posizioni in S_EKPO' END,
       k."_source"
FROM raw."S_EKKO" k
UNION ALL
SELECT 'S_EKPO', p."EBELN(k/*)" || '/' || p."EBELP(k/*)",
       NULLIF(CONCAT_WS(', ',
           CASE WHEN NOT EXISTS (SELECT 1 FROM raw."S_EKKO" k WHERE TRIM(k."EBELN(k/*)") = TRIM(p."EBELN(k/*)"))
                THEN 'Posizione senza testata in S_EKKO' END,
           CASE WHEN NOT EXISTS (SELECT 1 FROM raw."S_EKET" e WHERE TRIM(e."EBELN(k/*)") = TRIM(p."EBELN(k/*)")
                                   AND TRIM(e."EBELP(k/*)") = TRIM(p."EBELP(k/*)"))
                THEN 'posizione senza schedulazioni in S_EKET' END
       ), ''),
       p."_source"
FROM raw."S_EKPO" p
UNION ALL
SELECT 'S_EKET', e."EBELN(k/*)" || '/' || e."EBELP(k/*)" || '/' || e."ETENR(k/*)",
       CASE WHEN NOT EXISTS (SELECT 1 FROM raw."S_EKPO" p WHERE TRIM(p."EBELN(k/*)") = TRIM(e."EBELN(k/*)")
                               AND TRIM(p."EBELP(k/*)") = TRIM(e."EBELP(k/*)"))
            THEN 'Schedulazione senza posizione in S_EKPO' END,
       e."_source"
FROM raw."S_EKET" e
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK415",
        sql=SQL,
        required=[("raw", "S_EKKO"), ("raw", "S_EKPO"), ("raw", "S_EKET")],
    )
