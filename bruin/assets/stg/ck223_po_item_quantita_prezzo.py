""" @bruin
name: stg.ck223_po_item_quantita_prezzo
type: python
depends:
  - stg.clean_check_results
description: >
  CK223 — EXISTENCE: Ordini di acquisto (S_EKPO): quantità MENGE, prezzo netto NETPR e unità di prezzo
  PEINH obbligatori, numerici e maggiori di zero. Un esito per posizione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

# Numero da testo ('2500.00'); NULL se vuoto o non numerico
NUM = "CASE WHEN TRIM({c}) ~ '^-?[0-9]+([.,][0-9]+)?$' THEN REPLACE(TRIM({c}), ',', '.')::numeric END"

SQL = f"""
SELECT 'S_EKPO', p."EBELN(k/*)" || '/' || p."EBELP(k/*)",
       NULLIF(CONCAT_WS(', ',
           CASE WHEN COALESCE({NUM.format(c='p."MENGE"')}, 0) <= 0 THEN 'MENGE [' || COALESCE(TRIM(p."MENGE"), '') || ']' END,
           CASE WHEN COALESCE({NUM.format(c='p."NETPR"')}, 0) <= 0 THEN 'NETPR [' || COALESCE(TRIM(p."NETPR"), '') || ']' END,
           CASE WHEN COALESCE({NUM.format(c='p."PEINH"')}, 0) <= 0 THEN 'PEINH [' || COALESCE(TRIM(p."PEINH"), '') || ']' END
       ), '') || ' vuoto, non numerico o non maggiore di zero',
       p."_source"
FROM raw."S_EKPO" p
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK223",
        sql=SQL,
        required=[("raw", "S_EKPO")],
    )
