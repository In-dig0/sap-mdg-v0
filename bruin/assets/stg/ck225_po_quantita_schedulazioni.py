""" @bruin
name: stg.ck225_po_quantita_schedulazioni
type: python
depends:
  - stg.clean_check_results
description: >
  CK225 — EXISTENCE: Ordini di acquisto: la quantità della posizione (S_EKPO.MENGE) deve essere uguale
  alla somma delle quantità delle schedulazioni (S_EKET.MENGE(*)). Le posizioni senza schedulazioni
  sono segnalate da CK415. Un esito per posizione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

# Numero da testo ('2500.00'); NULL se vuoto o non numerico
NUM = "CASE WHEN TRIM({c}) ~ '^-?[0-9]+([.,][0-9]+)?$' THEN REPLACE(TRIM({c}), ',', '.')::numeric END"

SQL = f"""
SELECT 'S_EKPO', p."EBELN(k/*)" || '/' || p."EBELP(k/*)",
       CASE WHEN {NUM.format(c='p."MENGE"')} IS DISTINCT FROM s.tot
            THEN 'Quantità posizione MENGE [' || COALESCE(TRIM(p."MENGE"), '') || '] diversa dalla somma delle '
                 || s.n || ' schedulazioni S_EKET [' || COALESCE(s.tot::text, 'non numerica') || ']'
       END,
       p."_source"
FROM raw."S_EKPO" p
JOIN (
    SELECT TRIM(e."EBELN(k/*)") AS ebeln, TRIM(e."EBELP(k/*)") AS ebelp, COUNT(*) AS n,
           SUM({NUM.format(c='e."MENGE(*)"')}) AS tot
    FROM raw."S_EKET" e
    GROUP BY 1, 2
) s ON s.ebeln = TRIM(p."EBELN(k/*)") AND s.ebelp = TRIM(p."EBELP(k/*)")
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK225",
        sql=SQL,
        required=[("raw", "S_EKPO"), ("raw", "S_EKET")],
    )
