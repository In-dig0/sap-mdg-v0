""" @bruin
name: stg.ck224_po_consegna_passata
type: python
depends:
  - stg.clean_check_results
description: >
  CK224 — EXISTENCE: Ordini di acquisto (S_EKET): la data di consegna EINDT(*) deve essere una data
  valida (AAAAMMGG) non precedente alla data del run. Un esito per schedulazione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKET', e."EBELN(k/*)" || '/' || e."EBELP(k/*)" || '/' || e."ETENR(k/*)",
       CASE
           WHEN d.eindt IS NULL
               THEN 'Data di consegna EINDT(*) [' || COALESCE(TRIM(e."EINDT(*)"), '') || '] vuota o non valida'
           WHEN d.eindt < CURRENT_DATE
               THEN 'Data di consegna EINDT(*) [' || TO_CHAR(d.eindt, 'DD/MM/YYYY') || '] già passata'
       END,
       e."_source"
FROM raw."S_EKET" e
CROSS JOIN LATERAL (SELECT TRIM(e."EINDT(*)") AS v) t
CROSS JOIN LATERAL (
    -- Validazione prima della conversione: TO_DATE va in errore su mese o giorno fuori
    -- intervallo (es. 07203100), che deve invece diventare una segnalazione
    SELECT CASE WHEN t.v ~ '^[1-9][0-9]{3}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$' THEN
               CASE WHEN SUBSTR(t.v, 7, 2)::int <= EXTRACT(DAY FROM
                         MAKE_DATE(SUBSTR(t.v, 1, 4)::int, SUBSTR(t.v, 5, 2)::int, 1) + INTERVAL '1 month - 1 day')
                    THEN TO_DATE(t.v, 'YYYYMMDD') END
           END AS eindt
) d
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK224",
        sql=SQL,
        required=[("raw", "S_EKET")],
    )
