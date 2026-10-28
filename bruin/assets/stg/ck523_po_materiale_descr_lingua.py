""" @bruin
name: stg.ck523_po_materiale_descr_lingua
type: python
depends:
  - stg.clean_check_results
description: >
  CK523 — CROSS_SOURCE: Ordini di acquisto (S_EKPO): se la lingua del fornitore dell'ordine
  (S_SUPPL_GEN#ZBP_DatiGenerali.LANGU_CORR, collegato con S_EKKO.VERKF = LIFNR(k/*)) è diversa
  da IT, il materiale MATNR deve avere la descrizione in quella lingua in S_MAKT (SPRAS(k/*)).
  La descrizione IT è sempre presente in S_MARA. Le posizioni con fornitore assente
  dall'anagrafica (vedi CK524) o materiale vuoto non sono controllate. Un esito per posizione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKPO', p."EBELN(k/*)" || '/' || p."EBELP(k/*)",
       CASE WHEN f.lingua NOT IN ('', 'IT')
             AND NOT EXISTS (SELECT 1 FROM raw."S_MAKT" t
                             WHERE TRIM(t."PRODUCT(k/*)") = TRIM(p."MATNR")
                               AND TRIM(t."SPRAS(k/*)") = f.lingua
                               AND COALESCE(TRIM(t."MAKTX(*)"), '') <> '')
            THEN 'Materiale ' || TRIM(p."MATNR") || ' senza descrizione in lingua ' || f.lingua
                 || ' (lingua del fornitore ' || f.lifnr || ') in S_MAKT'
       END,
       p."_source"
FROM raw."S_EKPO" p
JOIN raw."S_EKKO" k ON TRIM(k."EBELN(k/*)") = TRIM(p."EBELN(k/*)")
JOIN (
    SELECT DISTINCT ON (TRIM("LIFNR(k/*)")) TRIM("LIFNR(k/*)") AS lifnr,
           UPPER(COALESCE(TRIM("LANGU_CORR"), '')) AS lingua
    FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali"
    ORDER BY TRIM("LIFNR(k/*)")
) f ON f.lifnr = TRIM(k."VERKF")
WHERE COALESCE(TRIM(p."MATNR"), '') <> ''
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK523",
        sql=SQL,
        required=[("raw", "S_EKPO"), ("raw", "S_EKKO"), ("raw", "S_MAKT"),
                  ("raw", "S_SUPPL_GEN#ZBP_DatiGenerali")],
    )
