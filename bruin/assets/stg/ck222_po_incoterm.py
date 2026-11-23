""" @bruin
name: stg.ck222_po_incoterm
type: python
depends:
  - stg.clean_check_results
description: >
  CK222 — EXISTENCE: Ordini di acquisto (S_EKKO): gli Incoterm INCO1 e INCO2_L devono essere
  valorizzati (le stringhe di soli spazi valgono come vuote). Un esito per ordine.
  Gli Incoterm vuoti che l'export prende dall'anagrafica del fornitore (S_SUPPL_PURCHASING
  ZBP_OrganAcq, poi ZBP_AddOrganAcq, per LIFNR + EKORG; INCO2_L solo se INCO1 coincide con quello
  del fornitore) non sono un errore: esito Ok con l'origine del valore.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, po_bp_incoterms_sql


def build_sql(cur) -> str:
    bp_sql = po_bp_incoterms_sql(cur)
    bp_join = (f"LEFT JOIN ({bp_sql}) b ON b.lifnr = TRIM(k.\"LIFNR(*)\") AND b.ekorg = TRIM(k.\"EKORG(*)\")"
               if bp_sql else
               "LEFT JOIN (SELECT NULL::text AS inco1, NULL::text AS inco2, NULL::text AS src) b ON FALSE")
    return f"""
SELECT 'S_EKKO', k."EBELN(k/*)",
       CASE
           WHEN m.i1 AND m.i2 THEN 'Incoterm mancanti: INCO1 e INCO2_L vuoti'
           WHEN m.i1 THEN 'Incoterm mancante: INCO1 vuoto'
           WHEN m.i2 THEN 'Incoterm mancante: INCO2_L vuoto'
       END,
       k."_source",
       CASE WHEN m.from_bp1 OR m.from_bp2 THEN
           'Ok — dall''anagrafica del fornitore (' || b.src || '): '
           || CONCAT_WS(', ', CASE WHEN m.from_bp1 THEN 'INCO1 ' || b.inco1 END,
                              CASE WHEN m.from_bp2 THEN 'INCO2_L ' || b.inco2 END)
       END
FROM raw."S_EKKO" k
{bp_join}
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(k."INCO1"), '') = '' AS e1, COALESCE(TRIM(k."INCO2_L"), '') = '' AS e2
) e
CROSS JOIN LATERAL (
    SELECT e.e1 AND b.inco1 IS NOT NULL AS from_bp1,
           e.e2 AND b.inco2 IS NOT NULL
                AND COALESCE(NULLIF(TRIM(k."INCO1"), ''), b.inco1) = b.inco1 AS from_bp2
) f
CROSS JOIN LATERAL (
    SELECT e.e1 AND NOT f.from_bp1 AS i1, e.e2 AND NOT f.from_bp2 AS i2,
           f.from_bp1, f.from_bp2
) m
"""


if __name__ == "__main__":
    run_query_check(
        check_id="CK222",
        sql=build_sql,
        required=[("raw", "S_EKKO")],
    )
