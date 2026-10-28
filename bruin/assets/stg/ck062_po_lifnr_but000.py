""" @bruin
name: stg.ck062_po_lifnr_but000
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK062 — SAP_REF: Ordini di acquisto (S_EKKO): il fornitore LIFNR(*) (codice SAP) deve essere presente
  in SAP_EXPORT_BUT000 (PARTNER) e il codice legacy VERKF deve coincidere con BU_SORT1.
  Saltato se SAP_EXPORT_BUT000 non è caricata. Un esito per ordine.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

SQL = """
SELECT 'S_EKKO', k."EBELN(k/*)",
       CASE
           WHEN COALESCE(TRIM(k."LIFNR(*)"), '') = '' THEN 'LIFNR(*) vuoto'
           WHEN b.partner IS NULL
               THEN 'Fornitore LIFNR(*) [' || TRIM(k."LIFNR(*)") || '] non presente in SAP_EXPORT_BUT000'
           WHEN COALESCE(TRIM(k."VERKF"), '') <> b.sort1
               THEN 'VERKF [' || COALESCE(TRIM(k."VERKF"), '') || '] diverso dal codice legacy del fornitore '
                    || b.partner || ' in SAP_EXPORT_BUT000 (BU_SORT1 [' || b.sort1 || '])'
       END,
       k."_source"
FROM raw."S_EKKO" k
LEFT JOIN (
    SELECT DISTINCT ON (TRIM("PARTNER")) TRIM("PARTNER") AS partner, COALESCE(TRIM("BU_SORT1"), '') AS sort1
    FROM ref."SAP_EXPORT_BUT000"
    ORDER BY TRIM("PARTNER")
) b ON b.partner = TRIM(k."LIFNR(*)")
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK062",
        sql=SQL,
        required=[("raw", "S_EKKO"), ("ref", "SAP_EXPORT_BUT000")],
    )
