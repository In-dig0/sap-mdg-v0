""" @bruin
name: stg.ck423_customer_partner_fornitore_sap
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK423 — CROSS_SOURCE: Partner dei clienti (S_CUST_SALES_PARTNER#ZBP-Partner) con funzione
  RA o ZA, che rimandano a un fornitore: il fornitore KUNN2(k) deve esistere in SAP
  (ref.SAP_EXPORT_BUT000, BU_SORT1; se BUT000 non è caricata, i fornitori di raw
  S_SUPPL_GEN#ZBP_DatiGenerali). Con l'esclusione dall'output attiva nel catalogo la riga non
  è esportata (prd.export_clienti_zip, file discarded/). Un esito per riga RA/ZA.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

TABLE = "S_CUST_SALES_PARTNER#ZBP-Partner"


def build_sql(cur):
    if not table_exists(cur, "raw", TABLE):
        return None
    if table_exists(cur, "ref", "SAP_EXPORT_BUT000"):
        vendors = 'SELECT TRIM("BU_SORT1") AS v FROM ref."SAP_EXPORT_BUT000"'
    elif table_exists(cur, "raw", "S_SUPPL_GEN#ZBP_DatiGenerali"):
        vendors = 'SELECT TRIM("LIFNR(k/*)") AS v FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali"'
    else:
        vendors = "SELECT NULL::text AS v WHERE FALSE"
    key = " || '|' || ".join(f'COALESCE(p."{c}", \'\')' for c in
                             ["KUNNR(k/*)", "VKORG(k/*)", "VTWEG(k/*)", "SPART(k/*)", "PARVW(k/*)", "KUNN2(k)"])
    return f"""
WITH vendors AS (SELECT DISTINCT v FROM ({vendors}) x WHERE COALESCE(v, '') <> '')
SELECT '{TABLE}', {key},
       CASE WHEN v.v IS NULL
            THEN 'Partner ' || TRIM(p."PARVW(k/*)") || ' del cliente ' || TRIM(p."KUNNR(k/*)")
                 || ': fornitore [' || COALESCE(TRIM(p."KUNN2(k)"), '') || '] non presente in SAP'
       END,
       p."_source"
FROM raw."{TABLE}" p
LEFT JOIN vendors v ON v.v = TRIM(p."KUNN2(k)")
WHERE TRIM(p."PARVW(k/*)") IN ('RA', 'ZA')"""


if __name__ == "__main__":
    run_query_check(check_id="CK423", sql=build_sql, required=[], category="BP")
