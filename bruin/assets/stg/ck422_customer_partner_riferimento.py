""" @bruin
name: stg.ck422_customer_partner_riferimento
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK422 — EXISTENCE: Partner dei clienti (S_CUST_SALES_PARTNER#ZBP-Partner): ogni riga deve
  indicare a chi si riferisce il partner, cioè almeno uno tra KUNN2(k), LIFNR(k), PERNR(k) e
  PARNR(k). Con l'esclusione dall'output attiva nel catalogo la riga non è esportata
  (prd.export_clienti_zip, file discarded/). Un esito per riga.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

TABLE = "S_CUST_SALES_PARTNER#ZBP-Partner"
REF_COLS = ["KUNN2(k)", "LIFNR(k)", "PERNR(k)", "PARNR(k)"]


def build_sql(cur):
    if not table_exists(cur, "raw", TABLE):
        return None
    empty = " AND ".join(f"COALESCE(TRIM(p.\"{c}\"), '') = ''" for c in REF_COLS)
    key = " || '|' || ".join(f'COALESCE(p."{c}", \'\')' for c in
                             ["KUNNR(k/*)", "VKORG(k/*)", "VTWEG(k/*)", "SPART(k/*)", "PARVW(k/*)"])
    return f"""
SELECT '{TABLE}', {key},
       CASE WHEN {empty}
            THEN 'Partner ' || COALESCE(TRIM(p."PARVW(k/*)"), '') || ' del cliente ' || TRIM(p."KUNNR(k/*)")
                 || ' senza riferimento (KUNN2, LIFNR, PERNR e PARNR vuoti)'
       END,
       p."_source"
FROM raw."{TABLE}" p"""


if __name__ == "__main__":
    run_query_check(check_id="CK422", sql=build_sql, required=[], category="BP")
