""" @bruin
name: stg.ck426_customer_add_solo_dm
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK426 — CROSS_TABLE: Dati aggiuntivi e interlocutori dei clienti (03b, file
  ZBP-AddInterlocutoreCli): il codice KUNNR(k/*) deve essere un cliente (presente in
  S_CUST_GEN#ZBP-DatiGenerali) e non solo un destinatario merci (S_CUST_GEN#ZDM-DatiGenerali).
  Con l'esclusione dall'output attiva nel catalogo le righe non sono esportate
  (prd.export_clienti_add_zip, file discarded/). Un esito per codice e tabella.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

TABLES = ["S_CUST_GEN#AddDatiGenerali", "S_CUST_CONT#ZBP-AddInterlocutore"]
CUSTOMERS = "S_CUST_GEN#ZBP-DatiGenerali"
DM = "S_CUST_GEN#ZDM-DatiGenerali"


def build_sql(cur):
    if not table_exists(cur, "raw", DM):
        return None
    not_customer = (f'NOT EXISTS (SELECT 1 FROM raw."{CUSTOMERS}" c WHERE c."KUNNR(k/*)" = x."KUNNR(k/*)")'
                    if table_exists(cur, "raw", CUSTOMERS) else "TRUE")
    parts = [f"""
SELECT DISTINCT '{t}', x."KUNNR(k/*)",
       CASE WHEN EXISTS (SELECT 1 FROM raw."{DM}" d WHERE d."KUNNR(k/*)" = x."KUNNR(k/*)") AND {not_customer}
            THEN 'Codice [' || TRIM(x."KUNNR(k/*)") || '] presente solo come destinatario merci, non come cliente'
       END,
       x."_source"
FROM raw."{t}" x
WHERE x."_source" ILIKE '%ZBP-AddInterlocutoreCli%' AND COALESCE(TRIM(x."KUNNR(k/*)"), '') <> ''"""
             for t in TABLES if table_exists(cur, "raw", t)]
    return "\nUNION ALL".join(parts) or None


if __name__ == "__main__":
    run_query_check(check_id="CK426", sql=build_sql, required=[], category="BP")
