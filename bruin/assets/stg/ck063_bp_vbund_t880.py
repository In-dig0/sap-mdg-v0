""" @bruin
name: stg.ck063_bp_vbund_t880
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK063 — SAP_REF: Business Partner (DatiGenerali di clienti, DM, fornitori): la società partner
  VBUND, se valorizzata, deve essere presente in SAP_EXPORT_T880 (RCOMP). Sono controllate solo
  le tabelle caricate; saltato se SAP_EXPORT_T880 non è caricata. Un esito per BP con VBUND.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

# Tabelle DatiGenerali con VBUND: {tabella: colonna chiave}
TABLES = {
    "S_CUST_GEN#ZBP-DatiGenerali":  "KUNNR(k/*)",
    "S_CUST_GEN#ZDM-DatiGenerali":  "KUNNR(k/*)",
    "S_SUPPL_GEN#ZBP_DatiGenerali": "LIFNR(k/*)",
    "S_SUPPL_GEN#ZDM_DatiGenerali": "LIFNR(k/*)",
}


def build_sql(cur):
    parts = [f"""
        SELECT '{table}', g."{key}",
               CASE WHEN NOT EXISTS (SELECT 1 FROM ref."SAP_EXPORT_T880" t WHERE TRIM(t."RCOMP") = TRIM(g."VBUND"))
                    THEN 'Società partner VBUND [' || TRIM(g."VBUND") || '] non presente in SAP_EXPORT_T880'
               END,
               g."_source"
        FROM raw."{table}" g
        WHERE COALESCE(TRIM(g."VBUND"), '') <> ''"""
             for table, key in TABLES.items() if table_exists(cur, "raw", table)]
    return "\nUNION ALL".join(parts)


if __name__ == "__main__":
    run_query_check(
        check_id="CK063",
        sql=build_sql,
        required=[("ref", "SAP_EXPORT_T880")],
        category="BP",
    )
