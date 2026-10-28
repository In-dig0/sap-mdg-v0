""" @bruin
name: stg.ck417_supplier_vbund_akont
type: python
depends:
  - stg.clean_check_results
description: >
  CK417 — CROSS_TABLE: Fornitori: coerenza tra società partner VBUND (S_SUPPL_GEN#ZBP_DatiGenerali)
  e conto di riconciliazione AKONT(*) (S_SUPPL_COMPANY#ZBP_DatiSocieta e, se caricata,
  S_SUPPL_COMPANY#ZBP_AddDatiSocieta). Con VBUND valorizzato il conto deve essere intercompany
  (elenco IC_ACCOUNTS); con conto intercompany VBUND è obbligatorio. Un esito per fornitore/società.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

# Conti di riconciliazione fornitori ammessi per le società partner (debiti v/società del gruppo)
IC_ACCOUNTS = (
    "0024090001", "0024090021", "0024100001", "0024100021",
    "0024110001", "0024110011", "0024110021", "0024110121",
    "0024115001", "0024115021",
)
IC_SQL = ", ".join(f"'{a}'" for a in IC_ACCOUNTS)

COMPANY_TABLES = ["S_SUPPL_COMPANY#ZBP_DatiSocieta", "S_SUPPL_COMPANY#ZBP_AddDatiSocieta"]


def build_sql(cur):
    parts = [f"""
        SELECT '{table}', s."LIFNR(k/*)" || '/' || s."BUKRS(k/*)",
               CASE
                   WHEN x.vbund <> '' AND x.akont NOT IN ({IC_SQL})
                       THEN 'Società partner VBUND [' || x.vbund || '] con conto di riconciliazione AKONT(*) ['
                            || x.akont || '] non intercompany'
                   WHEN x.vbund = '' AND x.akont IN ({IC_SQL})
                       THEN 'Conto di riconciliazione intercompany AKONT(*) [' || x.akont
                            || '] senza società partner VBUND'
               END,
               s."_source"
        FROM raw."{table}" s
        JOIN raw."S_SUPPL_GEN#ZBP_DatiGenerali" g ON TRIM(g."LIFNR(k/*)") = TRIM(s."LIFNR(k/*)")
        CROSS JOIN LATERAL (
            SELECT COALESCE(TRIM(g."VBUND"), '') AS vbund, COALESCE(TRIM(s."AKONT(*)"), '') AS akont
        ) x"""
             for table in COMPANY_TABLES if table_exists(cur, "raw", table)]
    return "\nUNION ALL".join(parts)


if __name__ == "__main__":
    run_query_check(
        check_id="CK417",
        sql=build_sql,
        required=[("raw", "S_SUPPL_GEN#ZBP_DatiGenerali")],
        category="BP",
    )
