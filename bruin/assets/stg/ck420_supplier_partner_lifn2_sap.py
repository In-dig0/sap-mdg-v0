""" @bruin
name: stg.ck420_supplier_partner_lifn2_sap
type: python
depends:
  - stg.clean_check_results
description: >
  CK420 — CROSS_SOURCE: Partner dei fornitori (S_SUPPL_PARTNER#ZBP_Partner, DM06-06a, e
  S_SUPPL_PARTNER#ZBP_AddPartner, DM06-05a): il fornitore partner LIFN2(k), se diverso dal
  fornitore LIFNR(k/*), deve essere già in SAP (ref.SAP_EXPORT_BUT000, BU_SORT1). È il codice
  del fornitore in SAP (es. indirizzi di ordinazione BA creati con il flusso DM06-02b
  ZDM-Fornitori), quindi esportato senza trascodifica.
    - presente in BUT000: Ok;
    - assente: Warning, la riga non è esportata nello ZIP (prd.export_suppliers_zip, file
      <tabella>_discarded.csv), perché SAP la rifiuta (fornitore non esistente in LFA1).
  Un esito per riga con LIFN2 diverso da LIFNR. Saltato se BUT000 non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

PARTNER_TABLES = ["S_SUPPL_PARTNER#ZBP_Partner", "S_SUPPL_PARTNER#ZBP_AddPartner"]


def build_sql(cur):
    tables = [t for t in PARTNER_TABLES if table_exists(cur, "raw", t)]
    if not tables:
        return None
    return "\nUNION ALL\n".join(f"""
SELECT '{t}', p."LIFNR(k/*)",
       CASE WHEN b."PARTNER" IS NULL
            THEN 'Partner ' || TRIM(p."PARVW(k/*)") || ' LIFN2 [' || TRIM(p."LIFN2(k)")
                 || '] non presente in SAP (SAP_EXPORT_BUT000, BU_SORT1): riga non esportata'
       END,
       p."_source",
       'Partner ' || TRIM(p."PARVW(k/*)") || ' LIFN2 [' || TRIM(p."LIFN2(k)") || '] presente in SAP (BP '
           || COALESCE(b."PARTNER", '') || ')'
FROM raw."{t}" p
LEFT JOIN (SELECT DISTINCT ON (TRIM("BU_SORT1")) TRIM("BU_SORT1") AS sort1, "PARTNER"
           FROM ref."SAP_EXPORT_BUT000" ORDER BY TRIM("BU_SORT1"), "PARTNER") b
       ON b.sort1 = TRIM(p."LIFN2(k)")
WHERE COALESCE(TRIM(p."LIFN2(k)"), '') <> ''
  AND TRIM(p."LIFN2(k)") <> TRIM(p."LIFNR(k/*)")""" for t in tables)


if __name__ == "__main__":
    run_query_check(
        check_id="CK420",
        sql=build_sql,
        required=[("ref", "SAP_EXPORT_BUT000")],
        category="BP",
    )
