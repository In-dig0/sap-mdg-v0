""" @bruin
name: stg.ck424_fornitori_cli_cliente_sap
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK424 — CROSS_SOURCE: Fornitori estesi da clienti (05a, S_SUPPL_GEN#AddDatiGenerali): il
  cliente di riferimento REF_KUNNR deve essere già in SAP (ref.SAP_EXPORT_BUT000, BU_SORT2),
  perché il fornitore è un'estensione del suo Business Partner. Con l'esclusione dall'output
  attiva nel catalogo il fornitore è escluso da tutto lo ZIP 05a (prd.export_suppliers_zip).
  Un esito per fornitore. Saltato se BUT000 non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

TABLE = "S_SUPPL_GEN#AddDatiGenerali"


def build_sql(cur):
    if not table_exists(cur, "raw", TABLE):
        return None
    return f"""
WITH customers AS (SELECT DISTINCT TRIM("BU_SORT2") AS kunnr FROM ref."SAP_EXPORT_BUT000"
                   WHERE COALESCE(TRIM("BU_SORT2"), '') <> '')
SELECT '{TABLE}', s."LIFNR(k/*)",
       CASE WHEN c.kunnr IS NULL
            THEN 'Cliente di riferimento [' || COALESCE(TRIM(s."REF_KUNNR"), '') || '] non presente in SAP: '
                 || 'il fornitore non può essere esteso'
       END,
       s."_source"
FROM raw."{TABLE}" s
LEFT JOIN customers c ON c.kunnr = TRIM(s."REF_KUNNR")"""


if __name__ == "__main__":
    run_query_check(check_id="CK424", sql=build_sql, required=[("ref", "SAP_EXPORT_BUT000")], category="BP")
