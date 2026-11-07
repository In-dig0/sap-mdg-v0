""" @bruin
name: stg.ck526_inforecord_lifnr_sap
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK526 — CROSS_SOURCE: Info record (S_EINA#INFORMATFOR): il fornitore LIFNR(*) deve avere un
  codice SAP in ref.SAP_EXPORT_BUT000, tramite BU_SORT1 oppure, per i fornitori del flusso 05a
  (BP creato come cliente), tramite REF_KUNNR di S_SUPPL_GEN#AddDatiGenerali e BU_SORT2.
  Assente: Warning, l'info record non è esportato nello ZIP (prd.export_inforecord_zip, file
  <tabella>_discarded.xlsx), perché SAP lo rifiuta.
  Un esito per info record. Saltato se BUT000 non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

CLI_SUPPLIER_TABLE = "S_SUPPL_GEN#AddDatiGenerali"


def build_sql(cur):
    via_customer = ""
    if table_exists(cur, "raw", CLI_SUPPLIER_TABLE):
        via_customer = f"""
    OR EXISTS (SELECT 1 FROM raw."{CLI_SUPPLIER_TABLE}" a
               JOIN ref."SAP_EXPORT_BUT000" b ON TRIM(b."BU_SORT2") = TRIM(a."REF_KUNNR")
               WHERE TRIM(a."LIFNR(k/*)") = TRIM(e."LIFNR(*)"))"""
    return f"""
SELECT 'S_EINA#INFORMATFOR', e."INFNR(k/*)",
       CASE WHEN NOT (
           EXISTS (SELECT 1 FROM ref."SAP_EXPORT_BUT000" b WHERE TRIM(b."BU_SORT1") = TRIM(e."LIFNR(*)")){via_customer}
       ) THEN 'Fornitore LIFNR(*) [' || COALESCE(TRIM(e."LIFNR(*)"), '')
              || '] senza codice SAP in SAP_EXPORT_BUT000: info record non esportato'
       END,
       e."_source"
FROM raw."S_EINA#INFORMATFOR" e"""


if __name__ == "__main__":
    run_query_check(
        check_id="CK526",
        sql=build_sql,
        required=[("raw", "S_EINA#INFORMATFOR"), ("ref", "SAP_EXPORT_BUT000")],
        category="PIR",
    )
