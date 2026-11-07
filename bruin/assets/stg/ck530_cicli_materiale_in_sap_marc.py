""" @bruin
name: stg.ck530_cicli_materiale_in_sap_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK530 — CROSS_SOURCE: Cicli di lavoro (S_MAPL): il materiale MATNR(k/*) deve essere già
  migrato in SAP nella divisione WERKS_MAT(k/*) (ref.SAP_EXPORT_MARC, MATNR + WERKS).
  Il codice materiale SAP è uguale a quello del vecchio ERP (confronto senza spazi iniziali
  e finali). Assente: Warning, in SAP la migrazione andrebbe in errore.
  Solo segnalazione (esclusione dallo ZIP dei cicli non ancora implementata).
  Un esito per riga. Saltato se SAP_EXPORT_MARC non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, material_in_sap_sql

# tabella -> (object_key, materiale, divisione o None, filtro righe o None)
TABLES = {
    "S_MAPL": ('x."PLNNR(k/*)" || \'|\' || x."PLNAL(k/*)" || \'|\' || x."MATNR(k/*)" || \'|\' || x."WERKS_MAT(k/*)"',
               'x."MATNR(k/*)"', 'x."WERKS_MAT(k/*)"', None),
}

if __name__ == "__main__":
    run_query_check(
        check_id="CK530",
        sql=lambda cur: material_in_sap_sql(cur, TABLES),
        required=[("ref", "SAP_EXPORT_MARC")],
        category="ROUTING",
    )
