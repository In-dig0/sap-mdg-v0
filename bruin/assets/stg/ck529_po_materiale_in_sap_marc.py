""" @bruin
name: stg.ck529_po_materiale_in_sap_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK529 — CROSS_SOURCE: Ordini di acquisto (S_EKPO): il materiale MATNR deve essere già
  migrato in SAP nella divisione WERKS (ref.SAP_EXPORT_MARC, MATNR + WERKS).
  Il codice materiale SAP è uguale a quello del vecchio ERP (confronto senza spazi iniziali
  e finali). Assente: Warning, in SAP la migrazione andrebbe in errore.
  Solo segnalazione: MDG non genera lo ZIP degli ordini. Posizioni senza materiale escluse
  dal controllo. Un esito per posizione. Saltato se SAP_EXPORT_MARC non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, material_in_sap_sql

# tabella -> (object_key, materiale, divisione o None, filtro righe o None)
TABLES = {
    "S_EKPO": ('x."EBELN(k/*)" || \'|\' || x."EBELP(k/*)"',
               'x."MATNR"', 'x."WERKS"', 'COALESCE(TRIM(x."MATNR"), \'\') <> \'\''),
}

if __name__ == "__main__":
    run_query_check(
        check_id="CK529",
        sql=lambda cur: material_in_sap_sql(cur, TABLES),
        required=[("ref", "SAP_EXPORT_MARC")],
        category="PO",
    )
