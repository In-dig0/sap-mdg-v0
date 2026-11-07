""" @bruin
name: stg.ck528_inforecord_materiale_in_sap_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK528 — CROSS_SOURCE: Info record acquisti (S_EINA#INFORMATFOR): il materiale MATNR deve
  essere già migrato in SAP, in qualunque divisione (ref.SAP_EXPORT_MARC, MATNR).
  Il codice materiale SAP è uguale a quello del vecchio ERP (confronto senza spazi iniziali
  e finali). Assente: Warning, in SAP la migrazione andrebbe in errore.
  Con l'esclusione dall'output attiva nel catalogo (default: disattivata) l'info record non
  è esportato in nessuna delle quattro tabelle (prd.export_inforecord_zip, file *_discarded.xlsx).
  Info record senza materiale esclusi dal controllo. Un esito per info record.
  Saltato se SAP_EXPORT_MARC non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, material_in_sap_sql

# tabella -> (object_key, materiale, divisione o None, filtro righe o None)
TABLES = {
    "S_EINA#INFORMATFOR": ('x."INFNR(k/*)"', 'x."MATNR"', None, 'COALESCE(TRIM(x."MATNR"), \'\') <> \'\''),
}

if __name__ == "__main__":
    run_query_check(
        check_id="CK528",
        sql=lambda cur: material_in_sap_sql(cur, TABLES),
        required=[("ref", "SAP_EXPORT_MARC")],
        category="PIR",
    )
