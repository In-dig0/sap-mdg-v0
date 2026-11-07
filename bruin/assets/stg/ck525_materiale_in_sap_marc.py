""" @bruin
name: stg.ck525_materiale_in_sap_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK525 — CROSS_SOURCE: Codici doganali (S_MARITC): il materiale MATNR(k/*) deve essere già
  migrato in SAP nella divisione PLANT(k/*), cioè presente in ref.SAP_EXPORT_MARC (MATNR + WERKS).
  Il codice materiale SAP è uguale a quello del vecchio ERP (confronto senza spazi iniziali
  e finali). Assente: Warning, in SAP la migrazione andrebbe in errore.
  Con l'esclusione dall'output attiva nel catalogo la riga non è esportata
  (prd.export_maritc_zip, S_MARITC_discarded.xlsx). Gli altri flussi materiali sono
  controllati da CK527-CK531. Un esito per riga. Saltato se SAP_EXPORT_MARC non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, material_in_sap_sql

# tabella -> (object_key, materiale, divisione o None, filtro righe o None)
TABLES = {
    "S_MARITC": ('x."MATNR(k/*)" || \'|\' || x."PLANT(k/*)"',
                 'x."MATNR(k/*)"', 'x."PLANT(k/*)"', None),
}

if __name__ == "__main__":
    run_query_check(
        check_id="CK525",
        sql=lambda cur: material_in_sap_sql(cur, TABLES),
        required=[("ref", "SAP_EXPORT_MARC")],
        category="MAT",
    )
