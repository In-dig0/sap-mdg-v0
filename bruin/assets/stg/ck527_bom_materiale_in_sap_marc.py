""" @bruin
name: stg.ck527_bom_materiale_in_sap_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK527 — CROSS_SOURCE: Distinte base: il materiale padre (S_BOM_HEADER, MATNR(k/*) + WERKS(k))
  e ogni componente (S_BOM_ITEM, IDNRK + WERKS(k)) devono essere già migrati in SAP, cioè
  presenti in ref.SAP_EXPORT_MARC (MATNR + WERKS).
  Il codice materiale SAP è uguale a quello del vecchio ERP (confronto senza spazi iniziali
  e finali). Assente: Warning, in SAP la migrazione andrebbe in errore.
  Con l'esclusione dall'output attiva nel catalogo (default: disattivata) l'intera distinta
  (testata e tutte le posizioni) non è esportata (prd.export_bom_zip, file *_discarded.xlsx).
  Chiave: MATNR|WERKS|STLAN|STLAL (testata), più |ITMID (posizione). Un esito per riga.
  Saltato se SAP_EXPORT_MARC non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, material_in_sap_sql

# tabella -> (object_key, materiale, divisione o None, filtro righe o None)
TABLES = {
    "S_BOM_HEADER": ('x."MATNR(k/*)" || \'|\' || x."WERKS(k)" || \'|\' || x."STLAN(k/*)" || \'|\' || x."STLAL(k/*)"',
                     'x."MATNR(k/*)"', 'x."WERKS(k)"', None),
    "S_BOM_ITEM": ('x."MATNR(k/*)" || \'|\' || x."WERKS(k)" || \'|\' || x."STLAN(k/*)" || \'|\' || x."STLAL(k/*)"'
                   ' || \'|\' || x."ITMID(k/*)"',
                   'x."IDNRK"', 'x."WERKS(k)"', 'COALESCE(TRIM(x."IDNRK"), \'\') <> \'\''),
}

if __name__ == "__main__":
    run_query_check(
        check_id="CK527",
        sql=lambda cur: material_in_sap_sql(cur, TABLES),
        required=[("ref", "SAP_EXPORT_MARC")],
        category="BOM",
    )
