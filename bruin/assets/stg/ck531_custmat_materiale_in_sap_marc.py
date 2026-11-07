""" @bruin
name: stg.ck531_custmat_materiale_in_sap_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK531 — CROSS_SOURCE: Info record cliente-materiale (S_CUSTMAT#INFORMATFOR): il materiale
  MATERIAL(k/*) deve essere già migrato in SAP nella divisione PLANT (solo il materiale se
  PLANT è vuota) (ref.SAP_EXPORT_MARC).
  Il codice materiale SAP è uguale a quello del vecchio ERP (confronto senza spazi iniziali
  e finali). Assente: Warning, in SAP la migrazione andrebbe in errore.
  Solo segnalazione: MDG non genera lo ZIP degli info record cliente.
  Un esito per riga. Saltato se SAP_EXPORT_MARC non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, material_in_sap_sql

# tabella -> (object_key, materiale, divisione o None, filtro righe o None)
TABLES = {
    "S_CUSTMAT#INFORMATFOR": ('x."SALESORGANIZATION(k/*)" || \'|\' || x."DISTRIBUTIONCHANNEL(k/*)" || \'|\' '
                              '|| x."CUSTOMER(k/*)" || \'|\' || x."MATERIAL(k/*)"',
                              'x."MATERIAL(k/*)"', 'NULLIF(TRIM(x."PLANT"), \'\')', None),
}

if __name__ == "__main__":
    run_query_check(
        check_id="CK531",
        sql=lambda cur: material_in_sap_sql(cur, TABLES),
        required=[("ref", "SAP_EXPORT_MARC")],
        category="CMIR",
    )
