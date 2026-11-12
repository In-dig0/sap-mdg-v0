""" @bruin
name: stg.ck534_custmat_cliente_in_sap
type: python
depends:
  - stg.clean_check_results
description: >
  CK534 — CROSS_SOURCE: Info record cliente-materiale (S_CUSTMAT#INFORMATFOR): il cliente
  CUSTOMER(k/*) deve essere già presente in SAP (ref.SAP_EXPORT_BUT000), come codice BP
  (PARTNER) oppure come chiave di ricerca (BU_SORT2, vecchio codice cliente/DM, poi BU_SORT1).
  Trovato con la chiave di ricerca: Ok, nel file per SAP il cliente è trascodificato con il
  codice BP (prd.export_custmat_zip). Errore se non è in BUT000 o se la chiave di ricerca
  corrisponde a più BP. Con l'esclusione dall'output attiva nel catalogo la riga non è
  esportata (file S_CUSTMAT_INFORMATFOR_discarded.xlsx).
  Confronto senza spazi iniziali e finali. Un esito per riga.
  Saltato se S_CUSTMAT o BUT000 non sono caricate.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, custmat_customer_sql, CUSTMAT_TABLE

KEY = ('x."SALESORGANIZATION(k/*)" || \'|\' || x."DISTRIBUTIONCHANNEL(k/*)" || \'|\' '
       '|| x."CUSTOMER(k/*)" || \'|\' || x."MATERIAL(k/*)"')

SQL = f"""
WITH cust AS ({custmat_customer_sql()})
SELECT '{CUSTMAT_TABLE}', {KEY},
       CASE
         WHEN c.n = 0 THEN
              'Cliente [' || c.cust || '] non presente in SAP (BUT000): né come codice BP né come chiave di ricerca'
         WHEN c.n > 1 THEN
              'Cliente [' || c.cust || '] corrisponde a più BP in SAP (BUT000, '
              || CASE c.via WHEN 2 THEN 'BU_SORT2' ELSE 'BU_SORT1' END || '): ' || c.partners
       END,
       x."_source",
       CASE WHEN c.via = 1 THEN 'Cliente [' || c.cust || '] presente in SAP'
            ELSE 'Cliente [' || c.cust || '] trascodificato con il BP SAP [' || c.partners || '] ('
                 || CASE c.via WHEN 2 THEN 'BU_SORT2' ELSE 'BU_SORT1' END || ')'
       END
FROM raw."{CUSTMAT_TABLE}" x
JOIN cust c ON c.cust = TRIM(x."CUSTOMER(k/*)")"""


if __name__ == "__main__":
    run_query_check(
        check_id="CK534",
        sql=SQL,
        required=[("raw", CUSTMAT_TABLE), ("ref", "SAP_EXPORT_BUT000")],
        category="CMIR",
    )
