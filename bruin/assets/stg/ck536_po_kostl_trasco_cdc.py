""" @bruin
name: stg.ck536_po_kostl_trasco_cdc
type: python
depends:
  - stg.clean_check_results
description: >
  CK536 — CROSS_SOURCE: Ordini di acquisto, imputazioni (S_EKKN): il centro di costo KOSTL
  (codice DBCOGE) deve avere la trascodifica SAP in ref.SAP_TRASCO_CDC, con COD_DBCOGE = KOSTL e
  DIV = WERKS della posizione (S_EKPO, stessi EBELN e EBELP). Nel file per SAP KOSTL diventa
  COD_SAP_DEF (prd.export_ordini_acquisto_zip).
  Errore se la posizione non esiste in S_EKPO, se manca il record di trascodifica, se COD_SAP_DEF
  è vuoto o non definito ('??') o se la coppia ha più codici SAP diversi. Solo segnalazione:
  l'ordine è comunque esportato con il KOSTL originale di DBCOGE.
  Confronto senza spazi iniziali e finali; imputazioni senza
  KOSTL escluse dal controllo. Un esito per imputazione (EBELN|EBELP|ZEKKN).
  Saltato se S_EKKN o SAP_TRASCO_CDC non sono caricate.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, po_cdc_sql, PO_ACCOUNT_TABLE, PO_ITEM_TABLE, CDC_TRASCO

SQL = f"""
SELECT '{PO_ACCOUNT_TABLE}', object_key,
       CASE
         WHEN werks IS NULL THEN
              'Centro di costo KOSTL [' || kostl || '] non trascodificabile: posizione non trovata in '
              || '{PO_ITEM_TABLE}, divisione sconosciuta'
         WHEN n_rec = 0 THEN
              'Centro di costo KOSTL [' || kostl || '] della divisione [' || werks
              || '] senza record di trascodifica in {CDC_TRASCO} (COD_DBCOGE + DIV)'
         WHEN n_sap = 0 THEN
              'Centro di costo KOSTL [' || kostl || '] della divisione [' || werks
              || '] senza codice SAP in {CDC_TRASCO} (COD_SAP_DEF vuoto o non definito)'
         WHEN n_sap > 1 THEN
              'Centro di costo KOSTL [' || kostl || '] della divisione [' || werks
              || '] con più codici SAP in {CDC_TRASCO}: ' || sap_list
       END,
       src,
       CASE WHEN sap IS NOT NULL THEN 'KOSTL [' || kostl || '] -> [' || sap || ']' END
FROM ({po_cdc_sql()}) c
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK536",
        sql=SQL,
        required=[("raw", PO_ACCOUNT_TABLE), ("raw", PO_ITEM_TABLE), ("ref", CDC_TRASCO)],
        category="PO",
    )
