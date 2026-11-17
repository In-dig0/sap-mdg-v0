""" @bruin
name: stg.ck535_custmat_chiave_duplicata
type: python
depends:
  - stg.clean_check_results
description: >
  CK535 — DUPLICATE: Info record cliente-materiale (S_CUSTMAT#INFORMATFOR): la chiave
  SALESORGANIZATION + DISTRIBUTIONCHANNEL + CUSTOMER + MATERIAL deve essere univoca nel file.
  L'ingestione carica solo la prima riga di ogni chiave: le righe con la stessa chiave si leggono
  dal file in ingresso (ZIP del flusso in in_source_pprod) e il messaggio elenca tutti i codici
  materiale del cliente (MATERIALBYCUSTOMER) trovati, da cui l'ERP deve scegliere quello giusto.
  Errore sulla riga caricata in raw; con l'esclusione dall'output attiva nel catalogo la chiave
  non è esportata (file S_CUSTMAT_INFORMATFOR_discarded.xlsx) finché il file non viene corretto.
  Ok se la chiave è già in SAP (ref.SAP_EXPORT_KNMT, il messaggio riporta il MATERIALBYCUSTOMER di SAP):
  non viene comunque esportata (prd.export_custmat_zip, report/<ZIP>_gia_in_SAP.xlsx).
  Confronto senza spazi iniziali e finali. Un esito per riga.
  Saltato se S_CUSTMAT non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import (run_query_check, table_exists, custmat_duplicates, custmat_in_sap_sql,
                          CUSTMAT_TABLE, CUSTMAT_KNMT, CUSTMAT_DUP_TABLE, CUSTMAT_OBJECT_KEY)


def build_sql(cur):
    n = custmat_duplicates(cur)
    print(f"[CK535] chiavi duplicate nei file in ingresso: {n}")
    in_sap = (custmat_in_sap_sql()
              if table_exists(cur, "ref", CUSTMAT_KNMT) and table_exists(cur, "ref", "SAP_EXPORT_BUT000")
              else "SELECT NULL::text AS src, NULL::text AS object_key, NULL::text AS kdmat WHERE FALSE")
    return f"""
WITH s AS ({in_sap})
SELECT '{CUSTMAT_TABLE}', {CUSTMAT_OBJECT_KEY},
       CASE WHEN d.src IS NOT NULL AND s.src IS NULL THEN
            'Chiave duplicata nel file: ' || d.n || ' righe per organizzazione commerciale/canale ['
            || d.vkorg || '/' || d.vtweg || '], cliente [' || d.cust || '], materiale [' || d.matnr
            || '] con codice materiale del cliente (MATERIALBYCUSTOMER) ' || d.variants
            || ': lasciare una sola riga'
       END,
       x."_source",
       CASE WHEN d.src IS NOT NULL THEN
            'Chiave duplicata nel file (' || d.n || ' righe), info record già in SAP con '
            || 'MATERIALBYCUSTOMER [' || s.kdmat || '] in SAP'
       END
FROM raw."{CUSTMAT_TABLE}" x
LEFT JOIN {CUSTMAT_DUP_TABLE} d
       ON d.src = x."_source"
      AND d.vkorg = TRIM(x."SALESORGANIZATION(k/*)") AND d.vtweg = TRIM(x."DISTRIBUTIONCHANNEL(k/*)")
      AND d.cust = TRIM(x."CUSTOMER(k/*)") AND d.matnr = TRIM(x."MATERIAL(k/*)")
LEFT JOIN s ON s.src = x."_source" AND s.object_key = {CUSTMAT_OBJECT_KEY}"""


if __name__ == "__main__":
    run_query_check(
        check_id="CK535",
        sql=build_sql,
        required=[("raw", CUSTMAT_TABLE)],
        category="CMIR",
    )
