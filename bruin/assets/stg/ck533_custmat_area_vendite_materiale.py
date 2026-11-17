""" @bruin
name: stg.ck533_custmat_area_vendite_materiale
type: python
depends:
  - stg.clean_check_results
description: >
  CK533 — CROSS_SOURCE: Info record cliente-materiale (S_CUSTMAT#INFORMATFOR): la coppia
  organizzazione commerciale + canale (SALESORGANIZATION, DISTRIBUTIONCHANNEL) deve essere
  tra i dati di vendita del materiale:
    - ref.SAP_EXPORT_MVKE (MATNR + VKORG + VTWEG), dati di vendita già caricati in SAP, se la
      tabella è caricata e contiene righe;
    - altrimenti raw.S_MVKE (PRODUCT + VKORG + VTWEG) del file materiali in ingresso.
  Un materiale senza dati di vendita nella fonte usata è segnalato; il messaggio indica la fonte.
  Con l'esclusione dall'output attiva nel catalogo la riga non è esportata
  (prd.export_custmat_zip, file S_CUSTMAT_INFORMATFOR_discarded.xlsx).
  Confronto senza spazi iniziali e finali. Un esito per riga.
  Saltato se S_CUSTMAT non è caricata o se mancano sia SAP_EXPORT_MVKE sia S_MVKE.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

CUSTMAT = "S_CUSTMAT#INFORMATFOR"
KEY = ('x."SALESORGANIZATION(k/*)" || \'|\' || x."DISTRIBUTIONCHANNEL(k/*)" || \'|\' '
       '|| x."CUSTOMER(k/*)" || \'|\' || x."MATERIAL(k/*)"')

# fonte dei dati di vendita: (schema, tabella, materiale, org. commerciale, canale, descrizione)
SOURCES = [
    ("ref", "SAP_EXPORT_MVKE", "MATNR", "VKORG", "VTWEG", "dati di vendita in SAP (SAP_EXPORT_MVKE)"),
    ("raw", "S_MVKE", "PRODUCT(k/*)", "VKORG(k/*)", "VTWEG(k/*)", "anagrafica materiali (S_MVKE)"),
]


def pick_source(cur):
    """Prima fonte caricata e con righe: SAP_EXPORT_MVKE, altrimenti S_MVKE."""
    for source in SOURCES:
        schema, table = source[:2]
        if table_exists(cur, schema, table):
            cur.execute(f'SELECT EXISTS (SELECT 1 FROM {schema}."{table}")')
            if cur.fetchone()[0]:
                return source
            print(f"[CK533] {schema}.{table} vuota")
    return None


def build_sql(cur):
    source = pick_source(cur)
    if not source:
        return None
    schema, table, matnr, vkorg, vtweg, label = source
    print(f"[CK533] dati di vendita del materiale da {schema}.{table}")
    return f"""
WITH mvke AS (SELECT DISTINCT TRIM("{matnr}") AS matnr, TRIM("{vkorg}") AS vkorg, TRIM("{vtweg}") AS vtweg
              FROM {schema}."{table}"),
     areas AS (SELECT matnr, string_agg(vkorg || '/' || vtweg, ', ' ORDER BY vkorg, vtweg) AS elenco
               FROM mvke GROUP BY matnr),
     x AS (SELECT c.*, TRIM(c."MATERIAL(k/*)") AS matnr, TRIM(c."SALESORGANIZATION(k/*)") AS vkorg,
                  TRIM(c."DISTRIBUTIONCHANNEL(k/*)") AS vtweg, TRIM(c."CUSTOMER(k/*)") AS cust
           FROM raw."{CUSTMAT}" c)
SELECT '{CUSTMAT}', {KEY},
       CASE
         WHEN a.matnr IS NULL THEN
              'Materiale [' || x.matnr || '] senza {label}: '
              || 'area di vendita ' || x.vkorg || '/' || x.vtweg || ' non presente'
         WHEN m.matnr IS NULL THEN
              'Materiale [' || x.matnr || '], cliente [' || x.cust || ']: organizzazione commerciale/canale '
              || x.vkorg || '/' || x.vtweg || ' non presente nei {label} ('
              || a.elenco || ')'
       END,
       x."_source",
       'Area di vendita ' || x.vkorg || '/' || x.vtweg || ' presente nei {label}'
FROM x
LEFT JOIN areas a ON a.matnr = x.matnr
LEFT JOIN mvke  m ON m.matnr = x.matnr AND m.vkorg = x.vkorg AND m.vtweg = x.vtweg"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK533",
        sql=build_sql,
        required=[("raw", CUSTMAT)],
        category="CMIR",
    )
