""" @bruin
name: stg.ck533_custmat_area_vendite_materiale
type: python
depends:
  - stg.clean_check_results
description: >
  CK533 — CROSS_SOURCE: Info record cliente-materiale (S_CUSTMAT#INFORMATFOR): la coppia
  organizzazione commerciale + canale (SALESORGANIZATION, DISTRIBUTIONCHANNEL) deve essere
  tra i dati di vendita del materiale (S_MVKE, PRODUCT + VKORG + VTWEG). Un materiale senza
  dati di vendita in S_MVKE è segnalato.
  Con l'esclusione dall'output attiva nel catalogo la riga non è esportata
  (prd.export_custmat_zip, file S_CUSTMAT_INFORMATFOR_discarded.xlsx).
  Confronto senza spazi iniziali e finali. Un esito per riga.
  Saltato se S_CUSTMAT o S_MVKE non sono caricate.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

CUSTMAT = "S_CUSTMAT#INFORMATFOR"
KEY = ('x."SALESORGANIZATION(k/*)" || \'|\' || x."DISTRIBUTIONCHANNEL(k/*)" || \'|\' '
       '|| x."CUSTOMER(k/*)" || \'|\' || x."MATERIAL(k/*)"')

SQL = f"""
WITH mvke AS (SELECT DISTINCT TRIM("PRODUCT(k/*)") AS matnr, TRIM("VKORG(k/*)") AS vkorg,
                     TRIM("VTWEG(k/*)") AS vtweg
              FROM raw."S_MVKE"),
     areas AS (SELECT matnr, string_agg(vkorg || '/' || vtweg, ', ' ORDER BY vkorg, vtweg) AS elenco
               FROM mvke GROUP BY matnr),
     x AS (SELECT c.*, TRIM(c."MATERIAL(k/*)") AS matnr, TRIM(c."SALESORGANIZATION(k/*)") AS vkorg,
                  TRIM(c."DISTRIBUTIONCHANNEL(k/*)") AS vtweg, TRIM(c."CUSTOMER(k/*)") AS cust
           FROM raw."{CUSTMAT}" c)
SELECT '{CUSTMAT}', {KEY},
       CASE
         WHEN a.matnr IS NULL THEN
              'Materiale [' || x.matnr || '] senza dati di vendita in anagrafica materiali (S_MVKE): '
              || 'area di vendita ' || x.vkorg || '/' || x.vtweg || ' non presente'
         WHEN m.matnr IS NULL THEN
              'Materiale [' || x.matnr || '], cliente [' || x.cust || ']: organizzazione commerciale/canale '
              || x.vkorg || '/' || x.vtweg || ' non presente nei dati di vendita del materiale ('
              || a.elenco || ')'
       END,
       x."_source",
       'Area di vendita ' || x.vkorg || '/' || x.vtweg || ' presente nei dati di vendita del materiale'
FROM x
LEFT JOIN areas a ON a.matnr = x.matnr
LEFT JOIN mvke  m ON m.matnr = x.matnr AND m.vkorg = x.vkorg AND m.vtweg = x.vtweg"""


if __name__ == "__main__":
    run_query_check(
        check_id="CK533",
        sql=SQL,
        required=[("raw", CUSTMAT), ("raw", "S_MVKE")],
        category="CMIR",
    )
