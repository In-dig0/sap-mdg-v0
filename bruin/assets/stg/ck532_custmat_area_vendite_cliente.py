""" @bruin
name: stg.ck532_custmat_area_vendite_cliente
type: python
depends:
  - stg.clean_check_results
description: >
  CK532 — CROSS_SOURCE: Info record cliente-materiale (S_CUSTMAT#INFORMATFOR): la coppia
  organizzazione commerciale + canale (SALESORGANIZATION, DISTRIBUTIONCHANNEL) deve essere
  tra le aree di vendita del cliente nei dati di vendita di clienti
  (S_CUST_SALES_DATA#ZBP-DatiVendite) o destinatari merci (S_CUST_SALES_DATA#ZDM-DatiVendite),
  VKORG + VTWEG.
  Il BP SAP del cliente è quello di CK534 (CUSTOMER come codice BP o chiave di ricerca in
  ref.SAP_EXPORT_BUT000); il vecchio codice ERP (KUNNR dei dati di vendita) è il suo BU_SORT2.
  Un cliente senza dati di vendita è segnalato (area di vendita non verificabile). Le righe
  con cliente non in SAP o ambiguo non hanno esito: le segnala CK534.
  Con l'esclusione dall'output attiva nel catalogo la riga non è esportata
  (prd.export_custmat_zip, file S_CUSTMAT_INFORMATFOR_discarded.xlsx).
  Confronto senza spazi iniziali e finali. Un esito per riga.
  Saltato se S_CUSTMAT o BUT000 non sono caricate.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists, custmat_customer_sql

CUSTMAT = "S_CUSTMAT#INFORMATFOR"
SALES   = ("S_CUST_SALES_DATA#ZBP-DatiVendite", "S_CUST_SALES_DATA#ZDM-DatiVendite")
KEY = ('x."SALESORGANIZATION(k/*)" || \'|\' || x."DISTRIBUTIONCHANNEL(k/*)" || \'|\' '
       '|| x."CUSTOMER(k/*)" || \'|\' || x."MATERIAL(k/*)"')


def check_sql(cur) -> str:
    sales = [f'SELECT TRIM("KUNNR(k/*)") AS kunnr, TRIM("VKORG(k/*)") AS vkorg, TRIM("VTWEG(k/*)") AS vtweg '
             f'FROM raw."{t}"' for t in SALES if table_exists(cur, "raw", t)]
    if not sales:
        sales = ["SELECT NULL::text AS kunnr, NULL::text AS vkorg, NULL::text AS vtweg WHERE FALSE"]
    return f"""
WITH cust AS ({custmat_customer_sql()}),
     but000 AS (SELECT c.cust, c.partners AS partner, NULLIF(TRIM(MIN(b."BU_SORT2")), '') AS kunnr
                FROM cust c JOIN ref."SAP_EXPORT_BUT000" b ON TRIM(b."PARTNER") = c.partners
                WHERE c.n = 1 GROUP BY c.cust, c.partners),
     sales AS ({" UNION ".join(sales)}),
     areas AS (SELECT kunnr, string_agg(vkorg || '/' || vtweg, ', ' ORDER BY vkorg, vtweg) AS elenco
               FROM sales GROUP BY kunnr),
     x AS (SELECT c.*, TRIM(c."CUSTOMER(k/*)") AS cust, TRIM(c."SALESORGANIZATION(k/*)") AS vkorg,
                  TRIM(c."DISTRIBUTIONCHANNEL(k/*)") AS vtweg, TRIM(c."MATERIAL(k/*)") AS matnr
           FROM raw."{CUSTMAT}" c)
SELECT '{CUSTMAT}', {KEY},
       CASE
         WHEN a.kunnr IS NULL THEN
              'Cliente [' || x.cust || '] (codice ERP ' || COALESCE(b.kunnr, '-')
              || ') senza dati di vendita in anagrafica clienti o destinatari merci: area di vendita '
              || x.vkorg || '/' || x.vtweg || ' non verificabile'
         WHEN NOT EXISTS (SELECT 1 FROM sales s WHERE s.kunnr = b.kunnr AND s.vkorg = x.vkorg AND s.vtweg = x.vtweg) THEN
              'Materiale [' || x.matnr || '], cliente [' || x.cust || '] (codice ERP ' || b.kunnr
              || '): organizzazione commerciale/canale ' || x.vkorg || '/' || x.vtweg
              || ' non presente nei dati di vendita del cliente (' || a.elenco || ')'
       END,
       x."_source",
       'Area di vendita ' || x.vkorg || '/' || x.vtweg || ' presente nei dati di vendita del cliente'
FROM x
JOIN but000 b ON b.cust = x.cust
LEFT JOIN areas  a ON a.kunnr = b.kunnr"""


if __name__ == "__main__":
    run_query_check(
        check_id="CK532",
        sql=check_sql,
        required=[("raw", CUSTMAT), ("ref", "SAP_EXPORT_BUT000")],
        category="CMIR",
    )
