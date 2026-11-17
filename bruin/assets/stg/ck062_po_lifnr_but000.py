""" @bruin
name: stg.ck062_po_lifnr_but000
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK062 — SAP_REF: Ordini di acquisto (S_EKKO): il fornitore LIFNR(*) deve avere il codice SAP in
  SAP_EXPORT_BUT000. LIFNR(*) è il codice del vecchio ERP: si trascodifica con la stessa regola
  degli info record acquisti (BU_SORT1 dei BP fornitori ZBP/ZIC oppure, per i fornitori del flusso
  05a, REF_KUNNR tramite BU_SORT2); è accettato anche un codice che sia già un BP SAP (PARTNER).
  Nel file per SAP LIFNR(*) diventa il BP SAP (prd.export_ordini_acquisto_zip); se non è
  trascodificabile l'ordine è esportato con il codice originale (solo segnalazione).
  Saltato se SAP_EXPORT_BUT000 non è caricata. Un esito per ordine.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

CLI_SUPPLIER_TABLE = "S_SUPPL_GEN#AddDatiGenerali"   # flusso 05a: fornitori estesi da cliente
# Solo BP dei gruppi dei fornitori (come CK526 e la trascodifica dell'export): non i destinatari merci ZDM
BU_GROUP_OK = """TRIM(b."BU_GROUP") IN ('ZBP', 'ZIC')"""


def build_sql(cur):
    via_customer = ""
    if table_exists(cur, "raw", CLI_SUPPLIER_TABLE):
        via_customer = f"""
        UNION ALL
        SELECT TRIM(a."LIFNR(k/*)"), TRIM(b."PARTNER")
        FROM raw."{CLI_SUPPLIER_TABLE}" a
        JOIN ref."SAP_EXPORT_BUT000" b ON TRIM(b."BU_SORT2") = TRIM(a."REF_KUNNR")
        WHERE {BU_GROUP_OK}"""
    return f"""
WITH m AS (
    SELECT old, MIN(partner) AS partner FROM (
        SELECT TRIM(b."BU_SORT1") AS old, TRIM(b."PARTNER") AS partner
        FROM ref."SAP_EXPORT_BUT000" b
        WHERE COALESCE(TRIM(b."BU_SORT1"), '') <> '' AND {BU_GROUP_OK}{via_customer}
    ) t GROUP BY old
)
SELECT 'S_EKKO', k."EBELN(k/*)",
       CASE
           WHEN COALESCE(TRIM(k."LIFNR(*)"), '') = '' THEN 'LIFNR(*) vuoto'
           WHEN p.partner IS NULL AND m.partner IS NULL
               THEN 'Fornitore LIFNR(*) [' || TRIM(k."LIFNR(*)") || '] senza codice SAP in SAP_EXPORT_BUT000 '
                    || '(né BP né chiave di ricerca BU_SORT1 di un fornitore ZBP/ZIC)'
       END,
       k."_source",
       CASE WHEN p.partner IS NOT NULL THEN 'Fornitore [' || p.partner || '] già codice BP SAP'
            WHEN m.partner IS NOT NULL THEN 'Fornitore [' || TRIM(k."LIFNR(*)") || '] -> BP SAP [' || m.partner || ']'
       END
FROM raw."S_EKKO" k
LEFT JOIN (SELECT DISTINCT TRIM("PARTNER") AS partner FROM ref."SAP_EXPORT_BUT000") p
       ON p.partner = TRIM(k."LIFNR(*)")
LEFT JOIN m ON m.old = TRIM(k."LIFNR(*)")
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK062",
        sql=build_sql,
        required=[("raw", "S_EKKO"), ("ref", "SAP_EXPORT_BUT000")],
    )
