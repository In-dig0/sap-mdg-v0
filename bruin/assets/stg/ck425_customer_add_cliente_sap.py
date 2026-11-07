""" @bruin
name: stg.ck425_customer_add_cliente_sap
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK425 — CROSS_SOURCE: Dati dei clienti caricati dopo la creazione del cliente (03b dati
  aggiuntivi e interlocutori, 07a gestione credito): il cliente KUNNR(k/*) deve essere già in
  SAP (ref.SAP_EXPORT_BUT000, BU_SORT2), perché nello ZIP il codice è sostituito con quello SAP.
  Con l'esclusione dall'output attiva nel catalogo le righe non sono esportate
  (prd.export_clienti_add_zip, file discarded/). Un esito per cliente e tabella.
  Saltato se BUT000 non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

# tabella -> filtro sul nome del file ZIP del flusso
TABLES = {
    "S_CUST_GEN#AddDatiGenerali":       "%ZBP-AddInterlocutoreCli%",
    "S_CUST_CONT#ZBP-AddInterlocutore": "%ZBP-AddInterlocutoreCli%",
    "S_UKM_PROFILE#ZBP-Rischio":        "%ZBP-GestioneCredito%",
    "S_UKM_SEGMENT#ZBP-Fido":           "%ZBP-GestioneCredito%",
}


def build_sql(cur):
    parts = [f"""
SELECT DISTINCT '{t}', x."KUNNR(k/*)",
       CASE WHEN c.kunnr IS NULL
            THEN 'Cliente [' || TRIM(x."KUNNR(k/*)") || '] non ancora presente in SAP'
       END,
       x."_source"
FROM raw."{t}" x
LEFT JOIN customers c ON c.kunnr = TRIM(x."KUNNR(k/*)")
WHERE x."_source" ILIKE '{flow}' AND COALESCE(TRIM(x."KUNNR(k/*)"), '') <> ''"""
             for t, flow in TABLES.items() if table_exists(cur, "raw", t)]
    if not parts:
        return None
    return ("""WITH customers AS (SELECT DISTINCT TRIM("BU_SORT2") AS kunnr FROM ref."SAP_EXPORT_BUT000"
                   WHERE COALESCE(TRIM("BU_SORT2"), '') <> '')""" + "\nUNION ALL".join(parts))


if __name__ == "__main__":
    run_query_check(check_id="CK425", sql=build_sql, required=[("ref", "SAP_EXPORT_BUT000")], category="BP")
