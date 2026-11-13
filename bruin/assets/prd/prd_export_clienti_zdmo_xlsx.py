""" @bruin
name: prd.export_clienti_zdmo_xlsx
type: python
description: >
  Genera il file Excel Clienti_ZDMO.xlsx con i clienti a cui assegnare in SAP il messaggio
  fattura ZDMO: clienti di S_CUST_SALES_DATA#ZBP-DatiVendite con INCO3_L = 'E' (in almeno
  un'area di vendita). Colonne: KUNNR (codice cliente SAP), KSCHL (messaggio fattura = ZDMO).
  KUNNR è il codice BP SAP (PARTNER di ref.SAP_EXPORT_BUT000 con BU_SORT2 = codice cliente ERP):
  i clienti non presenti in SAP o con più BP sono esclusi e scritti negli scarti
  (discarded/Clienti_ZDMO_discarded.xlsx). Senza BUT000 il file non viene creato.
  Nello ZIP clienti per SAP il campo INCO3_L viene svuotato (prd.export_clienti_zip).
  Il file ha data e ora di avvio del run nel nome, come gli ZIP (Clienti_ZDMO_MDGOUT_
  YYYYMMDD_HHMI.xlsx): resta solo quello dell'ultimo run. Se mancano i dati di vendita
  o nessun cliente ha INCO3_L = 'E' non viene creato.
  Output: /project/datalake/out_source_mdg/Clienti_ZDMO_MDGOUT_<YYYYMMDD_HHMI>.xlsx
depends:
  - prd.merge_dati_vendite
  - ingestion.ingest_xlsx_to_ref
@bruin """

import os
import sys
import logging
import psycopg2
from openpyxl import Workbook

sys.path.insert(0, os.path.dirname(__file__))
from prd_output_lib import output_zip_path, remove_previous_zips
from prd_discard_lib import add_discards, flush_discards, reset_discards

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

OUTPUT_DIR   = "/project/datalake/out_source_mdg"
OUTPUT_NAME  = "Clienti_ZDMO.xlsx"   # nome base: in uscita con il suffisso _MDGOUT_ del run
SALES_TABLE  = "S_CUST_SALES_DATA#ZBP-DatiVendite"
FLAG_VALUE   = "E"      # INCO3_L che richiede il messaggio fattura
MESSAGE_TYPE = "ZDMO"   # KSCHL assegnato
HEADER       = ["KUNNR", "KSCHL"]
BUT000_TABLE = "SAP_EXPORT_BUT000"
DISCARD_NAME = "Clienti_ZDMO"                   # discarded/Clienti_ZDMO_discarded.xlsx
DISCARD_COLS = ["KUNNR (codice ERP)", "BP SAP", "KSCHL"]

# Clienti con INCO3_L = 'E' e BP SAP corrispondenti (BU_SORT2 = codice cliente ERP)
SQL_ROWS = f"""
    WITH cli AS (
        SELECT DISTINCT TRIM("KUNNR(k/*)") AS kunnr
        FROM prd."{SALES_TABLE}"
        WHERE UPPER(TRIM(COALESCE("INCO3_L", ''))) = %s
          AND TRIM(COALESCE("KUNNR(k/*)", '')) <> ''
    )
    SELECT c.kunnr,
           ARRAY_REMOVE(ARRAY_AGG(DISTINCT TRIM(b."PARTNER") ORDER BY TRIM(b."PARTNER")), NULL)
    FROM cli c
    LEFT JOIN ref."{BUT000_TABLE}" b ON TRIM(b."BU_SORT2") = c.kunnr
                                    AND TRIM(COALESCE(b."PARTNER", '')) <> ''
    GROUP BY c.kunnr
    ORDER BY 1
"""


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table))
    return cur.fetchone() is not None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Rimosso a ogni run: se oggi mancano i dati non deve restare il file di un run precedente
    remove_previous_zips(OUTPUT_DIR, OUTPUT_NAME)
    reset_discards([DISCARD_NAME])

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for schema, table in (("prd", SALES_TABLE), ("ref", BUT000_TABLE)):
                if not table_exists(cur, schema, table):
                    log.warning("%s.%s non presente — %s non generato.", schema, table, OUTPUT_NAME)
                    return
            cur.execute(SQL_ROWS, (FLAG_VALUE,))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        log.warning("Nessun cliente con INCO3_L = '%s' — %s non generato.", FLAG_VALUE, OUTPUT_NAME)
        return

    customers  = [(kunnr, partners[0]) for kunnr, partners in rows if len(partners) == 1]
    not_in_sap = [(kunnr, "", MESSAGE_TYPE) for kunnr, partners in rows if not partners]
    ambiguous  = [(kunnr, ", ".join(partners), MESSAGE_TYPE) for kunnr, partners in rows if len(partners) > 1]
    add_discards(DISCARD_NAME, DISCARD_COLS, not_in_sap, "",
                 f"Cliente non presente in SAP ({BUT000_TABLE}, BU_SORT2): messaggio {MESSAGE_TYPE} "
                 "da assegnare dopo il caricamento del cliente")
    add_discards(DISCARD_NAME, DISCARD_COLS, ambiguous, "",
                 f"Cliente associato a più BP SAP ({BUT000_TABLE}, BU_SORT2)")
    flush_discards()
    if not_in_sap:
        log.warning("  *** %d clienti con INCO3_L = '%s' non presenti in SAP, esclusi: %s",
                    len(not_in_sap), FLAG_VALUE, ", ".join(r[0] for r in not_in_sap))
    if ambiguous:
        log.warning("  *** %d clienti con più BP SAP, esclusi: %s",
                    len(ambiguous), "; ".join(f"{r[0]} -> {r[1]}" for r in ambiguous))
    if not customers:
        log.warning("Nessun cliente con INCO3_L = '%s' presente in SAP — %s non generato.",
                    FLAG_VALUE, OUTPUT_NAME)
        return

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Clienti_ZDMO")
    ws.append(HEADER)
    for _, partner in sorted(customers, key=lambda c: c[1]):
        ws.append([partner, MESSAGE_TYPE])
    output_file = output_zip_path(OUTPUT_DIR, OUTPUT_NAME)
    wb.save(output_file)
    log.info("  ✓  %s  (%d clienti con INCO3_L = '%s' e codice BP SAP, KSCHL = %s; %d esclusi)",
             output_file, len(customers), FLAG_VALUE, MESSAGE_TYPE, len(not_in_sap) + len(ambiguous))


if __name__ == "__main__":
    main()
