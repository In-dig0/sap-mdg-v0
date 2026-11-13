""" @bruin
name: prd.export_clienti_zdmo_xlsx
type: python
description: >
  Genera il file Excel Clienti_ZDMO.xlsx con i clienti a cui assegnare in SAP il messaggio
  fattura ZDMO: clienti di S_CUST_SALES_DATA#ZBP-DatiVendite con INCO3_L = 'E' (in almeno
  un'area di vendita). Colonne: KUNNR (codice cliente), KSCHL (messaggio fattura = ZDMO).
  Nello ZIP clienti per SAP il campo INCO3_L viene svuotato (prd.export_clienti_zip).
  Il file ha data e ora di avvio del run nel nome, come gli ZIP (Clienti_ZDMO_MDGOUT_
  YYYYMMDD_HHMI.xlsx): resta solo quello dell'ultimo run. Se mancano i dati di vendita
  o nessun cliente ha INCO3_L = 'E' non viene creato.
  Output: /project/datalake/out_source_mdg/Clienti_ZDMO_MDGOUT_<YYYYMMDD_HHMI>.xlsx
depends:
  - prd.merge_dati_vendite
@bruin """

import os
import sys
import logging
import psycopg2
from openpyxl import Workbook

sys.path.insert(0, os.path.dirname(__file__))
from prd_output_lib import output_zip_path, remove_previous_zips

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

SQL_ROWS = f"""
    SELECT DISTINCT TRIM("KUNNR(k/*)")
    FROM prd."{SALES_TABLE}"
    WHERE UPPER(TRIM(COALESCE("INCO3_L", ''))) = %s
      AND TRIM(COALESCE("KUNNR(k/*)", '')) <> ''
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

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            if not table_exists(cur, "prd", SALES_TABLE):
                log.warning("prd.%s non presente — %s non generato.", SALES_TABLE, OUTPUT_NAME)
                return
            cur.execute(SQL_ROWS, (FLAG_VALUE,))
            customers = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not customers:
        log.warning("Nessun cliente con INCO3_L = '%s' — %s non generato.", FLAG_VALUE, OUTPUT_NAME)
        return

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Clienti_ZDMO")
    ws.append(HEADER)
    for kunnr in customers:
        ws.append([kunnr, MESSAGE_TYPE])
    output_file = output_zip_path(OUTPUT_DIR, OUTPUT_NAME)
    wb.save(output_file)
    log.info("  ✓  %s  (%d clienti con INCO3_L = '%s', KSCHL = %s)",
             output_file, len(customers), FLAG_VALUE, MESSAGE_TYPE)


if __name__ == "__main__":
    main()
