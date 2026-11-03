""" @bruin
name: prd.export_gruppo_prodotti_xlsx
type: python
description: >
  Genera il file Excel GruppoProdotti.xlsx con il gruppo prodotti di ogni materiale:
  S_MARA."EXTWG" = SAP_TRASCO_GRUPPO_PRODOTTI."CodTipologia".
  Colonne: MATNR (codice materiale senza spazi), Cod_TipoGP, GruppoProdotti.
  I materiali con EXTWG vuoto sono esclusi; gli EXTWG assenti dalla trascodifica
  sono segnalati nel log (e i relativi materiali esclusi).
  Il file viene riscritto a ogni run; se manca S_MARA o la trascodifica non viene creato.
  Output: /project/datalake/out_source_mdg/GruppoProdotti.xlsx
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
@bruin """

import os
import logging
import psycopg2
from openpyxl import Workbook

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

OUTPUT_DIR  = "/project/datalake/out_source_mdg"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "GruppoProdotti.xlsx")
MARA_TABLE  = "S_MARA"
TRASCO      = "SAP_TRASCO_GRUPPO_PRODOTTI"
HEADER      = ["MATNR", "Cod_TipoGP", "GruppoProdotti"]

# Materiali con EXTWG valorizzato e, se presente, la trascodifica (NULL se assente)
SQL_ROWS = f"""
    SELECT TRIM(m."PRODUCT(k/*)") AS matnr, TRIM(m."EXTWG") AS extwg,
           TRIM(t."Cod_TipoGP"), TRIM(t."GruppoProdotti")
    FROM raw."{MARA_TABLE}" m
    LEFT JOIN (SELECT DISTINCT ON (TRIM("CodTipologia")) * FROM ref."{TRASCO}"
               ORDER BY TRIM("CodTipologia")) t
           ON TRIM(t."CodTipologia") = TRIM(m."EXTWG")
    WHERE TRIM(COALESCE(m."EXTWG", '')) <> ''
    ORDER BY 1
"""


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table))
    return cur.fetchone() is not None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Rimosso a ogni run: se oggi mancano i dati non deve restare il file di un run precedente
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for schema, table in (("raw", MARA_TABLE), ("ref", TRASCO)):
                if not table_exists(cur, schema, table):
                    log.warning("%s.%s non presente — GruppoProdotti.xlsx non generato.", schema, table)
                    return
            cur.execute(SQL_ROWS)
            rows = cur.fetchall()
    finally:
        conn.close()

    missing = sorted({r[1] for r in rows if r[2] is None})
    if missing:
        log.warning("  *** EXTWG assenti in %s (%d codici, materiali esclusi): %s",
                    TRASCO, len(missing), ", ".join(missing))
    out_rows = [(r[0], r[2], r[3]) for r in rows if r[2] is not None]
    if not out_rows:
        log.warning("Nessun materiale con gruppo prodotti — GruppoProdotti.xlsx non generato.")
        return

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("GruppoProdotti")
    ws.append(HEADER)
    for row in out_rows:
        ws.append(row)
    wb.save(OUTPUT_FILE)
    log.info("  ✓  %s  (%d materiali; %d con EXTWG senza trascodifica)",
             OUTPUT_FILE, len(out_rows), len(rows) - len(out_rows))


if __name__ == "__main__":
    main()
