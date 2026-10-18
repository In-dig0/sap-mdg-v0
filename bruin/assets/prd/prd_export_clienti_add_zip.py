""" @bruin
name: prd.export_clienti_add_zip
type: python
description: >
  Genera gli archivi ZIP di output per i flussi clienti che contengono solo
  tabelle raw (nessun merge in prd):
    - ZBP-AddInterlocutoreCli: interlocutori aggiuntivi, dati generali
      aggiuntivi, punti di scarico
    - ZBP-GestioneCredito: profilo di rischio e fido (credit management)
  Un ZIP per ogni file di origine (_source il cui nome contiene il flusso),
  con lo stesso nome del file in ingresso. I CSV usano separatore ";" e non
  includono colonne audit (stesso formato di prd.export_clienti_zip).
  Le righe di clienti assenti dall'anagrafica (raw S_CUST_GEN#ZBP-DatiGenerali
  e S_CUST_GEN#ZDM-DatiGenerali) sono escluse e scritte in <tabella>_discarded.csv.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
depends:
  - ingestion.ingest_zip_to_raw
@bruin """

import os
import io
import sys
import zipfile
import logging

sys.path.insert(0, os.path.dirname(__file__))
from prd_export_clienti_zip import (
    OUTPUT_DIR, get_connection, q, get_columns, table_exists,
    fetch_table_csv, write_discarded_csv,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

KEY_COL = "KUNNR(k/*)"

# Flusso (contenuto nel nome del file ZIP) -> tabelle raw da esportare, in ordine
FLOWS = {
    "ZBP-AddInterlocutoreCli": [
        "S_CUST_GEN#AddDatiGenerali",
        "S_CUST_CONT#ZBP-AddInterlocutore",
        "S_UNL_POINT#ZBP-AddPuntoScarico",
    ],
    "ZBP-GestioneCredito": [
        "S_UKM_PROFILE#ZBP-Rischio",
        "S_UKM_SEGMENT#ZBP-Fido",
    ],
}

# Anagrafiche clienti: una riga dei flussi aggiuntivi è valida solo se il
# cliente esiste in almeno una di queste tabelle
MASTER_TABLES = ["S_CUST_GEN#ZBP-DatiGenerali", "S_CUST_GEN#ZDM-DatiGenerali"]


def get_flow_sources(conn, flow: str, tables: list[str]) -> list[str]:
    """File ZIP di origine (_source) del flusso presenti nelle sue tabelle raw."""
    sources = set()
    with conn.cursor() as cur:
        for table in tables:
            if not table_exists(conn, "raw", table):
                continue
            cur.execute(
                f'SELECT DISTINCT "_source" FROM raw.{q(table)} '
                f'WHERE "_source" ILIKE %s',
                (f"%{flow}%",),
            )
            sources.update(r[0] for r in cur.fetchall() if r[0])
    return sorted(sources)


def get_orphan_keys(conn, source_name: str, tables: list[str]) -> set:
    """Clienti presenti nelle tabelle del flusso ma in nessuna anagrafica clienti."""
    masters = [t for t in MASTER_TABLES if table_exists(conn, "raw", t)]
    not_in_master = " AND ".join(
        f'NOT EXISTS (SELECT 1 FROM raw.{q(m)} mst WHERE mst.{q(KEY_COL)} = x.{q(KEY_COL)})'
        for m in masters
    ) or "TRUE"
    orphans = set()
    with conn.cursor() as cur:
        for table in tables:
            if not table_exists(conn, "raw", table) or KEY_COL not in get_columns(conn, "raw", table):
                continue
            cur.execute(
                f'SELECT DISTINCT x.{q(KEY_COL)} FROM raw.{q(table)} x '
                f'WHERE x."_source" = %s AND {not_in_master}',
                (source_name,),
            )
            orphans.update(r[0] for r in cur.fetchall() if r[0])
    return orphans


def build_zip(conn, source_name: str, tables: list[str]) -> bytes:
    buf = io.BytesIO()

    orphan_keys = get_orphan_keys(conn, source_name, tables)
    if orphan_keys:
        log.warning(
            "  *** %d clienti assenti dall'anagrafica per '%s' "
            "— le loro righe verranno escluse: %s",
            len(orphan_keys), source_name, sorted(orphan_keys),
        )
    else:
        log.info("  Tutti i clienti di '%s' sono presenti in anagrafica.", source_name)

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for raw_table in tables:
            if not table_exists(conn, "raw", raw_table):
                log.warning(f"  SKIP raw.{raw_table} — tabella non trovata")
                continue
            raw_cols = get_columns(conn, "raw", raw_table)
            if not raw_cols:
                log.warning(f"  SKIP raw.{raw_table} — nessuna colonna")
                continue

            if orphan_keys:
                write_discarded_csv(conn, raw_table, source_name, raw_cols, orphan_keys, KEY_COL)
            csv_bytes = fetch_table_csv(
                conn, "raw", raw_table, source_name, raw_cols,
                excluded_keys=orphan_keys or None, fk_col=KEY_COL,
            )
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info(f"  raw.{raw_table}: 0 righe per '{source_name}' — skip")
                continue
            zf.writestr(f"{raw_table}.csv", csv_bytes)
            log.info(f"  + raw.{raw_table}: {n_rows} righe")

    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_connection()
    try:
        for flow, tables in FLOWS.items():
            sources = get_flow_sources(conn, flow, tables)
            if not sources:
                log.info(f"Flusso {flow}: nessun file caricato in raw — nulla da esportare.")
                continue
            for source_name in sources:
                log.info(f"=== Generazione ZIP: {source_name} (flusso {flow}) ===")
                zip_bytes   = build_zip(conn, source_name, tables)
                output_path = os.path.join(OUTPUT_DIR, source_name)
                with open(output_path, "wb") as f:
                    f.write(zip_bytes)
                log.info(f"  ✓  {output_path}  ({len(zip_bytes) / 1024:.1f} KB)")
    finally:
        conn.close()

    log.info("=== export_clienti_add_zip completato ===")


if __name__ == "__main__":
    main()
