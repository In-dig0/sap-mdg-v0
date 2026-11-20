""" @bruin
name: prd.export_fornitori_conto_lavoro_xlsx
type: python
description: >
  Genera il file Excel Fornitori_ContoLavoro.xlsx con i fornitori di conto lavoro:
    - Pieno: fornitori degli info record acquisti di conto lavoro (DM07_MM-InfoAcq,
      S_EINE#INFORDATIACQ con ESOKZ = 3, fornitore LIFNR(*) di S_EINA#INFORMATFOR),
      trascodificati con il codice SAP come nell'export degli info record (BU_SORT1 di
      ref.SAP_EXPORT_BUT000 o, per i fornitori 05a, REF_KUNNR tramite BU_SORT2);
    - Fase: fornitori dei contratti (DM14_CONTRATTI, S_EKKO#CONTRATTI), LIFNR(*) già codice SAP
      (se non è in BUT000 si prova il codice legacy UNSEZ tramite BU_SORT1).
  Colonne: LIFNR (codice fornitore SAP), DESCRIZIONE (NAME_ORG1 + NAME_ORG2 di BUT000, per le
  persone fisiche nome e cognome), Conto lavoro pieno (SI/NO), Conto lavoro di fase (SI/NO):
  una riga per fornitore.
  I fornitori senza codice SAP sono esclusi e scritti negli scarti
  (discarded/Fornitori_ContoLavoro_discarded.xlsx). Senza BUT000 il file non viene creato.
  Il file ha data e ora di avvio del run nel nome, come gli ZIP (Fornitori_ContoLavoro_MDGOUT_
  YYYYMMDD_HHMI.xlsx): resta solo quello dell'ultimo run.
  Output: /project/datalake/out_source_mdg/excel/Fornitori_ContoLavoro_MDGOUT_<YYYYMMDD_HHMI>.xlsx
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
@bruin """

import os
import sys
import logging
import psycopg2
from openpyxl import Workbook

sys.path.insert(0, os.path.dirname(__file__))
from prd_output_lib import excel_output_path, remove_previous_excel
from prd_discard_lib import add_discards, flush_discards, reset_discards
from prd_export_inforecord_zip import get_lifnr_map

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

OUTPUT_DIR    = "/project/datalake/out_source_mdg"
OUTPUT_NAME   = "Fornitori_ContoLavoro.xlsx"   # nome base: in uscita con il suffisso _MDGOUT_ del run
HEADER        = ["LIFNR", "DESCRIZIONE", "Conto lavoro pieno", "Conto lavoro di fase"]
BUT000_TABLE  = "SAP_EXPORT_BUT000"
EINE_TABLE    = "S_EINE#INFORDATIACQ"
EINA_TABLE    = "S_EINA#INFORMATFOR"
CONTRACT_TABLE = "S_EKKO#CONTRATTI"
FULL, PHASE   = "Pieno", "Fase"
DISCARD_NAME  = "Fornitori_ContoLavoro"       # discarded/Fornitori_ContoLavoro_discarded.xlsx
DISCARD_COLS  = ["TIPO_CONTO_LAVORO", "FONTE", "CODICE FORNITORE", "CODICE LEGACY", "DOCUMENTI"]

# Conto lavoro pieno: fornitori (codice ERP) degli info record con ESOKZ = 3
SQL_FULL = f"""
    SELECT TRIM(a."LIFNR(*)"), COUNT(DISTINCT e."INFNR(k/*)")
    FROM raw."{EINE_TABLE}" e
    JOIN raw."{EINA_TABLE}" a ON TRIM(a."INFNR(k/*)") = TRIM(e."INFNR(k/*)")
    WHERE TRIM(e."ESOKZ(k/*)") = '3' AND TRIM(COALESCE(a."LIFNR(*)", '')) <> ''
    GROUP BY 1 ORDER BY 1
"""
# Conto lavoro di fase: fornitori (codice SAP e codice legacy UNSEZ) dei contratti
SQL_PHASE = f"""
    SELECT TRIM("LIFNR(*)"), MIN(TRIM(COALESCE("UNSEZ", ''))), COUNT(DISTINCT "LEGACY_NUMBER(k/*)")
    FROM raw."{CONTRACT_TABLE}"
    WHERE TRIM(COALESCE("LIFNR(*)", '')) <> ''
    GROUP BY 1 ORDER BY 1
"""
# Descrizione dei BP SAP
SQL_NAMES = f"""
    SELECT TRIM("PARTNER"),
           TRIM(CONCAT_WS(' ', NULLIF(TRIM("NAME_ORG1"), ''), NULLIF(TRIM("NAME_ORG2"), ''),
                               NULLIF(TRIM("NAME_FIRST"), ''), NULLIF(TRIM("NAME_LAST"), '')))
    FROM ref."{BUT000_TABLE}"
    WHERE TRIM(COALESCE("PARTNER", '')) <> ''
"""


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table))
    return cur.fetchone() is not None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Rimosso a ogni run: se oggi mancano i dati non deve restare il file di un run precedente
    remove_previous_excel(OUTPUT_NAME)
    reset_discards([DISCARD_NAME])

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            if not table_exists(cur, "ref", BUT000_TABLE):
                log.warning("ref.%s non presente — %s non generato.", BUT000_TABLE, OUTPUT_NAME)
                return
            cur.execute(SQL_NAMES)
            names = dict(cur.fetchall())
            full, phase = [], []
            if table_exists(cur, "raw", EINE_TABLE) and table_exists(cur, "raw", EINA_TABLE):
                cur.execute(SQL_FULL)
                full = cur.fetchall()
            else:
                log.warning("Info record acquisti non presenti — nessun fornitore di conto lavoro pieno.")
            if table_exists(cur, "raw", CONTRACT_TABLE):
                cur.execute(SQL_PHASE)
                phase = cur.fetchall()
            else:
                log.warning("raw.%s non presente — nessun fornitore di conto lavoro di fase.", CONTRACT_TABLE)
        lifnr_map = get_lifnr_map(conn) if full or phase else {}
    finally:
        conn.close()

    out, discarded = {}, []
    for lifnr, n_infnr in full:
        sap = lifnr_map.get(lifnr)
        if sap:
            out[(sap, FULL)] = names.get(sap, "")
        else:
            discarded.append((FULL, "DM07 info record (ESOKZ = 3)", lifnr, lifnr, f"{n_infnr} info record"))
    for lifnr, unsez, n_contracts in phase:
        sap = lifnr if lifnr in names else lifnr_map.get(unsez)
        if sap:
            out[(sap, PHASE)] = names.get(sap, "")
        else:
            discarded.append((PHASE, "DM14 contratti", lifnr, unsez, f"{n_contracts} contratti"))

    add_discards(DISCARD_NAME, DISCARD_COLS, discarded, "",
                 f"Fornitore senza codice SAP ({BUT000_TABLE}): né come BP né come chiave di ricerca BU_SORT1")
    flush_discards()
    if discarded:
        log.warning("  *** %d fornitori di conto lavoro senza codice SAP, esclusi: %s",
                    len(discarded), ", ".join(f"{d[2]} ({d[0]})" for d in discarded))
    if not out:
        log.warning("Nessun fornitore di conto lavoro con codice SAP — %s non generato.", OUTPUT_NAME)
        return

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Fornitori_ContoLavoro")
    ws.append(HEADER)
    flag = lambda sap, kind: "SI" if (sap, kind) in out else "NO"
    for sap in sorted({s for s, _ in out}):
        name = out.get((sap, FULL), out.get((sap, PHASE), ""))
        ws.append([sap, name, flag(sap, FULL), flag(sap, PHASE)])
    output_file = excel_output_path(OUTPUT_NAME)
    wb.save(output_file)
    n_full  = sum(1 for _, k in out if k == FULL)
    n_phase = sum(1 for _, k in out if k == PHASE)
    n_both  = len({s for s, k in out if k == FULL} & {s for s, k in out if k == PHASE})
    log.info("  ✓  %s  (%d fornitori: %d conto lavoro pieno, %d di fase, %d con entrambi; %d esclusi)",
             output_file, len({s for s, _ in out}), n_full, n_phase, n_both, len(discarded))


if __name__ == "__main__":
    main()
