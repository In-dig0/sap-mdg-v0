""" @bruin
name: prd.export_clienti_add_zip
type: python
description: >
  Flussi clienti che contengono solo tabelle raw (nessun merge in prd):
    - ZBP-GestioneCredito: profilo di rischio e fido (credit management).
      Un ZIP per ogni file di origine (_source il cui nome contiene il flusso),
      con lo stesso nome del file in ingresso. I CSV usano separatore ";" e non
      includono colonne audit (stesso formato di prd.export_clienti_zip).
    - ZBP-AddInterlocutoreCli: ZIP con dati generali aggiuntivi e interlocutori
      (email) di clienti e destinatari merci, da caricare in SAP in un secondo
      momento. I punti di scarico sono esportati negli ZIP delle anagrafiche
      (prd.export_clienti_zip e prd.export_dest_merci_zip): qui solo i loro scarti.
  Estrazione ristretta (test): se stg.export_filter contiene codici cliente, vengono
  generati anche gli ZIP dei soli clienti indicati in out_source_mdg/test.
  KUNNR(k/*) (codice del vecchio ERP) è trascodificato con il codice SAP (PARTNER di
  ref.SAP_EXPORT_BUT000, tramite BU_SORT2): questi ZIP si caricano quando clienti e DM
  esistono già in SAP. Le righe senza corrispondenza sono escluse e scritte in
  <tabella>_discarded.csv.
  I clienti di stg.export_exclude_add (interlocutori e credito già caricati in SAP) sono
  esclusi: riepilogo in <nome ZIP>_gia_in_SAP.csv; se non resta nulla lo ZIP non è generato.
  Le righe di clienti assenti dall'anagrafica (raw S_CUST_GEN#ZBP-DatiGenerali
  e S_CUST_GEN#ZDM-DatiGenerali) sono escluse e scritte in <tabella>_discarded.csv.
  ZBP-AddInterlocutoreCli: esclusi (negli scarti) anche i codici presenti solo come
  destinatari merci (ZDM e non ZBP). Negli interlocutori (S_CUST_CONT#ZBP-AddInterlocutore)
  le email E_MAIL_B/E_MAIL sono in minuscolo; COUNTRY e REGION vengono dall'anagrafica
  prd."S_CUST_GEN#ZBP_DatiGenerali" (per il Brasile, COUNTRY = BR, anche il CAP POSTLCD); VNAME/LNAME = NAME_FIRST/NAME_LAST per le persone
  fisiche, LNAME = NAMORG1 (VNAME vuoto) per le società.
  Gli ZIP di output (e i relativi riepiloghi) di un flusso senza file caricati in raw, ad
  esempio perché l'ERP l'ha estratto vuoto, vengono rimossi: non restano quelli di run precedenti.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
  - prd.merge_clienti
@bruin """

import os
import io
import sys
import zipfile
import logging

sys.path.insert(0, os.path.dirname(__file__))
from prd_export_clienti_zip import (
    OUTPUT_DIR, get_connection, q, get_columns, table_exists,
    fetch_table_csv, write_discarded_csv, remove_discarded_csv, ADD_FLOW, ADD_MOVED_TABLES,
    get_export_filter, write_test_zip, get_but000_map,
    get_exclude_add, already_in_sap_rows, write_already_in_sap, write_output_zip,
    TEST_DIR, already_in_sap_path,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

KEY_COL = "KUNNR(k/*)"

# Flusso (contenuto nel nome del file ZIP) -> tabelle raw del flusso, in ordine
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
CUSTOMER_TABLE, DM_TABLE = MASTER_TABLES

# Interlocutori clienti: dati presi dall'anagrafica clienti in prd
CONTACT_TABLE   = "S_CUST_CONT#ZBP-AddInterlocutore"
PRD_CUSTOMER    = "S_CUST_GEN#ZBP_DatiGenerali"
EMAIL_COLS      = ["E_MAIL_B", "E_MAIL"]


def get_dm_only_keys(conn, source_name: str, tables: list[str]) -> set:
    """Codici del flusso presenti solo come destinatari merci (ZDM e non ZBP)."""
    if not table_exists(conn, "raw", DM_TABLE):
        return set()
    not_customer = (f'AND NOT EXISTS (SELECT 1 FROM raw.{q(CUSTOMER_TABLE)} c WHERE c.{q(KEY_COL)} = x.{q(KEY_COL)})'
                    if table_exists(conn, "raw", CUSTOMER_TABLE) else "")
    keys = set()
    with conn.cursor() as cur:
        for table in tables:
            if not table_exists(conn, "raw", table) or KEY_COL not in get_columns(conn, "raw", table):
                continue
            cur.execute(
                f'SELECT DISTINCT x.{q(KEY_COL)} FROM raw.{q(table)} x '
                f'WHERE x."_source" = %s '
                f'AND EXISTS (SELECT 1 FROM raw.{q(DM_TABLE)} d WHERE d.{q(KEY_COL)} = x.{q(KEY_COL)}) '
                f'{not_customer}',
                (source_name,),
            )
            keys.update(r[0] for r in cur.fetchall() if r[0])
    return keys


def get_customer_data(conn) -> dict:
    """Vecchio codice cliente -> (NAME_FIRST, NAME_LAST, NAMORG1, COUNTRY, REGION, POST_CODE1) da prd."""
    if not table_exists(conn, "prd", PRD_CUSTOMER):
        log.warning(f"  prd.{PRD_CUSTOMER} non trovata — interlocutori senza dati anagrafici")
        return {}
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {q(KEY_COL)}, '
            f'COALESCE(TRIM("NAME_FIRST"), \'\'), COALESCE(TRIM("NAME_LAST"), \'\'), '
            f'COALESCE(TRIM("NAMORG1"), \'\'), COALESCE(TRIM("COUNTRY(*)"), \'\'), '
            f'COALESCE(TRIM("REGION"), \'\'), COALESCE(TRIM("POST_CODE1"), \'\') '
            f'FROM prd.{q(PRD_CUSTOMER)}'
        )
        return {r[0]: r[1:] for r in cur.fetchall()}


def contact_row_fn(customers: dict):
    """Interlocutori: email in minuscolo; nome, paese, regione (e CAP per BR) dall'anagrafica cliente."""
    def fn(cols: list, row: list):
        for col in EMAIL_COLS:
            if col in cols and row[cols.index(col)]:
                row[cols.index(col)] = row[cols.index(col)].strip().lower()
        data = customers.get(row[cols.index(KEY_COL)])
        if not data:
            return
        first, last, org1, country, region, post_code = data
        if first or last:                      # persona fisica
            vname, lname = first, last
        else:                                  # società
            vname, lname = "", org1
        for col, val in (("VNAME", vname), ("LNAME", lname), ("COUNTRY", country), ("REGION", region)):
            if col in cols:
                row[cols.index(col)] = val
        if country == "BR" and "POSTLCD" in cols:   # Brasile: CAP obbligatorio nell'indirizzo
            row[cols.index("POSTLCD")] = post_code
    return fn

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


def write_discards(conn, source_name: str, tables: list[str]):
    """Solo scarti (righe di clienti assenti dall'anagrafica) per tabelle esportate altrove."""
    orphan_keys = get_orphan_keys(conn, source_name, tables)
    if not orphan_keys:
        log.info("  Tutti i clienti di '%s' sono presenti in anagrafica.", source_name)
        return
    log.warning(
        "  *** %d clienti assenti dall'anagrafica per '%s' — righe scritte negli scarti: %s",
        len(orphan_keys), source_name, sorted(orphan_keys),
    )
    for raw_table in tables:
        if table_exists(conn, "raw", raw_table):
            write_discarded_csv(conn, raw_table, source_name,
                                get_columns(conn, "raw", raw_table), orphan_keys, KEY_COL)


def build_zip(conn, source_name: str, tables: list[str], key_filter: set | None = None,
              excluded_out: list | None = None) -> bytes:
    """
    key_filter:   codici cliente dell'estrazione ristretta (None = output completo).
    excluded_out: se passata, riceve le righe escluse perché i dati sono già caricati in SAP.
    """
    buf = io.BytesIO()
    # Clienti e DM già caricati in SAP: KUNNR con il nuovo codice (BUT000, BU_SORT2)
    sap_codes = get_but000_map(conn, sort_cols=("BU_SORT2",))
    # Clienti con questi dati già caricati in SAP (elenco manuale): esclusi dallo ZIP
    done_map  = {c: sap_codes.get(c, "") for c in get_exclude_add(conn)}
    excluded  = []

    orphan_keys = get_orphan_keys(conn, source_name, tables)
    if orphan_keys:
        log.warning(
            "  *** %d clienti assenti dall'anagrafica per '%s' "
            "— le loro righe verranno escluse: %s",
            len(orphan_keys), source_name, sorted(orphan_keys),
        )
    else:
        log.info("  Tutti i clienti di '%s' sono presenti in anagrafica.", source_name)

    contact_fn = None
    if ADD_FLOW in source_name:
        # Solo veri clienti: esclusi (negli scarti) i codici che sono solo destinatari merci
        dm_keys = get_dm_only_keys(conn, source_name, tables)
        if dm_keys:
            log.warning("  *** %d destinatari merci (non clienti) esclusi da '%s': %s",
                        len(dm_keys), source_name, sorted(dm_keys))
            orphan_keys |= dm_keys
        contact_fn = contact_row_fn(get_customer_data(conn))

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for raw_table in tables:
            if not table_exists(conn, "raw", raw_table):
                log.warning(f"  SKIP raw.{raw_table} — tabella non trovata")
                continue
            raw_cols = get_columns(conn, "raw", raw_table)
            if not raw_cols:
                log.warning(f"  SKIP raw.{raw_table} — nessuna colonna")
                continue

            if orphan_keys and not key_filter:
                write_discarded_csv(conn, raw_table, source_name, raw_cols, orphan_keys, KEY_COL)
            excluded += already_in_sap_rows(conn, "raw", raw_table, source_name, KEY_COL, done_map)
            csv_bytes = fetch_table_csv(
                conn, "raw", raw_table, source_name, raw_cols,
                excluded_keys=(orphan_keys | set(done_map)) or None, fk_col=KEY_COL,
                key_filter=(KEY_COL, key_filter) if key_filter else None,
                write_discards=not key_filter,
                value_map={KEY_COL: sap_codes},
                drop_if_unmapped=[KEY_COL],
                row_fn=contact_fn if raw_table == CONTACT_TABLE else None,
            )
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info(f"  raw.{raw_table}: 0 righe per '{source_name}' — skip")
                continue
            zf.writestr(f"{raw_table}.csv", csv_bytes)
            log.info(f"  + raw.{raw_table}: {n_rows} righe")

    if excluded_out is not None:
        excluded_out.extend(excluded)
    buf.seek(0)
    return buf.read()


def remove_stale_outputs(flow: str, sources: list[str]):
    """Rimuove gli ZIP del flusso (output e test) che non corrispondono a un file caricato in raw."""
    for out_dir in (OUTPUT_DIR, TEST_DIR):
        if not os.path.isdir(out_dir):
            continue
        for name in os.listdir(out_dir):
            if name.lower().endswith(".zip") and flow.lower() in name.lower() and name not in sources:
                os.remove(os.path.join(out_dir, name))
                log.info(f"  Rimosso {os.path.join(out_dir, name)}: nessun file del flusso caricato in raw")
                if out_dir == OUTPUT_DIR and os.path.exists(already_in_sap_path(name)):
                    os.remove(already_in_sap_path(name))


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    remove_discarded_csv([t for tables in FLOWS.values() for t in tables])
    conn = get_connection()
    try:
        for flow, tables in FLOWS.items():
            sources = get_flow_sources(conn, flow, tables)
            remove_stale_outputs(flow, sources)
            if not sources:
                log.info(f"Flusso {flow}: nessun file caricato in raw — nulla da esportare.")
                continue
            # Tabelle esportate negli ZIP delle anagrafiche (punti di scarico): qui solo gli scarti
            zip_tables = [t for t in tables if t not in ADD_MOVED_TABLES] if flow == ADD_FLOW else tables
            for source_name in sources:
                if flow == ADD_FLOW:
                    write_discards(conn, source_name, ADD_MOVED_TABLES)
                log.info(f"=== Generazione ZIP: {source_name} (flusso {flow}) ===")
                excluded  = []
                zip_bytes = build_zip(conn, source_name, zip_tables, excluded_out=excluded)
                write_output_zip(source_name, zip_bytes)
                write_already_in_sap(source_name, excluded)
                # Estrazione ristretta (test): solo i clienti di stg.export_filter
                customers = get_export_filter(conn)
                write_test_zip(source_name, lambda: build_zip(conn, source_name, zip_tables, key_filter=customers),
                               customers, "clienti")
    finally:
        conn.close()

    log.info("=== export_clienti_add_zip completato ===")


if __name__ == "__main__":
    main()
