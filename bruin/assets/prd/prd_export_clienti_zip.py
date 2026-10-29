""" @bruin
name: prd.export_clienti_zip
type: python
description: >
  Genera gli archivi ZIP di output per i clienti combinando:
    - prd."S_CUST_GEN#ZBP_DatiGenerali"  (dati anagrafici con indirizzi normalizzati)
    - tutte le altre tabelle clienti da schema raw
    - punti di scarico dei clienti, entrati con il flusso ZBP-AddInterlocutoreCli
      (gli interlocutori restano nello ZIP di quel flusso: si caricano in un secondo momento)
  In S_CUST_SALES_PARTNER#ZBP-Partner la funzione partner RA è scritta come SP
  (spedizioniere) e il partner KUNN2(k) è trascodificato con il
  nuovo codice SAP (PARTNER di ref.SAP_EXPORT_BUT000): destinatari merci tramite
  BU_SORT2, spedizionieri tramite BU_SORT1. Senza corrispondenza resta il codice originale.
  Non sono trascodificati i riferimenti al cliente stesso (KUNN2 = KUNNR): il file crea il
  cliente con il codice originale.
  Le righe partner senza alcun riferimento (KUNN2, LIFNR, PERNR, PARNR vuoti) sono escluse
  e scritte negli scarti; il file è ordinato per le colonne chiave.
  Le righe partner con funzione di fornitore (RA spedizioniere, ZA) il cui KUNN2 non è un
  fornitore creato in SAP (BU_SORT1 di ref.SAP_EXPORT_BUT000; se la tabella non è caricata,
  i vettori di raw S_SUPPL_GEN#ZBP_DatiGenerali) sono escluse e scritte negli scarti:
  in SAP darebbero errore (fornitore non esistente in LFA1).
  In S_CUST_TAXNUMBERS#ZBP-CodiciFisc le righe senza identificativo fiscale (TAXNUM vuoto)
  sono escluse e scritte negli scarti (in SAP danno errore).
  Estrazione ristretta (test): se stg.export_filter contiene codici cliente, viene
  generato anche lo ZIP con i soli clienti indicati in out_source_mdg/test.
  I clienti già presenti in SAP (ref.SAP_EXPORT_BUT000, BU_SORT2) sono esclusi da tutte le
  tabelle: riepilogo in <nome ZIP>_gia_in_SAP.csv; se non resta nulla lo ZIP non è generato.
  Un ZIP separato viene creato per ogni valore distinto di _source
  trovato in prd."S_CUST_GEN#ZBP_DatiGenerali".
  I CSV dentro il ZIP usano separatore ";" e non includono colonne audit.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
depends:
  - ingestion.ingest_xlsx_to_ref
  - prd.merge_clienti
  - prd.merge_taxnumbers_clienti
  - prd.merge_dati_vendite
@bruin """

import os
import io
import csv
import zipfile
import logging
import math
import psycopg2

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

OUTPUT_DIR = "/project/datalake/out_source_mdg"

AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}

# Tabelle da schema prd che sostituiscono le rispettive versioni raw.
# L'ordine determina la sequenza di scrittura nel ZIP.
PRD_TABLES = [
    ("prd", "S_CUST_GEN#ZBP_DatiGenerali"),
    ("prd", "S_CUST_TAXNUMBERS#ZBP-CodiciFisc"),
    ("prd", "S_CUST_BANK_DATA#ZBP-AppoggioBanca"),
]

# Nome del CSV nello ZIP per le tabelle prd il cui nome differisce dall'originale:
# l'output deve avere gli stessi nomi del file in ingresso (usati dal caricamento in SAP)
PRD_CSV_NAME = {
    "S_CUST_GEN#ZBP_DatiGenerali": "S_CUST_GEN#ZBP-DatiGenerali",
}

# Tabelle da raw (filtrate per _source)
RAW_TABLES = [
    "S_CUST_BANK_DATA#ZBP-AppoggioBanca",    # sostituita da prd (filtro CK048)
    "S_CUST_COMPANY#ZBP-DatiSocieta",
    "S_CUST_GEN#ZBP-DatiGenerali",            # sostituita da prd
    "S_CUST_SALES_DATA#ZBP-DatiVendite",
    "S_CUST_SALES_PARTNER#ZBP-Partner",
    "S_CUST_TAXCLASS#ZBP-IvaVendite",
    "S_CUST_TAXNUMBERS#ZBP-CodiciFisc",        # sostituita da prd
    "S_ROLES#ZBP-RuoliClienti",
]

# Mappa: nome raw → tabella prd sostitutiva
RAW_REPLACED_BY_PRD = {
    "S_CUST_GEN#ZBP-DatiGenerali",         # con -
    "S_CUST_TAXNUMBERS#ZBP-CodiciFisc",
    "S_CUST_BANK_DATA#ZBP-AppoggioBanca"
}
# Tabelle secondarie lette da prd invece che da raw (stesso nome, stesso filtro orfani):
# prd.merge_dati_vendite imposta i default Incoterm INCO1='EXW' / INCO2='.' (CK218/CK219)
RAW_FROM_PRD = {
    "S_CUST_SALES_DATA#ZBP-DatiVendite",
}

# Flusso aggiuntivo i cui punti di scarico sono esportati negli ZIP delle anagrafiche:
# clienti ZBP (questo export) e destinatari merci (export DM). Gli interlocutori
# (S_CUST_CONT) restano nello ZIP del flusso, caricato in SAP in un secondo momento.
ADD_FLOW = "ZBP-AddInterlocutoreCli"
ADD_MOVED_TABLES = [
    "S_UNL_POINT#ZBP-AddPuntoScarico",
]
ADD_KEY_COL = "KUNNR(k/*)"

# Trascodifica del partner con i codici assegnati da SAP ai BP già caricati
PARTNER_TABLE = "S_CUST_SALES_PARTNER#ZBP-Partner"
PARTNER_COL   = "KUNN2(k)"
# Funzioni partner con codice diverso in SAP: RA (spedizioniere nell'ERP) -> SP
PARVW_COL = "PARVW(k/*)"
PARVW_MAP = {"RA": "SP"}
BUT000_TABLE  = "SAP_EXPORT_BUT000"
# Riferimenti al partner: una riga senza nessuno di questi valori è esclusa dall'output
PARTNER_REF_COLS = ["KUNN2(k)", "LIFNR(k)", "PERNR(k)", "PARNR(k)"]
# Funzioni partner che rimandano a un fornitore: il fornitore (KUNN2) deve esistere in SAP
VENDOR_PARVW = {"RA", "ZA"}
VENDOR_TABLE = "S_SUPPL_GEN#ZBP_DatiGenerali"
# Codici fiscali: una riga senza identificativo (TAXNUM vuoto) è esclusa dall'output
TAXNUM_TABLE = "S_CUST_TAXNUMBERS#ZBP-CodiciFisc"
TAXNUM_COLS  = ["TAXNUM(*)"]
# Ordinamento dell'output: chiave del file
PARTNER_KEY_COLS = ["KUNNR(k/*)", "VKORG(k/*)", "VTWEG(k/*)", "SPART(k/*)", "PARVW(k/*)"] + PARTNER_REF_COLS

# Estrazione ristretta (test): codici cliente in stg.export_filter -> ZIP in out_source_mdg/test
TEST_DIR = os.path.join(OUTPUT_DIR, "test")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_distinct_sources(conn) -> list[str]:
    schema, table = PRD_TABLES[0]
    fqt = f'{schema}.{q(table)}'
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT DISTINCT "_source" FROM {fqt} WHERE "_source" IS NOT NULL ORDER BY 1'
        )
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def get_orphan_keys(conn, check_id: str) -> set:
    """
    Recupera le chiavi (LIFNR o KUNNR) orfane dall'ultimo run del check_id
    specificato in stg.check_results (status != 'Ok').
    Ritorna un set vuoto se il check non è attivo o non ha risultati.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(is_active, FALSE)
            FROM stg.check_catalog WHERE check_id = %s
        """, (check_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return set()
        cur.execute("""
            SELECT DISTINCT object_key
            FROM stg.check_results
            WHERE check_id = %s
              AND status  != 'Ok'
              AND run_id  = (
                  SELECT MAX(run_id) FROM stg.check_results
                  WHERE check_id = %s
              )
        """, (check_id, check_id))
        return {r[0] for r in cur.fetchall()}


def fetch_table_csv(conn, schema: str, table: str,
                    source_filter: str, cols: list[str],
                    excluded_keys: set | None = None,
                    fk_col: str | None = None,
                    value_map: dict[str, dict] | None = None,
                    value_map_skip_if_equal: dict[str, str] | None = None,
                    drop_if_all_empty: list[str] | None = None,
                    drop_if_not_in: tuple[str, set, str, set] | None = None,
                    drop_if_unmapped: list[str] | None = None,
                    row_fn=None,
                    sort_cols: list[str] | None = None,
                    key_filter: tuple[str, set] | None = None,
                    write_discards: bool = True) -> bytes:
    """
    value_map:         {colonna: {valore_originale: nuovo_valore}} applicato in uscita.
    value_map_skip_if_equal: {colonna: altra_colonna} — non trascodifica se i due valori coincidono.
    drop_if_all_empty: righe con tutte queste colonne vuote escluse e scritte negli scarti.
    drop_if_not_in:    (colonna_filtro, valori_filtro, colonna, valori_ammessi) — righe con
                       colonna_filtro tra valori_filtro e colonna (valore originale, prima della
                       trascodifica) non tra valori_ammessi escluse e scritte negli scarti.
    drop_if_unmapped:  colonne di value_map: righe con valore senza trascodifica escluse e
                       scritte negli scarti.
    row_fn:            funzione (cols, riga come lista) che modifica la riga in uscita;
                       applicata dopo le esclusioni e prima di value_map (chiavi originali).
    sort_cols:         ordinamento dell'output (dopo la trascodifica).
    key_filter:        (colonna, valori) — solo le righe con quei valori (estrazione ristretta).
    write_discards:    False nell'estrazione ristretta (non tocca gli scarti dell'output completo).
    """
    fqt      = f'{schema}.{q(table)}'
    col_list = ", ".join(q(c) for c in cols)
    where    = ['"_source" = %s']
    params   = [source_filter]
    # Esclude in SQL i record orfani (solo tabelle secondarie)
    if excluded_keys and fk_col and fk_col in cols:
        where.append(f'{q(fk_col)} NOT IN ({",".join(["%s"] * len(excluded_keys))})')
        params.extend(excluded_keys)
        log.warning(
            "  [%s] %s.%s: %d chiavi escluse (orfani o BP già in SAP, _source=%s)",
            fk_col, schema, table, len(excluded_keys), source_filter
        )
    if key_filter and key_filter[0] in cols:
        where.append(f'{q(key_filter[0])} = ANY(%s)')
        params.append(sorted(key_filter[1]))
    with conn.cursor() as cur:
        cur.execute(f'SELECT {col_list} FROM {fqt} WHERE {" AND ".join(where)} ORDER BY 1', params)
        rows = cur.fetchall()

    def sanitize(val):
        """Converte None, float NaN e stringa 'NaN' in stringa vuota."""
        if val is None:
            return ""
        if isinstance(val, float) and math.isnan(val):
            return ""
        if isinstance(val, str) and val.strip() == "NaN":
            return ""
        return val

    if drop_if_not_in and drop_if_not_in[0] in cols and drop_if_not_in[2] in cols:
        f_col, f_values, col, allowed = drop_if_not_in
        i, j = cols.index(f_col), cols.index(col)
        dropped = [r for r in rows if r[i] in f_values and str(sanitize(r[j])).strip() not in allowed]
        if dropped:
            rows = [r for r in rows if not (r[i] in f_values and str(sanitize(r[j])).strip() not in allowed)]
            log.warning("  %s.%s: %d righe %s=%s con %s non presente in SAP escluse: %s",
                        schema, table, len(dropped), f_col, "/".join(sorted(f_values)), col,
                        sorted({str(r[j]) for r in dropped}))
            if write_discards:
                write_discarded_rows(table, cols, [tuple(sanitize(v) for v in r) for r in dropped])

    for col in drop_if_unmapped or []:
        if col not in cols:
            continue
        i, mapping = cols.index(col), (value_map or {}).get(col, {})
        dropped = [r for r in rows if r[i] not in mapping]
        if dropped:
            rows = [r for r in rows if r[i] in mapping]
            log.warning("  %s.%s: %d righe con %s senza trascodifica escluse: %s",
                        schema, table, len(dropped), col, sorted({str(r[i]) for r in dropped}))
            if write_discards:
                write_discarded_rows(table, cols, [tuple(sanitize(v) for v in r) for r in dropped])

    if row_fn:
        rows = [list(r) for r in rows]
        for r in rows:
            row_fn(cols, r)

    if value_map:
        rows = [list(r) for r in rows]
        for col, mapping in value_map.items():
            if col not in cols:
                continue
            i = cols.index(col)
            skip = (value_map_skip_if_equal or {}).get(col)
            j = cols.index(skip) if skip in cols else None
            n = 0
            for r in rows:
                if j is not None and r[i] == r[j]:
                    continue
                if r[i] in mapping:
                    r[i] = mapping[r[i]]
                    n += 1
            log.info("  [%s] %s.%s: %d valori trascodificati", col, schema, table, n)

    if drop_if_all_empty:
        idx  = [cols.index(c) for c in drop_if_all_empty if c in cols]
        keep = [r for r in rows if any(str(sanitize(r[i])).strip() for i in idx)]
        dropped = [r for r in rows if not any(str(sanitize(r[i])).strip() for i in idx)]
        if dropped and write_discards:
            write_discarded_rows(table, cols, [tuple(sanitize(v) for v in r) for r in dropped])
            log.warning("  %s.%s: %d righe senza %s escluse", schema, table, len(dropped), "/".join(drop_if_all_empty))
        rows = keep

    if sort_cols:
        idx = [cols.index(c) for c in sort_cols if c in cols]
        rows = sorted(rows, key=lambda r: tuple(str(sanitize(r[i])) for i in idx))

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    return buf.getvalue().encode("utf-8")


def get_but000_map(conn, sort_cols: tuple = ("BU_SORT2", "BU_SORT1")) -> dict[str, str]:
    """
    Vecchio codice -> nuovo codice SAP (PARTNER) da ref.SAP_EXPORT_BUT000:
    clienti e destinatari merci tramite BU_SORT2, spedizionieri tramite BU_SORT1.
    sort_cols: colonne usate, in ordine di priorità. Vuoto se la tabella non è stata caricata.
    """
    if not table_exists(conn, "ref", BUT000_TABLE):
        log.warning(f"  ref.{BUT000_TABLE} non trovata — {PARTNER_COL} non trascodificato")
        return {}
    mapping = {}
    with conn.cursor() as cur:
        for sort_col in sort_cols:
            cur.execute(
                f'SELECT {q(sort_col)}, "PARTNER" FROM ref.{q(BUT000_TABLE)} '
                f'WHERE COALESCE(TRIM({q(sort_col)}), \'\') <> \'\' AND COALESCE("PARTNER", \'\') <> \'\''
            )
            for old, new in cur.fetchall():
                mapping.setdefault(old.strip(), new)
    return mapping


def get_known_vendors(conn) -> set:
    """
    Fornitori esistenti in SAP (BU_SORT1 di ref.SAP_EXPORT_BUT000). Se BUT000 non è caricata
    (primo caricamento) i vettori di raw S_SUPPL_GEN#ZBP_DatiGenerali, creati nel primo passo.
    """
    with conn.cursor() as cur:
        if table_exists(conn, "ref", BUT000_TABLE):
            cur.execute(f'SELECT TRIM("BU_SORT1") FROM ref.{q(BUT000_TABLE)} '
                        f'WHERE COALESCE(TRIM("BU_SORT1"), \'\') <> \'\'')
        elif table_exists(conn, "raw", VENDOR_TABLE):
            cur.execute(f'SELECT TRIM("LIFNR(k/*)") FROM raw.{q(VENDOR_TABLE)}')
        else:
            return set()
        return {r[0] for r in cur.fetchall()}


def get_export_filter(conn) -> set:
    """Codici cliente dell'estrazione ristretta (stg.export_filter); vuoto = nessun filtro."""
    if not table_exists(conn, "stg", "export_filter"):
        return set()
    with conn.cursor() as cur:
        cur.execute("SELECT TRIM(kunnr) FROM stg.export_filter WHERE COALESCE(TRIM(kunnr), '') <> ''")
        return {r[0] for r in cur.fetchall()}


def get_exclude_add(conn) -> set:
    """Clienti con dati aggiuntivi già caricati in SAP (stg.export_exclude_add): esclusi da 03b e 07a."""
    if not table_exists(conn, "stg", "export_exclude_add"):
        return set()
    with conn.cursor() as cur:
        cur.execute("SELECT TRIM(kunnr) FROM stg.export_exclude_add WHERE COALESCE(TRIM(kunnr), '') <> ''")
        return {r[0] for r in cur.fetchall()}


def write_test_zip(source_name: str, build, keys: set, what: str):
    """
    Estrazione ristretta: rimuove lo ZIP di test precedente e, se ci sono chiavi,
    scrive in TEST_DIR lo ZIP prodotto da build() (solo se contiene almeno un CSV).
    """
    test_path = os.path.join(TEST_DIR, source_name)
    if os.path.exists(test_path):
        os.remove(test_path)
    if not keys:
        return
    zip_bytes = build()
    if not zipfile.ZipFile(io.BytesIO(zip_bytes)).namelist():
        log.info(f"  [TEST] {source_name}: nessuna riga per {what} {sorted(keys)} — ZIP non generato")
        return
    os.makedirs(TEST_DIR, exist_ok=True)
    with open(test_path, "wb") as f:
        f.write(zip_bytes)
    log.info(f"  [TEST] ✓  {test_path}  ({what}: {sorted(keys)})")


def already_in_sap_rows(conn, schema: str, table: str, source_name: str,
                        key_col: str, sap_map: dict[str, str]) -> list[tuple]:
    """
    Righe di una tabella (per _source) dei BP già presenti in SAP (chiave in sap_map):
    [(tabella, codice, codice_sap, n_righe)]. Le righe sono escluse dallo ZIP.
    """
    if not sap_map or key_col not in get_columns(conn, schema, table):
        return []
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {q(key_col)}, COUNT(*) FROM {schema}.{q(table)} '
            f'WHERE "_source" = %s AND {q(key_col)} = ANY(%s) GROUP BY 1 ORDER BY 1',
            (source_name, sorted(sap_map)),
        )
        return [(table, code, sap_map[code], n) for code, n in cur.fetchall()]


def already_in_sap_path(source_name: str) -> str:
    """Riepilogo dei BP esclusi da uno ZIP perché già in SAP: <nome ZIP>_gia_in_SAP.csv."""
    return os.path.join(OUTPUT_DIR, f"{os.path.splitext(source_name)[0]}_gia_in_SAP.csv")


def write_already_in_sap(source_name: str, entries: list[tuple], csv_names: dict | None = None):
    """
    Scrive (o rimuove, se non ci sono esclusioni) il riepilogo dei BP già in SAP esclusi
    dallo ZIP: TABELLA;CODICE;CODICE_SAP;RIGHE. TABELLA è il nome del CSV nello ZIP.
    """
    path = already_in_sap_path(source_name)
    if os.path.exists(path):
        os.remove(path)
    if not entries:
        return
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        writer.writerow(["TABELLA", "CODICE", "CODICE_SAP", "RIGHE"])
        writer.writerows([((csv_names or {}).get(t, t), c, s, n) for t, c, s, n in entries])
    codes = sorted({c for _, c, _, _ in entries})
    log.info("  [GIA' IN SAP] %d BP esclusi da '%s' (%d righe): riepilogo in %s",
             len(codes), source_name, sum(n for *_, n in entries), path)


def write_output_zip(source_name: str, zip_bytes: bytes):
    """Scrive lo ZIP di output; se non contiene CSV (es. tutti i BP già in SAP) rimuove quello precedente."""
    output_path = os.path.join(OUTPUT_DIR, source_name)
    if not zipfile.ZipFile(io.BytesIO(zip_bytes)).namelist():
        if os.path.exists(output_path):
            os.remove(output_path)
        log.info(f"  ✗  {source_name}: nessuna riga da esportare (BP già in SAP) — ZIP non generato")
        return
    with open(output_path, "wb") as f:
        f.write(zip_bytes)
    log.info(f"  ✓  {output_path}  ({len(zip_bytes) / 1024:.1f} KB)")


def write_discarded_csv(conn, table: str, source_name: str,
                        cols: list, orphan_keys: set, fk_col: str):
    """
    Scrive i record scartati (orfani) in un file CSV nella cartella OUTPUT_DIR.
    Nome file: <table>_discarded.csv  (es. S_ROLES#ZBP_RuoliFornitori_discarded.csv)
    Se non ci sono orfani o la colonna chiave non è presente, non scrive nulla.
    """
    if not orphan_keys or fk_col not in cols:
        return
    fqt      = "raw." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    placeholders = ",".join(["%s"] * len(orphan_keys))
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM {fqt}'
            f' WHERE "_source" = %s'
            f' AND {q(fk_col)} IN ({placeholders})'
            f' ORDER BY 1',
            (source_name, *orphan_keys)
        )
        rows = cur.fetchall()
    if not rows:
        return
    import io as _io, csv as _csv, math as _math
    def sanitize(val):
        if val is None: return ""
        if isinstance(val, float) and _math.isnan(val): return ""
        if isinstance(val, str) and val.strip() == "NaN": return ""
        return val
    buf = _io.StringIO()
    writer = _csv.writer(buf, delimiter=";", quoting=_csv.QUOTE_MINIMAL,
                         lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    safe_table = table.replace("#", "_").replace("/", "_")
    out_path   = os.path.join(OUTPUT_DIR, f"{safe_table}_discarded.csv")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    log.warning(
        "  [DISCARDED] %s: %d record orfani scritti in %s",
        table, len(rows), out_path
    )


def write_discarded_rows(table: str, cols: list, rows: list):
    """Accoda righe escluse a <table>_discarded.csv (intestazione solo se il file è nuovo)."""
    safe_table = table.replace("#", "_").replace("/", "_")
    out_path   = os.path.join(OUTPUT_DIR, f"{safe_table}_discarded.csv")
    new_file   = not os.path.exists(out_path)
    with open(out_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        if new_file:
            writer.writerow(cols)
        writer.writerows(rows)
    log.warning("  [DISCARDED] %s: %d righe scritte in %s", table, len(rows), out_path)


def remove_discarded_csv(tables: list[str]):
    """
    Cancella i file <table>_discarded.csv delle tabelle gestite da questo export:
    vanno eliminati a ogni run, altrimenti resterebbero gli scarti di run precedenti
    (write_discarded_csv scrive il file solo se ci sono scarti).
    """
    for table in tables:
        safe_table = table.replace("#", "_").replace("/", "_")
        out_path   = os.path.join(OUTPUT_DIR, f"{safe_table}_discarded.csv")
        if os.path.exists(out_path):
            os.remove(out_path)
            log.info("  [DISCARDED] rimosso file di scarto precedente: %s", out_path)


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        return cur.fetchone() is not None


def fetch_add_rows_csv(conn, table: str, master_table: str, source_name: str,
                       exclude_master: str | None = None, key_filter: set | None = None,
                       skip_keys: set | None = None) -> bytes:
    """
    Righe di una tabella del flusso ZBP-AddInterlocutoreCli relative ai clienti
    dell'anagrafica raw master_table con lo stesso _source dello ZIP in costruzione.
    exclude_master: anagrafica i cui clienti vanno esclusi (evita righe in due ZIP).
    """
    cols     = get_columns(conn, "raw", table)
    col_list = ", ".join(f"x.{q(c)}" for c in cols)
    k        = q(ADD_KEY_COL)
    exclude  = (
        f'AND NOT EXISTS (SELECT 1 FROM raw.{q(exclude_master)} ex WHERE ex.{k} = x.{k}) '
        if exclude_master else ""
    )
    only     = f'AND x.{k} = ANY(%s) ' if key_filter else ""
    skip     = f'AND NOT (x.{k} = ANY(%s)) ' if skip_keys else ""
    params   = ([f"%{ADD_FLOW}%", source_name]
                + ([sorted(key_filter)] if key_filter else [])
                + ([sorted(skip_keys)] if skip_keys else []))
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM raw.{q(table)} x '
            f'WHERE x."_source" ILIKE %s '
            f'AND EXISTS (SELECT 1 FROM raw.{q(master_table)} mst '
            f'            WHERE mst.{k} = x.{k} AND mst."_source" = %s) '
            f'{exclude}{only}{skip}'
            f'ORDER BY 1',
            params,
        )
        rows = cur.fetchall()

    def sanitize(val):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return ""
        return val

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    return buf.getvalue().encode("utf-8")


def write_add_tables(conn, zf, master_table: str, source_name: str,
                     exclude_master: str | None = None, key_filter: set | None = None,
                     sap_map: dict | None = None) -> list[tuple]:
    """
    Aggiunge allo ZIP le tabelle ADD_MOVED_TABLES (punti di scarico) dei clienti di master_table,
    esclusi i BP già in SAP (sap_map). Ritorna le righe escluse come already_in_sap_rows.
    """
    required = ADD_MOVED_TABLES + [master_table] + ([exclude_master] if exclude_master else [])
    if not all(table_exists(conn, "raw", t) for t in required):
        log.info(f"  Flusso {ADD_FLOW} non caricato — nessun punto di scarico")
        return []
    excluded = []
    for table in ADD_MOVED_TABLES:
        if sap_map:
            # righe dei BP già in SAP: stesso perimetro (anagrafica di questo ZIP), contate per il riepilogo
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT x.{q(ADD_KEY_COL)}, COUNT(*) FROM raw.{q(table)} x '
                    f'WHERE x."_source" ILIKE %s AND x.{q(ADD_KEY_COL)} = ANY(%s) '
                    f'AND EXISTS (SELECT 1 FROM raw.{q(master_table)} mst '
                    f'            WHERE mst.{q(ADD_KEY_COL)} = x.{q(ADD_KEY_COL)} AND mst."_source" = %s) '
                    + (f'AND NOT EXISTS (SELECT 1 FROM raw.{q(exclude_master)} ex '
                       f'                WHERE ex.{q(ADD_KEY_COL)} = x.{q(ADD_KEY_COL)}) ' if exclude_master else '')
                    + 'GROUP BY 1',
                    (f"%{ADD_FLOW}%", sorted(sap_map), source_name),
                )
                excluded += [(table, c, sap_map[c], n) for c, n in cur.fetchall()]
        csv_bytes = fetch_add_rows_csv(conn, table, master_table, source_name, exclude_master, key_filter,
                                       skip_keys=set(sap_map) if sap_map else None)
        n_rows = csv_bytes.count(b"\r\n") - 1
        if n_rows <= 0:
            log.info(f"  raw.{table} ({ADD_FLOW}): 0 righe — skip")
            continue
        zf.writestr(f"{table}.csv", csv_bytes)
        log.info(f"  + raw.{table} ({ADD_FLOW}): {n_rows} righe")
    return excluded


def build_zip(conn, source_name: str, key_filter: set | None = None,
              excluded_out: list | None = None) -> bytes:
    """
    key_filter:   codici cliente dell'estrazione ristretta (None = output completo).
    excluded_out: se passata, riceve le righe escluse perché il cliente è già in SAP.
    """
    buf = io.BytesIO()
    kf  = ("KUNNR(k/*)", key_filter) if key_filter else None
    # Clienti già creati in SAP (BUT000, BU_SORT2): esclusi da tutte le tabelle dello ZIP
    sap_map  = get_but000_map(conn, sort_cols=("BU_SORT2",))
    sap_keys = set(sap_map)
    excluded = []

    # Carica le chiavi orfane (CK404) — solo tabelle secondarie raw
    orphan_keys = get_orphan_keys(conn, "CK404")
    if orphan_keys:
        log.warning(
            "  *** [CK404] %d chiavi orfane per '%s' "
            "— verranno escluse dalle tabelle secondarie: %s",
            len(orphan_keys), source_name, sorted(orphan_keys)
        )
    else:
        log.info("  [CK404] Nessuna chiave orfana per '%s'.", source_name)

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:

        # 1. Tabelle da prd (ordine definito in PRD_TABLES)
        for prd_schema, prd_table in PRD_TABLES:
            prd_cols = get_columns(conn, prd_schema, prd_table)
            if prd_cols:
                excluded += already_in_sap_rows(conn, prd_schema, prd_table, source_name, "KUNNR(k/*)", sap_map)
                csv_bytes = fetch_table_csv(conn, prd_schema, prd_table, source_name, prd_cols, key_filter=kf,
                                            excluded_keys=sap_keys or None, fk_col="KUNNR(k/*)",
                                            drop_if_all_empty=TAXNUM_COLS if prd_table == TAXNUM_TABLE else None,
                                            write_discards=not key_filter)
                n_rows = csv_bytes.count(b"\r\n") - 1
                if n_rows <= 0:
                    log.info(f"  {prd_schema}.{prd_table}: 0 righe per '{source_name}' — skip")
                    continue
                zf.writestr(f"{PRD_CSV_NAME.get(prd_table, prd_table)}.csv", csv_bytes)
                log.info(f"  + {prd_schema}.{prd_table}: {n_rows} righe")
            else:
                log.warning(f"  Nessuna colonna trovata per {prd_schema}.{prd_table}")

        # 2. Tabelle da raw (skip quelle già prese da prd)
        for raw_table in RAW_TABLES:
            if raw_table in RAW_REPLACED_BY_PRD:
                log.info(f"  SKIP raw.{raw_table} — sostituito da prd")
                continue

            src_schema = "prd" if raw_table in RAW_FROM_PRD else "raw"
            if not table_exists(conn, src_schema, raw_table):
                log.warning(f"  SKIP {src_schema}.{raw_table} — tabella non trovata")
                continue

            raw_cols = get_columns(conn, src_schema, raw_table)
            if not raw_cols:
                log.warning(f"  SKIP {src_schema}.{raw_table} — nessuna colonna")
                continue

            _is_master = raw_table in {'S_CUST_GEN#ZBP-DatiGenerali'}
            if not _is_master and orphan_keys and not key_filter:
                write_discarded_csv(conn, raw_table, source_name,
                                   raw_cols, orphan_keys, "KUNNR(k/*)")
            excluded += already_in_sap_rows(conn, src_schema, raw_table, source_name, "KUNNR(k/*)", sap_map)
            csv_bytes = fetch_table_csv(
                conn, src_schema, raw_table, source_name, raw_cols,
                excluded_keys=(sap_keys if _is_master else orphan_keys | sap_keys) or None,
                fk_col="KUNNR(k/*)",
                value_map={PARTNER_COL: get_but000_map(conn), PARVW_COL: PARVW_MAP}
                          if raw_table == PARTNER_TABLE else None,
                value_map_skip_if_equal={PARTNER_COL: "KUNNR(k/*)"} if raw_table == PARTNER_TABLE else None,
                drop_if_all_empty=PARTNER_REF_COLS if raw_table == PARTNER_TABLE else None,
                drop_if_not_in=(PARVW_COL, VENDOR_PARVW, PARTNER_COL, get_known_vendors(conn))
                               if raw_table == PARTNER_TABLE else None,
                sort_cols=PARTNER_KEY_COLS if raw_table == PARTNER_TABLE else None,
                key_filter=kf,
                write_discards=not key_filter,
            )
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info(f"  raw.{raw_table}: 0 righe per '{source_name}' — skip")
                continue

            zf.writestr(f"{raw_table}.csv", csv_bytes)
            log.info(f"  + {src_schema}.{raw_table}: {n_rows} righe")

        # 3. Punti di scarico dei clienti entrati con il flusso ZBP-AddInterlocutoreCli
        excluded += write_add_tables(conn, zf, "S_CUST_GEN#ZBP-DatiGenerali", source_name,
                                     key_filter=key_filter, sap_map=sap_map)

    if excluded_out is not None:
        excluded_out.extend(excluded)
    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    remove_discarded_csv(RAW_TABLES)
    conn = get_connection()

    try:
        sources = get_distinct_sources(conn)
        if not sources:
            log.warning("Nessun valore di _source trovato in prd — nulla da esportare.")
            return

        log.info(f"Sorgenti trovate: {sources}")

        for source_name in sources:
            log.info(f"=== Generazione ZIP: {source_name} ===")
            excluded  = []
            zip_bytes = build_zip(conn, source_name, excluded_out=excluded)
            write_output_zip(source_name, zip_bytes)
            write_already_in_sap(source_name, excluded, PRD_CSV_NAME)

        # Estrazione ristretta (test): solo i clienti di stg.export_filter
        customers = get_export_filter(conn)
        for source_name in sources:
            write_test_zip(source_name, lambda: build_zip(conn, source_name, key_filter=customers),
                           customers, "clienti")

    finally:
        conn.close()

    log.info("=== export_clienti_zip completato ===")


if __name__ == "__main__":
    main()
