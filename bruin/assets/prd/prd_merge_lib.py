"""
Libreria comune per i 3 asset prd.merge_*.
Non è un asset Bruin — viene importata dagli asset merge.
"""
import os
import re
import logging
import psycopg2

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

PRD_SCHEMA     = "prd"
STG_AUDIT_COLS = {"_status", "_source", "_loaded_at", "_xlsx_source"}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_columns(cur, schema: str, table: str) -> list[str]:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    return [r[0] for r in cur.fetchall()]


def get_key_columns(cols: list[str]) -> list[str]:
    return [c for c in cols if re.search(r"\(.*?k.*?\)", c)]


def ensure_prd_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PRD_SCHEMA}")


def check_vat_vies_exists(cur) -> bool:
    """Verifica che stg.check_vat_vies esista."""
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'stg' AND table_name = 'check_vat_vies'
    """)
    return cur.fetchone()[0] > 0


VIES_TAXTYPE_COL = "TAXTYPE(k/*)"
VIES_TAXNUM_COL  = "TAXNUM(*)"


def _vat_number_sql(alias: str = "raw") -> str:
    """
    P.IVA normalizzata come in check_vat_vies.vat_number: CC + numero, senza
    spazi, punti e trattini (stessa regola di normalize_taxnum() in CK801/CK803).
    """
    num = f"regexp_replace(UPPER({alias}.{q(VIES_TAXNUM_COL)}), '[[:space:].-]', '', 'g')"
    cc  = f"UPPER(LEFT({alias}.{q(VIES_TAXTYPE_COL)}, 2))"
    return f"(CASE WHEN {num} LIKE {cc} || '%' THEN {num} ELSE {cc} || {num} END)"


def _vies_invalid_sql(alias: str = "raw") -> str:
    """
    Condizione vera se il record è una P.IVA CEE (TAXTYPE '%0') il cui ULTIMO
    esito reale in stg.check_vat_vies è "UE non valida". La validità VIES è della
    P.IVA, quindi conta l'ultimo esito per quel numero, di qualunque cliente/fornitore
    e di qualunque run. Esclusi CACHED/PENDING (segnaposto) ed ERROR (servizio
    non raggiungibile: non dice nulla sulla validità).
    """
    return f"""(
            {alias}.{q(VIES_TAXTYPE_COL)} LIKE '%0'
            AND (
                SELECT v.is_eu = TRUE AND v.vies_valid = FALSE
                FROM stg.check_vat_vies v
                WHERE v.vat_number = {_vat_number_sql(alias)}
                  AND v.check_status NOT IN ('CACHED', 'PENDING', 'ERROR')
                ORDER BY v.created_at DESC
                LIMIT 1
            ) IS TRUE
        )"""


def _log_vies_discards(cur, raw_fqt: str, vies_lifnr_col: str):
    """Logga i record scartati dal filtro VIES: uno per codice + TAXTYPE + TAXNUM."""
    cur.execute(f"""
        SELECT DISTINCT
            raw.{q(vies_lifnr_col)}, raw.{q(VIES_TAXTYPE_COL)}, raw.{q(VIES_TAXNUM_COL)},
            {_vat_number_sql('raw')} AS vat_number
        FROM {raw_fqt} raw
        WHERE {_vies_invalid_sql('raw')}
        ORDER BY 1
    """)
    discarded = cur.fetchall()

    if not discarded:
        log.info("  ✓  VIES — nessun record scartato.")
        return

    col_headers = [vies_lifnr_col, VIES_TAXTYPE_COL, VIES_TAXNUM_COL, "vat_number (VIES)"]

    log.warning(
        f"  ⚠  VIES — {len(discarded)} record scartati da "
        f"{raw_fqt} (P.IVA CEE con ultimo esito VIES non valido):"
    )
    header = "  " + " | ".join(f"{h:<30}" for h in col_headers)
    log.warning(header)
    log.warning("  " + "-" * (len(header) - 2))
    for row in discarded:
        log.warning("  " + " | ".join(f"{str(v):<30}" for v in row))


def check_ck047_results_exists(cur) -> bool:
    """Verifica che stg.check_results abbia righe per CK047 con status != 'Ok'."""
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'stg' AND table_name = 'check_results'
    """)
    return cur.fetchone()[0] > 0


def _log_ck047_discards(cur, raw_fqt: str, ck047_lifnr_col: str):
    """
    Logga i record scartati dal filtro CK047 (coordinate bancarie IT non valide).
    Un record per LIFNR+BANKS+BANKL.
    """
    cur.execute(f"""
        SELECT
            raw.{q(ck047_lifnr_col)},
            raw."BANKS(k/*)",
            raw."BANKL(k)"
        FROM {raw_fqt} raw
        WHERE raw."BANKS(k/*)" = 'IT'
          AND NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_BNKA_IT" b
            WHERE b."BANKS" = 'IT'
              AND b."BANKL" = raw."BANKL(k)"
              AND COALESCE(b."LOEVM", '') <> 'X'
          )
        ORDER BY raw.{q(ck047_lifnr_col)}, raw."BANKL(k)"
    """)
    discarded = cur.fetchall()

    if not discarded:
        log.info("  ✓  CK047 — nessun record scartato (coordinate bancarie IT tutte valide).")
        return

    log.warning(
        f"  ⚠  CK047 — {len(discarded)} record scartati da "
        f"{raw_fqt} (BANKS=IT, BANKL non presente o cancellata in ref.SAP_EXPORT_BNKA_IT):"
    )
    col_headers = [ck047_lifnr_col, "BANKS(k/*)", "BANKL(k)"]
    header = "  " + " | ".join(f"{h:<30}" for h in col_headers)
    log.warning(header)
    log.warning("  " + "-" * (len(header) - 2))
    for row in discarded:
        log.warning("  " + " | ".join(f"{str(v):<30}" for v in row))


def _log_ck048_discards(cur, raw_fqt: str, ck048_kunnr_col: str):
    """
    Logga i record scartati dal filtro CK048 (coordinate bancarie IT clienti non valide).
    Un record per KUNNR+BANKS+BANKL.
    """
    cur.execute(f"""
        SELECT
            raw.{q(ck048_kunnr_col)},
            raw."BANKS(k)",
            raw."BANKL(k)"
        FROM {raw_fqt} raw
        WHERE raw."BANKS(k)" = 'IT'
          AND NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_BNKA_IT" b
            WHERE b."BANKS" = 'IT'
              AND b."BANKL" = raw."BANKL(k)"
              AND COALESCE(b."LOEVM", '') <> 'X'
          )
        ORDER BY raw.{q(ck048_kunnr_col)}, raw."BANKL(k)"
    """)
    discarded = cur.fetchall()

    if not discarded:
        log.info("  ✓  CK048 — nessun record scartato (coordinate bancarie IT clienti tutte valide).")
        return

    log.warning(
        f"  ⚠  CK048 — {len(discarded)} record scartati da "
        f"{raw_fqt} (BANKS=IT, BANKL non presente o cancellata in ref.SAP_EXPORT_BNKA_IT):"
    )
    col_headers = [ck048_kunnr_col, "BANKS(k)", "BANKL(k)"]
    header = "  " + " | ".join(f"{h:<30}" for h in col_headers)
    log.warning(header)
    log.warning("  " + "-" * (len(header) - 2))
    for row in discarded:
        log.warning("  " + " | ".join(f"{str(v):<30}" for v in row))


BANK_UE_REF = 'ref."SAP_EXPORT_BNKA_UE"'
BANK_UE_COLS = ("BANKL(k)", "IBAN(k)", "BKREF")   # BKREF = SWIFT indicato dall'ERP


def _bank_ue_call(banks_col: str, alias: str = "raw") -> str:
    """
    Chiamata a stg.fn_banca_estera (setup/fn_banca_estera.sql): unica regola per
    determinare la chiave banca di una coordinata estera (BANKL esatto, poi IBAN,
    poi SWIFT in BKREF), condivisa con i check CK055/CK056.
    """
    return (f'stg.fn_banca_estera({alias}.{q(banks_col)}, {alias}."BANKL(k)", '
            f'{alias}."IBAN(k)", {alias}."BKREF")')


def _bank_ue_invalid_sql(banks_col: str, alias: str = "raw") -> str:
    """Vera per una coordinata estera (BANKS <> 'IT') la cui chiave banca non è determinabile."""
    return (f"(COALESCE({alias}.{q(banks_col)}, '') <> 'IT' "
            f"AND (SELECT r.esito FROM {_bank_ue_call(banks_col, alias)} r) <> 'OK')")


def bank_ue_ready(cur) -> bool:
    """Anagrafica BNKA UE caricata e funzione stg.fn_banca_estera presente."""
    cur.execute("""
        SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'ref' AND tablename = 'SAP_EXPORT_BNKA_UE')
           AND EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                       WHERE n.nspname = 'stg' AND p.proname = 'fn_banca_estera')
    """)
    return cur.fetchone()[0]


def _log_bank_ue(cur, raw_fqt: str, key_col: str, banks_col: str, check_id: str):
    """
    Logga le coordinate bancarie estere:
      - scartate (chiave banca non determinabile), con il motivo;
      - con chiave banca ricavata da IBAN o SWIFT e scritta in prd.
    """
    cur.execute(f"""
        SELECT raw.{q(key_col)}, raw.{q(banks_col)}, raw."BANKL(k)", raw."IBAN(k)", raw."BKREF",
               r.bankl, r.metodo, r.esito, r.dettaglio
        FROM {raw_fqt} raw
        CROSS JOIN LATERAL {_bank_ue_call(banks_col)} r
        WHERE COALESCE(raw.{q(banks_col)}, '') <> 'IT'
        ORDER BY 1, 2
    """)
    rows = cur.fetchall()
    scartate = [r for r in rows if r[7] != "OK"]
    ricavate = [r for r in rows if r[7] == "OK" and r[6] in ("IBAN", "SWIFT")]

    if scartate:
        log.warning(
            f"  ⚠  {check_id} — {len(scartate)} record scartati da {raw_fqt} "
            f"(BANKS<>IT, chiave banca non determinabile in {BANK_UE_REF}):"
        )
        for key, banks, bankl, iban, swift, _, _, esito, dettaglio in scartate:
            log.warning(f"    {key:<14} {banks or '':<3} {esito:<14} {dettaglio or ''}")
    else:
        log.info(f"  ✓  {check_id} — nessun record scartato (coordinate bancarie estere tutte valide).")

    if ricavate:
        log.info(f"  {check_id} — {len(ricavate)} chiavi banca (BANKL) ricavate e scritte in prd (BKREF svuotato):")
        for key, banks, _, iban, swift, bankl, metodo, _, _ in ricavate:
            fonte = f"IBAN {iban}" if metodo == "IBAN" else f"SWIFT {swift}"
            log.info(f"    {key:<14} {banks:<3} {fonte:<40} → BANKL {bankl}")


def merge_table(cur, raw_schema: str, raw_table: str,
                stg_schema: str, stg_table: str,
                prd_table: str,
                vies_filter: bool = False,
                vies_lifnr_col: str | None = None,
                ck047_filter: bool = False,
                ck047_lifnr_col: str | None = None,
                ck048_filter: bool = False,
                ck048_kunnr_col: str | None = None,
                bank_ue_filter: bool = False,
                bank_ue_key_col: str | None = None,
                bank_ue_banks_col: str | None = None,
                bank_ue_check_id: str = "CK055/CK056"):
    """
    Merge raw + stg -> prd.
    - Colonne comuni: COALESCE(stg.col, raw.col) — preferenza a stg
    - _source e _loaded_at: sempre da raw
    - _status esclusa da prd
    - Record stg con _status='DELETED' esclusi dal JOIN
    - Se la tabella STG non esiste, usa solo raw (nessun JOIN).
    - vies_filter=True: esclude TAXTYPE LIKE '%0' con PIVA EU non valida.
    - ck047_filter=True: esclude record con BANKS=IT e BANKL non in ref.SAP_EXPORT_BNKA_IT (fornitori).
    - ck048_filter=True: esclude record con BANKS=IT e BANKL non in ref.SAP_EXPORT_BNKA_IT (clienti).
    - bank_ue_filter=True: coordinate bancarie estere (BANKS<>IT), regola stg.fn_banca_estera
      (CK055 fornitori, CK056 clienti): esclude quelle senza chiave banca determinabile,
      scrive in prd la chiave ricavata (da IBAN o SWIFT) e svuota BKREF.
    """
    raw_fqt = f'{raw_schema}.{q(raw_table)}'
    stg_fqt = f'{stg_schema}.{q(stg_table)}'
    prd_fqt = f'{PRD_SCHEMA}.{q(prd_table)}'

    log.info(f"=== Merge: {raw_fqt} + {stg_fqt} -> {prd_fqt} ===")

    raw_cols = get_columns(cur, raw_schema, raw_table)
    stg_cols = get_columns(cur, stg_schema, stg_table)

    if not raw_cols:
        log.warning(f"  {raw_fqt} non trovata — skip.")
        return

    has_stg = bool(stg_cols)
    if not has_stg:
        log.warning(f"  {stg_fqt} non trovata — uso solo raw (nessun JOIN STG).")

    key_cols = get_key_columns(raw_cols)
    if not key_cols:
        log.warning(f"  Nessuna colonna chiave in {raw_fqt} — skip.")
        return
    log.info(f"  Chiavi: {key_cols}")

    stg_data_cols = [
        c for c in stg_cols
        if c not in STG_AUDIT_COLS and c not in key_cols
    ]
    raw_col_set = set(raw_cols)
    common_cols = set(c for c in stg_data_cols if c in raw_col_set)
    log.info(f"  Colonne comuni raw<->stg (override da stg): {len(common_cols)}")

    # ---- SELECT ----
    # Usiamo la prima colonna chiave come sentinella: se il record STG esiste
    # (LEFT JOIN trovato) prendiamo SEMPRE il valore da STG, anche se NULL o
    # vuoto. Usiamo RAW solo quando il record STG è assente (sentinel IS NULL).
    stg_sentinel = q(key_cols[0]) if key_cols else None

    # Banche estere: chiave banca determinata da stg.fn_banca_estera (BANKL, IBAN o
    # SWIFT) e scritta in prd; BKREF (usato dall'ERP per lo SWIFT) svuotato
    fill_bank_ue = (
        bank_ue_filter and bank_ue_key_col and bank_ue_banks_col
        and all(c in raw_cols for c in (bank_ue_banks_col,) + BANK_UE_COLS)
        and bank_ue_ready(cur)
    )

    select_parts = []
    for col in raw_cols:
        if col in ("_source", "_loaded_at"):
            select_parts.append(f'raw.{q(col)}')
            continue
        if col in common_cols and stg_sentinel:
            expr = (f'CASE WHEN stg.{stg_sentinel} IS NOT NULL '
                    f'THEN stg.{q(col)} ELSE raw.{q(col)} END')
        else:
            expr = f'raw.{q(col)}'
        if fill_bank_ue and col in ("BANKL(k)", "BKREF"):
            estera = f"COALESCE(raw.{q(bank_ue_banks_col)}, '') <> 'IT'"
            if col == "BANKL(k)":
                expr = (f"CASE WHEN {estera} "
                        f"THEN COALESCE((SELECT r.bankl FROM {_bank_ue_call(bank_ue_banks_col)} r), {expr}) "
                        f"ELSE {expr} END")
            else:
                expr = f"CASE WHEN {estera} THEN '' ELSE {expr} END"
        select_parts.append(expr if expr == f'raw.{q(col)}' else f'{expr} AS {q(col)}')
    select_sql = ",\n        ".join(select_parts)

    # ---- JOIN e WHERE condizionali alla presenza di STG ----
    if has_stg:
        join_clause = (
            f"LEFT JOIN {stg_fqt} stg ON "
            + " AND ".join(f'raw.{q(k)} = stg.{q(k)}' for k in key_cols)
            + " AND stg.\"_status\" != 'DELETED'"
        )
        deleted_cond = " AND ".join(f'd.{q(k)} = raw.{q(k)}' for k in key_cols)
        where_clauses = [f"""NOT EXISTS (
            SELECT 1 FROM {stg_fqt} d
            WHERE {deleted_cond}
              AND d."_status" = 'DELETED'
        )"""]
    else:
        join_clause   = ""
        where_clauses = []

    # ---- Filtro VIES ----
    if vies_filter and vies_lifnr_col:
        if check_vat_vies_exists(cur):
            if VIES_TAXTYPE_COL in raw_cols and VIES_TAXNUM_COL in raw_cols:
                log.info(
                    f"  Filtro VIES attivo (P.IVA CEE, TAXTYPE LIKE '%0': "
                    f"ultimo esito VIES per numero di P.IVA)"
                )
                where_clauses.append(f"NOT {_vies_invalid_sql('raw')}")
            else:
                log.warning(
                    f"  Filtro VIES saltato: colonne {VIES_TAXTYPE_COL} / "
                    f"{VIES_TAXNUM_COL} assenti in {raw_table}."
                )
        else:
            log.warning("  stg.check_vat_vies non trovata — filtro VIES saltato.")

    # ---- Filtro CK047 (coordinate bancarie IT non valide) ----
    if ck047_filter and ck047_lifnr_col:
        if check_ck047_results_exists(cur):
            bankl_col = "BANKL(k)"
            banks_col = "BANKS(k/*)"
            if bankl_col in raw_cols and banks_col in raw_cols:
                log.info(
                    f"  Filtro CK047 attivo su {q(ck047_lifnr_col)} "
                    f"(BANKS=IT, BANKL non presente o cancellata in ref.SAP_EXPORT_BNKA_IT)"
                )
                where_clauses.append(f"""NOT (
            raw.{q(banks_col)} = 'IT'
            AND NOT EXISTS (
                SELECT 1 FROM ref."SAP_EXPORT_BNKA_IT" b
                WHERE b."BANKS" = 'IT'
                  AND b."BANKL" = raw.{q(bankl_col)}
                  AND COALESCE(b."LOEVM", '') <> 'X'
            )
        )""")
            else:
                log.warning(
                    f"  Filtro CK047: colonne {bankl_col} o {banks_col} "
                    f"non trovate in {raw_fqt} — filtro saltato."
                )
        else:
            log.warning("  stg.check_results non trovata — filtro CK047 saltato.")

    # ---- Log record scartati da VIES (prima del DROP, un record per PIVA) ----
    if (vies_filter and vies_lifnr_col and check_vat_vies_exists(cur)
            and VIES_TAXTYPE_COL in raw_cols and VIES_TAXNUM_COL in raw_cols):
        _log_vies_discards(cur, raw_fqt, vies_lifnr_col)

    # ---- Log record scartati da CK047 (prima del DROP) ----
    if ck047_filter and ck047_lifnr_col and check_ck047_results_exists(cur):
        _log_ck047_discards(cur, raw_fqt, ck047_lifnr_col)

    # ---- Filtro CK048 (coordinate bancarie IT clienti non valide) ----
    if ck048_filter and ck048_kunnr_col:
        if check_ck047_results_exists(cur):  # riusa lo stesso check su stg.check_results
            bankl_col  = "BANKL(k)"
            banks_col  = "BANKS(k)"
            if bankl_col in raw_cols and banks_col in raw_cols:
                log.info(
                    f"  Filtro CK048 attivo su {q(ck048_kunnr_col)} "
                    f"(BANKS=IT, BANKL non presente o cancellata in ref.SAP_EXPORT_BNKA_IT)"
                )
                where_clauses.append(f"""NOT (
            raw.{q(banks_col)} = 'IT'
            AND NOT EXISTS (
                SELECT 1 FROM ref."SAP_EXPORT_BNKA_IT" b
                WHERE b."BANKS" = 'IT'
                  AND b."BANKL" = raw.{q(bankl_col)}
                  AND COALESCE(b."LOEVM", '') <> 'X'
            )
        )""")
            else:
                log.warning(
                    f"  Filtro CK048: colonne {bankl_col} o {banks_col} "
                    f"non trovate in {raw_fqt} — filtro saltato."
                )
        else:
            log.warning("  stg.check_results non trovata — filtro CK048 saltato.")

    # ---- Log record scartati da CK048 (prima del DROP) ----
    if ck048_filter and ck048_kunnr_col and check_ck047_results_exists(cur):
        _log_ck048_discards(cur, raw_fqt, ck048_kunnr_col)

    # ---- Filtro banche estere (CK055 fornitori / CK056 clienti) ----
    if bank_ue_filter and bank_ue_key_col and bank_ue_banks_col:
        needed = [bank_ue_banks_col, *BANK_UE_COLS]
        if not all(c in raw_cols for c in needed):
            log.warning(f"  Filtro {bank_ue_check_id}: colonne {needed} non tutte presenti in {raw_fqt} — filtro saltato.")
        elif not bank_ue_ready(cur):
            log.warning(f"  {BANK_UE_REF} o stg.fn_banca_estera non disponibili — filtro {bank_ue_check_id} saltato.")
        else:
            log.info(
                f"  Filtro {bank_ue_check_id} attivo su {q(bank_ue_key_col)} "
                f"(BANKS<>IT: chiave banca da BANKL, IBAN o SWIFT in {BANK_UE_REF})"
            )
            where_clauses.append(f"NOT {_bank_ue_invalid_sql(bank_ue_banks_col)}")
            _log_bank_ue(cur, raw_fqt, bank_ue_key_col, bank_ue_banks_col, bank_ue_check_id)

    # Costruita dopo TUTTI i filtri (prima il filtro CK048 veniva aggiunto
    # quando la WHERE era già stata composta e non veniva mai applicato)
    where_clause = ("WHERE " + "\n          AND ".join(where_clauses)
                    if where_clauses else "")

    # ---- DROP + CREATE ----
    cur.execute(f"DROP TABLE IF EXISTS {prd_fqt}")
    cur.execute(f"""
        CREATE TABLE {prd_fqt} AS
        SELECT
        {select_sql}
        FROM {raw_fqt} raw
        {join_clause}
        {where_clause}
    """)

    cur.execute(f"SELECT COUNT(*) FROM {prd_fqt}")
    n = cur.fetchone()[0]
    log.info(f"  ✓  {prd_fqt}: {n} record")
