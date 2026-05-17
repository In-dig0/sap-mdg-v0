""" @bruin
name: prd.reconcile_zip
type: python
description: >
  Quadratura finale: confronta il numero di record nelle tabelle raw
  con il numero di righe scritte nei corrispondenti CSV dentro i file ZIP
  nella cartella out_source_mdg.
  Produce un report di quadratura nel log della pipeline con evidenza
  di eventuali discrepanze. Scrive anche un file CSV di riepilogo in
  out_source_mdg/reconcile_report.csv.
depends:
  - prd.export_suppliers_zip
  - prd.export_clienti_zip
  - prd.export_dest_merci_zip
  - prd.export_materiali_zip
  - prd.export_bom_zip
  - prd.export_inforecord_zip
  - prd.export_maritc_zip
  - prd.export_cicli_lavoro_zip
@bruin """

import os
import io
import csv
import zipfile
import logging
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

OUTPUT_DIR  = "/project/datalake/out_source_mdg"
REPORT_FILE = os.path.join(OUTPUT_DIR, "reconcile_report.csv")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_raw_counts(conn) -> dict[str, int]:
    """
    Restituisce il conteggio righe di tutte le tabelle nello schema raw,
    usando pg_stat_user_tables per una stima rapida (n_live_tup).
    Per le tabelle con 0 stimato, esegue COUNT(*) esatto.
    """
    counts = {}
    with conn.cursor() as cur:
        # Stima veloce
        cur.execute("""
            SELECT relname, n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'raw'
            ORDER BY relname
        """)
        rows = cur.fetchall()
        for table, n in rows:
            counts[table] = int(n)

        # Per le tabelle con stima 0, conta esatto
        for table, n in list(counts.items()):
            if n == 0:
                try:
                    cur.execute(
                        f'SELECT COUNT(*) FROM raw."{table}"'
                    )
                    counts[table] = cur.fetchone()[0]
                except Exception:
                    pass
    return counts


def count_csv_rows(zip_path: str) -> dict[str, int]:
    """
    Conta le righe dati (esclusa l'intestazione) in ogni CSV dentro uno ZIP.
    Ritorna dict {nome_tabella: nr_righe}.
    """
    result = {}
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if not name.endswith(".csv"):
                    continue
                table = name.replace(".csv", "")
                with zf.open(name) as f:
                    # Conta le righe lette (escludi header)
                    reader = csv.reader(
                        io.TextIOWrapper(f, encoding="utf-8"),
                        delimiter=";"
                    )
                    n = -1  # -1 per escludere l'header
                    for _ in reader:
                        n += 1
                result[table] = max(n, 0)
    except Exception as e:
        log.warning("  Errore lettura ZIP %s: %s", zip_path, e)
    return result


def main():
    conn = get_connection()
    try:
        log.info("=== Quadratura record raw <-> ZIP ===")

        # ── 1. Conteggio tabelle raw ─────────────────────────────────────────
        raw_counts = get_raw_counts(conn)
        log.info("  Tabelle raw trovate: %d", len(raw_counts))

        # ── 2. Conteggio CSV nei ZIP ─────────────────────────────────────────
        zip_counts: dict[str, int] = {}   # {table: nr_righe_nel_zip}
        zip_source: dict[str, str] = {}   # {table: nome_zip}

        zip_files = sorted([
            f for f in os.listdir(OUTPUT_DIR)
            if f.endswith(".zip") and f != "reconcile_report.csv"
        ])

        if not zip_files:
            log.warning("  Nessun file ZIP trovato in %s — quadratura impossibile.", OUTPUT_DIR)
            return

        log.info("  File ZIP trovati: %s", zip_files)

        for zf_name in zip_files:
            zf_path = os.path.join(OUTPUT_DIR, zf_name)
            csv_rows = count_csv_rows(zf_path)
            for table, n in csv_rows.items():
                if table in zip_counts:
                    # Tabella presente in più ZIP (es. stessa tabella su sorgenti diverse)
                    zip_counts[table] += n
                    zip_source[table] += f" + {zf_name}"
                else:
                    zip_counts[table] = n
                    zip_source[table] = zf_name

        # ── 3. Costruisci report di quadratura ───────────────────────────────
        # Considera tutte le tabelle presenti in almeno uno dei due insiemi
        all_tables = sorted(set(raw_counts.keys()) | set(zip_counts.keys()))

        report_rows = []
        n_ok = n_diff = n_missing_zip = n_missing_raw = 0

        for table in all_tables:
            raw_n = raw_counts.get(table)
            zip_n = zip_counts.get(table)
            src   = zip_source.get(table, "—")

            if raw_n is None:
                # Tabella nel ZIP ma non in raw — anomalia
                status = "⚠ SOLO_ZIP"
                n_missing_raw += 1
            elif zip_n is None:
                # Tabella in raw ma non in nessun ZIP — normale per tabelle non esportate
                status = "— NON_ESPORTATA"
            elif raw_n == zip_n:
                status = "✓ OK"
                n_ok += 1
            else:
                diff = zip_n - raw_n
                status = f"✗ DIFF ({diff:+d})"
                n_diff += 1

            report_rows.append({
                "tabella":    table,
                "raw_count":  raw_n if raw_n is not None else "—",
                "zip_count":  zip_n if zip_n is not None else "—",
                "stato":      status,
                "zip_source": src,
            })

        # ── 4. Log ───────────────────────────────────────────────────────────
        sep      = "=" * 80
        sep_thin = "-" * 80
        log.info(sep)
        log.info("  REPORT QUADRATURA  raw <-> ZIP")
        log.info(sep_thin)
        log.info("  %-40s %10s %10s  %s", "Tabella", "raw", "ZIP", "Stato")
        log.info("  %s", sep_thin)

        for r in report_rows:
            if "NON_ESPORTATA" in r["stato"]:
                continue  # non mostra le tabelle non esportate nel log principale
            log.info(
                "  %-40s %10s %10s  %s",
                r["tabella"], str(r["raw_count"]), str(r["zip_count"]), r["stato"]
            )

        log.info(sep_thin)
        log.info("  Tabelle OK:          %d", n_ok)
        if n_diff:
            log.warning("  Tabelle con diff:    %d  ← VERIFICARE", n_diff)
        else:
            log.info("  Tabelle con diff:    %d", n_diff)
        if n_missing_zip:
            log.warning("  Solo nel ZIP:        %d  ← ANOMALIA", n_missing_zip)
        log.info(sep)

        # ── 5. Scrivi CSV di riepilogo ───────────────────────────────────────
        with open(REPORT_FILE, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["tabella", "raw_count", "zip_count", "stato", "zip_source"],
                delimiter=";", quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()
            writer.writerows(report_rows)

        log.info("  Report CSV scritto: %s", REPORT_FILE)

        if n_diff > 0:
            log.warning(
                "  *** ATTENZIONE: %d tabelle hanno un numero di record "
                "diverso tra raw e ZIP — verificare i log degli step di export.",
                n_diff
            )

    finally:
        conn.close()

    log.info("=== reconcile_zip completato ===")


if __name__ == "__main__":
    main()
