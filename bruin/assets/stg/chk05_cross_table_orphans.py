""" @bruin
name: stg.chk05_cross_table_orphans
type: python
depends:
  - stg.clean_check_results
description: >
  CHK05 — CROSS_TABLE: verifica che ogni chiave FK presente nelle
  tabelle secondarie di ogni flusso ZIP esista nella tabella master
  dello stesso flusso. Un record orfano indica inconsistenza interna
  al flusso che causerebbe errori in SAP.

  Configurazione flussi:
    01-ZBP-Vettori / 04-ZBP-Fornitori → master: S_SUPPL_GEN#ZBP_DatiGenerali, FK: LIFNR(k/*)
    02-ZDM-Clienti                     → master: S_CUST_GEN#ZDM-DatiGenerali,  FK: KUNNR(k/*)
    03-ZBP-Clienti                     → master: S_CUST_GEN#ZBP-DatiGenerali,  FK: KUNNR(k/*)
connection: mdg_postgres
@bruin """

import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

# ---------------------------------------------------------------------------
# Configurazione flussi ZIP
# Ogni flusso ha: zip_prefix (pattern nel nome zip), tabella master,
# colonna FK nella master, colonna FK nelle secondarie (stessa colonna)
# ---------------------------------------------------------------------------
FLOWS = [
    {
        "check_id":      "CHK05_SUPPL",
        "zip_prefixes":  ["01-ZBP-Vettori", "04-ZBP-Fornitori",
                          "05-ZBP-FornitoriCli", "07-ZBP-AddInterlocutoreCli"],
        "master_table":  "S_SUPPL_GEN#ZBP_DatiGenerali",
        "master_fk":     "LIFNR(k/*)",
        "secondary_fk":  "LIFNR(k/*)",
        "category":      "BP",
    },
    {
        "check_id":      "CHK05_CUST_ZDM",
        "zip_prefixes":  ["02-ZDM-Clienti"],
        "master_table":  "S_CUST_GEN#ZDM-DatiGenerali",
        "master_fk":     "KUNNR(k/*)",
        "secondary_fk":  "KUNNR(k/*)",
        "category":      "BP",
    },
    {
        "check_id":      "CHK05_CUST",
        "zip_prefixes":  ["03-ZBP-Clienti", "06-ZBP-GestioneCredito"],
        "master_table":  "S_CUST_GEN#ZBP-DatiGenerali",
        "master_fk":     "KUNNR(k/*)",
        "secondary_fk":  "KUNNR(k/*)",
        "category":      "BP",
    },
]

def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_secondary_tables(cur, master_table: str, secondary_fk: str) -> list:
    """
    Restituisce tutte le tabelle nello schema raw che:
    1. Hanno la colonna FK richiesta
    2. Non sono la tabella master stessa
    """
    cur.execute("""
        SELECT DISTINCT c.table_name
        FROM information_schema.columns c
        WHERE c.table_schema = 'raw'
          AND c.column_name  = %s
          AND c.table_name  != %s
        ORDER BY c.table_name
    """, (secondary_fk, master_table))
    return [row[0] for row in cur.fetchall()]


def check_orphans_for_flow(cur, flow: dict, run_id: int, now) -> list:
    """
    Per ogni tabella secondaria del flusso, trova i valori FK
    che non esistono nella master, limitando al zip_source
    corrispondente al flusso.
    """
    master_table = flow["master_table"]
    master_fk    = flow["master_fk"]
    secondary_fk = flow["secondary_fk"]
    check_id     = flow["check_id"]
    category     = flow["category"]
    zip_prefixes = flow["zip_prefixes"]

    # Costruisce la condizione sui zip_source
    zip_conditions = " OR ".join(
        [f"sec.\"_zip_source\" ILIKE %s" for _ in zip_prefixes]
    )
    zip_params = [f"{p}%" for p in zip_prefixes]

    secondary_tables = get_secondary_tables(cur, master_table, secondary_fk)

    rows = []
    for sec_table in secondary_tables:
        sql = f"""
            SELECT DISTINCT
                sec.{q(secondary_fk)}  AS object_key,
                sec."_zip_source"       AS zip_source
            FROM raw.{q(sec_table)} sec
            WHERE ({zip_conditions})
              AND sec.{q(secondary_fk)} IS NOT NULL
              AND sec.{q(secondary_fk)} <> ''
              AND NOT EXISTS (
                SELECT 1 FROM raw.{q(master_table)} mst
                WHERE mst.{q(master_fk)} = sec.{q(secondary_fk)}
              )
        """
        cur.execute(sql, zip_params)
        for rec in cur.fetchall():
            rows.append((
                sec_table,
                category,
                rec[0],
                check_id,
                f"[{secondary_fk.split('(')[0]}={rec[0]}] presente in {sec_table} "
                f"ma assente nella master {master_table}",
                "Error",
                run_id,
                rec[1],
                now,
            ))

    return rows


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Recupera run_id corrente
            cur.execute("""
                SELECT run_id FROM stg.pipeline_runs
                WHERE status = 'running'
                ORDER BY started_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Nessun run attivo in stg.pipeline_runs")
            run_id = row[0]

            now = datetime.now(timezone.utc)
            total_orphans = 0

            for flow in FLOWS:
                print(f"[INFO] Verifica flusso {flow['check_id']} "
                      f"— master: {flow['master_table']}")

                orphan_rows = check_orphans_for_flow(cur, flow, run_id, now)
                total_orphans += len(orphan_rows)

                if orphan_rows:
                    execute_values(cur, """
                        INSERT INTO stg.check_results (
                            source_table, category, object_key, check_id,
                            message, status, run_id, zip_source, created_at
                        ) VALUES %s
                    """, orphan_rows, page_size=500)
                    print(f"[WARN] {flow['check_id']}: {len(orphan_rows)} orfani trovati")
                else:
                    print(f"[OK]   {flow['check_id']}: nessun orfano")

        conn.commit()
        print(f"[OK] Totale orfani inseriti: {total_orphans}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
