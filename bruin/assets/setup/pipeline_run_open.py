""" @bruin
name: setup.pipeline_run_open
type: python
depends:
  - setup.init_db
  - setup.check_catalog
  - setup.apply_check_states
description: >
  Apre un nuovo record in stg.pipeline_runs usando la sequence
  stg.pipeline_run_seq come run_id progressivo (1, 2, 3...).
  Salva il run_id in /tmp/mdg_run_id.txt per pipeline_run_close.
  Rifiuta l'avvio (asset in errore, nessun caricamento né check) se nel
  container è attivo un altro processo 'bruin run'. Se non ce ne sono, i run
  rimasti 'running' sono stati interrotti (errore prima della chiusura,
  container riavviato): vengono chiusi come 'failed' e il nuovo run parte.
@bruin """

import os
import psycopg2
from datetime import datetime, timezone

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

SEMAPHORE_PATH = "/tmp/mdg_run_id.txt"

# Chiave del lock che serializza i controlli di due avvii quasi simultanei
RUN_OPEN_LOCK_KEY = 20261030


def _antenati(pid: int) -> set[int]:
    """PID del processo e dei suoi antenati (il 'bruin run' che sta eseguendo questo asset)."""
    pids = set()
    while pid > 0 and pid not in pids:
        pids.add(pid)
        try:
            with open(f"/proc/{pid}/stat") as f:
                # il nome del comando è tra parentesi e può contenere spazi: ppid è dopo ')'
                pid = int(f.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    return pids


def altri_run_bruin() -> list[int]:
    """PID dei processi 'bruin run' attivi nel container, escluso quello del run corrente."""
    propri = _antenati(os.getpid())
    altri = []
    for voce in os.listdir("/proc"):
        if not voce.isdigit() or int(voce) in propri:
            continue
        try:
            with open(f"/proc/{voce}/cmdline", "rb") as f:
                args = [a.decode(errors="replace") for a in f.read().split(b"\0") if a]
        except OSError:
            continue
        if args and os.path.basename(args[0]) == "bruin" and "run" in args[1:3]:
            altri.append(int(voce))
    return altri


def main():
    now = datetime.now(timezone.utc)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (RUN_OPEN_LOCK_KEY,))

            altri = altri_run_bruin()
            if altri:
                cur.execute("""
                    SELECT run_id, started_at FROM stg.pipeline_runs
                    WHERE status = 'running'
                    ORDER BY started_at DESC LIMIT 1
                """)
                attivo = cur.fetchone()
                dettaglio = (f"run #{attivo[0]} avviato alle {attivo[1].strftime('%Y-%m-%d %H:%M:%S')}"
                             if attivo else f"processo bruin PID {altri[0]}")
                raise RuntimeError(
                    f"Pipeline già in esecuzione ({dettaglio}): nuovo run rifiutato. "
                    f"Attendere il completamento del run in corso."
                )

            # Nessun altro run attivo: i record ancora 'running' sono run interrotti
            # (started_at/finished_at sono TIMESTAMP nel fuso della sessione, Europe/Rome)
            cur.execute("""
                UPDATE stg.pipeline_runs SET
                    status      = 'failed',
                    finished_at = LOCALTIMESTAMP,
                    notes       = COALESCE(notes || ' | ', '')
                                  || 'Chiuso d''ufficio all''avvio di un nuovo run: '
                                  || 'nessun processo Bruin attivo, run interrotto'
                WHERE status = 'running'
                RETURNING run_id
            """)
            for (stale_id,) in cur.fetchall():
                print(f"[WARN] Run #{stale_id} rimasto 'running' senza processo Bruin attivo: chiuso come 'failed'")

            cur.execute("SELECT nextval('stg.pipeline_run_seq')")
            run_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO stg.pipeline_runs (
                    run_id, pipeline_name, started_at, status
                ) VALUES (%s, %s, %s, %s)
            """, (run_id, "mdg_migration_pipeline", now, "running"))

        conn.commit()

        with open(SEMAPHORE_PATH, "w") as f:
            f.write(str(run_id))

        print(f"[OK] Pipeline run #{run_id} aperto alle {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
