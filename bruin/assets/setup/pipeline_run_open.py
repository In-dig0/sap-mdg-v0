""" @bruin
name: setup.pipeline_run_open
type: python
depends:
  - setup.init_db
  - setup.check_catalog
  - setup.apply_check_states
secrets:
  - key: mdg_postgres        # connessione degli asset SQL: per il controllo di coerenza
    inject_as: MDG_BRUIN_PG
description: >
  Apre un nuovo record in stg.pipeline_runs usando la sequence
  stg.pipeline_run_seq come run_id progressivo (1, 2, 3...).
  Salva il run_id in /tmp/mdg_run_id.txt per pipeline_run_close.
  Rifiuta l'avvio (asset in errore, nessun caricamento né check) se nel
  container è attivo un altro processo 'bruin run'. Se non ce ne sono, i run
  rimasti 'running' sono stati interrotti (errore prima della chiusura,
  container riavviato): vengono chiusi come 'failed' e il nuovo run parte.
  Ambiente SAP (mdg_env, variabile MDG_ENV): il run si ferma se il database degli asset
  Python è diverso da quello della connessione Bruin degli asset SQL (mdg_postgres).
@bruin """

import os
import sys
import json
import psycopg2
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from mdg_env import DB_CONFIG, MDG_ENV, OUT_MDG  # noqa: E402

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


def check_env():
    """
    Stesso database per asset SQL (connessione Bruin mdg_postgres) e asset Python (mdg_env):
    altrimenti i dati di un ambiente finirebbero in un altro.
    """
    bruin_db = json.loads(os.environ.get("MDG_BRUIN_PG") or "{}").get("database")
    print(f"[OK] Ambiente: {MDG_ENV} — database {DB_CONFIG['dbname']} — output {OUT_MDG}")
    if bruin_db and bruin_db != DB_CONFIG["dbname"]:
        raise RuntimeError(
            f"Ambiente {MDG_ENV}: database degli asset Python ({DB_CONFIG['dbname']}) diverso da quello "
            f"della connessione Bruin degli asset SQL ({bruin_db}). Run interrotto."
        )


def main():
    now = datetime.now(timezone.utc)
    check_env()

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

            # Opzioni richieste dalla pagina Gestione pipeline (API) negli ultimi minuti: copiate
            # nel run e consumate; senza richiesta (run da terminale) restano i valori di default
            cur.execute("""
                DELETE FROM stg.pipeline_run_request
                WHERE requested_at > LOCALTIMESTAMP - INTERVAL '15 minutes'
                RETURNING opt_full, opt_original_codes
            """)
            request = cur.fetchone()
            cur.execute("DELETE FROM stg.pipeline_run_request")
            opt_full, opt_original = request if request else (False, False)
            cur.execute("UPDATE stg.pipeline_runs SET opt_full = %s, opt_original_codes = %s WHERE run_id = %s",
                        (opt_full, opt_original, run_id))

        conn.commit()

        with open(SEMAPHORE_PATH, "w") as f:
            f.write(str(run_id))

        print(f"[OK] Pipeline run #{run_id} aperto alle {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"[OK] Estrazione full (oggetti già in SAP non esclusi): {'SI' if opt_full else 'NO'} — "
              f"codice BP: {'originale (nessuna trascodifica)' if opt_original else 'SAP (trascodificato)'}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
