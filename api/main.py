"""
MDG Pipeline API — FastAPI
Endpoints per avvio, monitoraggio e log della pipeline Bruin.
"""

import asyncio
import os
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import psycopg2
from fastapi import FastAPI, BackgroundTasks, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------

BRUIN_CONTAINER   = os.getenv("BRUIN_CONTAINER", "mdg_bruin")
BRUIN_PIPELINE    = os.getenv("BRUIN_PIPELINE", "/project/bruin")
SEMAPHORE_PATH    = os.getenv("SEMAPHORE_PATH", "/data/inbound/DATASET_READY.txt")
SAP_PATH          = Path(os.getenv("SAP_PATH", "/data/from_sap"))
LOG_DIR           = Path(os.getenv("LOG_DIR", "/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "mdg")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "mdg_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# ---------------------------------------------------------------------------
# Stato in memoria (per v0 — in produzione usare Redis o DB)
# ---------------------------------------------------------------------------

class RunStatus(str, Enum):
    IDLE      = "idle"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILED    = "failed"

class PipelineState:
    def __init__(self):
        self.run_id:     Optional[str]      = None
        self.status:     RunStatus          = RunStatus.IDLE
        self.started_at: Optional[datetime] = None
        self.ended_at:   Optional[datetime] = None
        self.log_file:   Optional[Path]     = None
        self.exit_code:  Optional[int]      = None
        self.error_msg:  Optional[str]      = None

state = PipelineState()

# ---------------------------------------------------------------------------
# App FastAPI
# ---------------------------------------------------------------------------

app = FastAPI(
    title="MDG Pipeline API",
    description="API per la gestione della pipeline Bruin — Progetto MDG",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # In produzione: restringere a Streamlit
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Modelli risposta
# ---------------------------------------------------------------------------

class RunResponse(BaseModel):
    run_id:     str
    status:     RunStatus
    started_at: datetime
    message:    str

class StatusResponse(BaseModel):
    run_id:     Optional[str]
    status:     RunStatus
    started_at: Optional[datetime]
    ended_at:   Optional[datetime]
    exit_code:  Optional[int]
    duration_s: Optional[float]
    error_msg:  Optional[str]

class LogResponse(BaseModel):
    run_id:   Optional[str]
    log_file: Optional[str]
    lines:    list[str]

class HealthResponse(BaseModel):
    api:        str
    postgres:   str
    bruin:      str
    semaphore:  bool

# ---------------------------------------------------------------------------
# Funzioni helper
# ---------------------------------------------------------------------------

def check_semaphore() -> bool:
    """Verifica se il file semaforo DATASET_READY.txt esiste."""
    return Path(SEMAPHORE_PATH).exists()

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=3,
    )

def check_postgres() -> str:
    try:
        conn = get_db_connection()
        conn.close()
        return "ok"
    except Exception as e:
        return f"error: {e}"

def check_bruin_container() -> str:
    """
    Verifica lo stato del container Bruin interrogando il Docker socket
    direttamente via HTTP Unix socket — non richiede il binario 'docker'.
    """
    try:
        import http.client
        import json
        import socket

        class UnixSocketHTTPConnection(http.client.HTTPConnection):
            def connect(self):
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.connect("/var/run/docker.sock")

        conn = UnixSocketHTTPConnection("localhost")
        conn.request("GET", f"/containers/{BRUIN_CONTAINER}/json")
        resp = conn.getresponse()
        if resp.status == 200:
            data = json.loads(resp.read())
            return data["State"]["Status"]   # "running", "exited", ecc.
        elif resp.status == 404:
            return "not found"
        return f"http {resp.status}"
    except Exception as e:
        return f"error: {e}"

def save_run_to_db(pipeline_name: str, status: str, started_at: datetime,
                   finished_at: Optional[datetime], notes: Optional[str] = None):
    """
    Inserisce o aggiorna il record del run in stg.pipeline_runs.

    Struttura reale della tabella:
        run_id          serial PK (auto-increment — non gestiamo noi)
        pipeline_name   varchar(100)
        started_at      timestamp
        finished_at     timestamp
        status          varchar(30)
        records_loaded  integer      (NULL — non disponibile da FastAPI)
        checks_run      integer      (NULL — non disponibile da FastAPI)
        checks_error    integer      (NULL — non disponibile da FastAPI)
        checks_warning  integer      (NULL — non disponibile da FastAPI)
        notes           text
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO stg.pipeline_runs
                (pipeline_name, started_at, finished_at, status, notes)
            VALUES (%s, %s, %s, %s, %s)
        """, (pipeline_name, started_at, finished_at, status, notes))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass  # Non bloccare l'esecuzione in caso di errore DB

def cleanup_inbox(log_file: Path) -> tuple[int, int]:
    """
    Cancella tutti i file nella cartella inbound (eccetto il semaforo,
    già rimosso in precedenza). Restituisce (file_eliminati, errori).
    """
    inbound     = Path(SEMAPHORE_PATH).parent
    eliminated  = 0
    errors      = 0

    for item in inbound.iterdir():
        try:
            if item.is_file():
                item.unlink()
                eliminated += 1
            elif item.is_dir():
                import shutil
                shutil.rmtree(item)
                eliminated += 1
        except Exception:
            errors += 1

    with open(log_file, "a") as lf:
        lf.write(f"\n[CLEANUP] Inbox svuotata: {eliminated} elementi rimossi, {errors} errori.\n")

    return eliminated, errors


async def execute_bruin_pipeline(run_id: str, log_file: Path, cleanup: bool = False):
    """
    Esegue 'bruin run' nel container mdg_bruin tramite 'docker exec'.
    Usa asyncio.create_subprocess_exec — genuinamente non bloccante,
    uvicorn rimane reattivo durante tutta l'esecuzione.

    Se cleanup=True e exit_code==0, svuota la cartella inbox al termine.
    """
    global state

    cmd = ["docker", "exec", BRUIN_CONTAINER, "bruin", "run", BRUIN_PIPELINE]

    try:
        with open(log_file, "w") as lf:
            lf.write(f"[{datetime.now().isoformat()}] MDG Pipeline — Run ID: {run_id}\n")
            lf.write(f"[{datetime.now().isoformat()}] Cmd: {' '.join(cmd)}\n")
            lf.write(f"[{datetime.now().isoformat()}] Cleanup inbox post-run: {'SI' if cleanup else 'NO'}\n")
            lf.write("-" * 60 + "\n")
            lf.flush()

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        # Streaming riga per riga → log file in tempo reale
        with open(log_file, "a") as lf:
            async for line in process.stdout:
                lf.write(line.decode("utf-8", errors="replace"))
                lf.flush()

        await process.wait()

        state.exit_code = process.returncode
        state.ended_at  = datetime.now()
        state.status    = RunStatus.SUCCESS if process.returncode == 0 else RunStatus.FAILED
        if process.returncode != 0:
            state.error_msg = f"Bruin terminato con exit code {process.returncode}"

        # Rimuovi semaforo dopo il run
        try:
            Path(SEMAPHORE_PATH).unlink(missing_ok=True)
        except Exception:
            pass

        # Cleanup inbox se richiesto e pipeline OK
        if cleanup and process.returncode == 0:
            await asyncio.to_thread(cleanup_inbox, log_file)

    except Exception as e:
        state.status    = RunStatus.FAILED
        state.ended_at  = datetime.now()
        state.exit_code = -1
        state.error_msg = str(e)
        with open(log_file, "a") as lf:
            lf.write(f"\n[ERRORE API] {e}\n")

    finally:
        notes = state.error_msg if state.error_msg else None
        save_run_to_db(
            pipeline_name=BRUIN_PIPELINE,
            status=state.status.value,
            started_at=state.started_at,
            finished_at=state.ended_at,
            notes=notes,
        )

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["Sistema"])
def health():
    """Verifica lo stato di API, Postgres, container Bruin e file semaforo."""
    return HealthResponse(
        api="ok",
        postgres=check_postgres(),
        bruin=check_bruin_container(),
        semaphore=check_semaphore(),
    )


@app.post("/pipeline/run", response_model=RunResponse, tags=["Pipeline"])
async def run_pipeline(background_tasks: BackgroundTasks,
                       force: bool = False,
                       cleanup: bool = False):
    """
    Avvia la pipeline Bruin.

    - `force=true`   — bypassa il controllo del file semaforo
    - `cleanup=true` — svuota la cartella inbox al termine se Bruin esce con successo (exit_code=0)
    - Rifiuta se una pipeline è già in esecuzione
    """
    global state

    # Blocca un secondo run concorrente
    if state.status == RunStatus.RUNNING:
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline già in esecuzione (run_id: {state.run_id}). Attendi il completamento."
        )

    # Controlla il semaforo (a meno che force=true)
    if not force and not check_semaphore():
        raise HTTPException(
            status_code=422,
            detail=f"File semaforo non trovato: {SEMAPHORE_PATH}. "
                   f"Usa ?force=true per bypassare il controllo."
        )

    # Inizializza il nuovo run
    run_id   = str(uuid.uuid4())[:8]
    log_file = LOG_DIR / f"run_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    state.run_id     = run_id
    state.status     = RunStatus.RUNNING
    state.started_at = datetime.now()
    state.ended_at   = None
    state.exit_code  = None
    state.error_msg  = None
    state.log_file   = log_file

    background_tasks.add_task(execute_bruin_pipeline, run_id, log_file, cleanup)

    cleanup_msg = " (cleanup inbox attivo)" if cleanup else ""
    return RunResponse(
        run_id=run_id,
        status=RunStatus.RUNNING,
        started_at=state.started_at,
        message=f"Pipeline avviata{cleanup_msg}. Usa /pipeline/status per monitorare il progresso.",
    )


@app.get("/pipeline/status", response_model=StatusResponse, tags=["Pipeline"])
def pipeline_status():
    """
    Restituisce lo stato corrente (o dell'ultimo run completato).
    """
    duration = None
    if state.started_at and state.ended_at:
        duration = (state.ended_at - state.started_at).total_seconds()
    elif state.started_at and state.status == RunStatus.RUNNING:
        duration = (datetime.now() - state.started_at).total_seconds()

    return StatusResponse(
        run_id=state.run_id,
        status=state.status,
        started_at=state.started_at,
        ended_at=state.ended_at,
        exit_code=state.exit_code,
        duration_s=duration,
        error_msg=state.error_msg,
    )


@app.get("/pipeline/logs", response_model=LogResponse, tags=["Pipeline"])
def pipeline_logs(tail: int = 100):
    """
    Restituisce le ultime N righe del log del run corrente o dell'ultimo run.

    - `tail`: numero di righe dalla fine (default 100, max consigliato 500)
    """
    if not state.log_file or not state.log_file.exists():
        return LogResponse(
            run_id=state.run_id,
            log_file=None,
            lines=["Nessun log disponibile per questo run."],
        )

    with open(state.log_file, "r", errors="replace") as f:
        all_lines = f.readlines()

    return LogResponse(
        run_id=state.run_id,
        log_file=str(state.log_file),
        lines=[l.rstrip("\n") for l in all_lines[-tail:]],
    )


@app.get("/pipeline/runs", tags=["Pipeline"])
def pipeline_runs(limit: int = 20):
    """
    Recupera lo storico dei run da stg.pipeline_runs.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT run_id, pipeline_name, started_at, finished_at,
                   status, records_loaded, checks_run, checks_error,
                   checks_warning, notes
            FROM stg.pipeline_runs
            ORDER BY started_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "run_id":          r[0],
                "pipeline_name":   r[1],
                "started_at":      r[2].isoformat() if r[2] else None,
                "finished_at":     r[3].isoformat() if r[3] else None,
                "status":          r[4],
                "records_loaded":  r[5],
                "checks_run":      r[6],
                "checks_error":    r[7],
                "checks_warning":  r[8],
                "notes":           r[9],
            }
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Errore DB: {e}")


@app.delete("/pipeline/semaphore", tags=["Pipeline"])
def delete_semaphore():
    """Rimuove manualmente il file semaforo (utile per reset/test)."""
    path = Path(SEMAPHORE_PATH)
    if path.exists():
        path.unlink()
        return {"message": f"Semaforo rimosso: {SEMAPHORE_PATH}"}
    return {"message": "Semaforo già assente."}



@app.post("/pipeline/semaphore", tags=["Pipeline"])
def create_semaphore():
    """Crea il file semaforo manualmente (utile per test senza ERP)."""
    path = Path(SEMAPHORE_PATH)
    path.touch()
    return {"message": f"Semaforo creato: {SEMAPHORE_PATH}"}


@app.delete("/files/inbox/{filename}", tags=["File"])
def delete_inbox_file(filename: str):
    """Elimina un singolo file dalla cartella inbound."""
    inbound = Path(SEMAPHORE_PATH).parent
    target  = inbound / filename
    if not target.resolve().is_relative_to(inbound.resolve()):
        raise HTTPException(status_code=400, detail="Nome file non valido.")
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File non trovato: {filename}")
    target.unlink()
    return {"message": f"File eliminato: {filename}"}


@app.delete("/files/inbox", tags=["File"])
def delete_all_inbox_files():
    """Elimina tutti i file dalla cartella inbound."""
    inbound    = Path(SEMAPHORE_PATH).parent
    eliminated = 0
    errors     = 0
    for item in inbound.iterdir():
        try:
            if item.is_file():
                item.unlink()
                eliminated += 1
            elif item.is_dir():
                import shutil
                shutil.rmtree(item)
                eliminated += 1
        except Exception:
            errors += 1
    return {"message": f"Inbox svuotata: {eliminated} eliminati, {errors} errori."}


@app.post("/files/inbox/upload", tags=["File"])
async def upload_inbox_file(file: UploadFile):
    """Carica un file nella cartella inbound."""
    inbound = Path(SEMAPHORE_PATH).parent
    dest    = inbound / file.filename
    if not dest.resolve().is_relative_to(inbound.resolve()):
        raise HTTPException(status_code=400, detail="Nome file non valido.")
    content = await file.read()
    dest.write_bytes(content)
    return {"message": f"File caricato: {file.filename}", "size_kb": round(len(content) / 1024, 1)}


@app.get("/files/inbox", tags=["File"])
def list_inbox():
    """
    Lista i file presenti nella cartella inbound (volume datalake_olderp).
    Restituisce nome, dimensione in KB, data di modifica e tipo.
    """
    inbound = Path(SEMAPHORE_PATH).parent  # /data/inbound

    if not inbound.exists():
        raise HTTPException(status_code=404, detail=f"Cartella non trovata: {inbound}")

    files = []
    for f in sorted(inbound.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        stat = f.stat()
        files.append({
            "name":        f.name,
            "type":        "dir" if f.is_dir() else f.suffix.lstrip(".").upper() or "FILE",
            "size_kb":     round(stat.st_size / 1024, 1),
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "is_semaphore": f.name == Path(SEMAPHORE_PATH).name,
        })

    return {
        "path":  str(inbound),
        "count": len(files),
        "files": files,
    }


@app.get("/files/sap", tags=["File"])
def list_sap():
    """Lista i file presenti nella cartella from_sap (volume datalake_sap)."""
    if not SAP_PATH.exists():
        raise HTTPException(status_code=404, detail=f"Cartella non trovata: {SAP_PATH}")
    files = []
    for f in sorted(SAP_PATH.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        stat = f.stat()
        files.append({
            "name":        f.name,
            "type":        "DIR" if f.is_dir() else f.suffix.lstrip(".").upper() or "FILE",
            "size_kb":     round(stat.st_size / 1024, 1),
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "is_semaphore": False,
        })
    return {"path": str(SAP_PATH), "count": len(files), "files": files}


@app.delete("/files/sap/{filename}", tags=["File"])
def delete_sap_file(filename: str):
    """Elimina un singolo file dalla cartella from_sap."""
    target = SAP_PATH / filename
    if not target.resolve().is_relative_to(SAP_PATH.resolve()):
        raise HTTPException(status_code=400, detail="Nome file non valido.")
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File non trovato: {filename}")
    target.unlink()
    return {"message": f"File eliminato: {filename}"}


@app.delete("/files/sap", tags=["File"])
def delete_all_sap_files():
    """Elimina tutti i file dalla cartella from_sap."""
    eliminated = 0
    errors     = 0
    for item in SAP_PATH.iterdir():
        try:
            if item.is_file():
                item.unlink()
                eliminated += 1
            elif item.is_dir():
                import shutil
                shutil.rmtree(item)
                eliminated += 1
        except Exception:
            errors += 1
    return {"message": f"Cartella SAP svuotata: {eliminated} eliminati, {errors} errori."}


@app.post("/files/sap/upload", tags=["File"])
async def upload_sap_file(file: UploadFile):
    """Carica un file nella cartella from_sap."""
    dest = SAP_PATH / file.filename
    if not dest.resolve().is_relative_to(SAP_PATH.resolve()):
        raise HTTPException(status_code=400, detail="Nome file non valido.")
    content = await file.read()
    dest.write_bytes(content)
    return {"message": f"File caricato: {file.filename}", "size_kb": round(len(content) / 1024, 1)}
