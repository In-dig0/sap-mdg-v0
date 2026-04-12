"""
MDG — Migration Data Governance
Pipeline Admin — Controllo pipeline e analisi log
"""

import re
import time
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from mdg_auth import require_login, render_sidebar_menu, require_role

st.set_page_config(
    page_title="Pipeline Admin",
    page_icon="⚙️",
    layout="wide",
)
require_role("it_role")
render_sidebar_menu()

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------
API_BASE  = "http://mdg_fastapi:8000"
LOGS_DIR  = Path("/app/logs")

STATUS_COLORS = {
    "idle":    "🔵",
    "running": "🟡",
    "success": "🟢",
    "failed":  "🔴",
}

# ---------------------------------------------------------------------------
# Client API — tutte protette da try/except
# ---------------------------------------------------------------------------

def api_health():
    try:
        r = requests.get(f"{API_BASE}/health", timeout=3)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_run_pipeline(force: bool = False, cleanup: bool = False):
    try:
        r = requests.post(
            f"{API_BASE}/pipeline/run",
            params={"force": force, "cleanup": cleanup},
            timeout=5,
        )
        r.raise_for_status()
        return r.json(), None
    except requests.HTTPError as e:
        detail = e.response.json().get("detail", str(e))
        return None, detail
    except Exception as e:
        return None, str(e)

def api_status():
    try:
        r = requests.get(f"{API_BASE}/pipeline/status", timeout=3)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_logs(tail: int = 200):
    try:
        r = requests.get(f"{API_BASE}/pipeline/logs", params={"tail": tail}, timeout=5)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_runs_history(limit: int = 20):
    try:
        r = requests.get(f"{API_BASE}/pipeline/runs", params={"limit": limit}, timeout=5)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_create_semaphore():
    try:
        r = requests.post(f"{API_BASE}/pipeline/semaphore", timeout=3)
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_delete_semaphore():
    try:
        r = requests.delete(f"{API_BASE}/pipeline/semaphore", timeout=3)
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_inbox():
    try:
        r = requests.get(f"{API_BASE}/files/inbox", timeout=5)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_delete_file(filename: str):
    try:
        r = requests.delete(f"{API_BASE}/files/inbox/{filename}", timeout=5)
        r.raise_for_status()
        return r.json(), None
    except requests.HTTPError as e:
        return None, e.response.json().get("detail", str(e))
    except Exception as e:
        return None, str(e)

def api_delete_all_files():
    try:
        r = requests.delete(f"{API_BASE}/files/inbox", timeout=5)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_upload_file(file_bytes: bytes, filename: str):
    try:
        r = requests.post(
            f"{API_BASE}/files/inbox/upload",
            files={"file": (filename, file_bytes)},
            timeout=30,
        )
        r.raise_for_status()
        return r.json(), None
    except requests.HTTPError as e:
        return None, e.response.json().get("detail", str(e))
    except Exception as e:
        return None, str(e)

# ---------------------------------------------------------------------------
# Helpers log
# ---------------------------------------------------------------------------

def colorize_line(line: str) -> str:
    if "FAIL" in line or "[ERROR]" in line or "error" in line.lower():
        color = "#e24b4a"
    elif "PASS" in line:
        color = "#4ade80"
    elif "[WARNING]" in line or "[WARN]" in line or "WARN" in line:
        color = "#EF9F27"
    elif "[INFO]" in line:
        color = "#a0aec0"
    else:
        color = "#e2e8f0"
    escaped = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<span style="color:{color};">{escaped}</span>'

def render_log_html(lines: list[str]) -> None:
    html = "<br>".join(colorize_line(l) for l in lines)
    st.markdown(
        f'<div style="background:#0f1117; padding:16px; border-radius:8px; '
        f'font-family:monospace; font-size:12px; line-height:1.6; '
        f'overflow-x:auto; max-height:550px; overflow-y:auto;">'
        f'{html}</div>',
        unsafe_allow_html=True,
    )

def format_log_label(path: Path) -> str:
    name  = path.stem
    match = re.match(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})", name)
    if match:
        y, mo, d, h, mi, s = match.groups()
        size_kb = path.stat().st_size // 1024
        return f"{d}/{mo}/{y} {h}:{mi}:{s}  —  {size_kb} KB"
    return path.name

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("⚙️ MDG — Pipeline Admin")

# ── Stato sistema (sempre visibile in cima) ───────────────────────────────
with st.expander("🩺 Stato sistema", expanded=False):
    health, err = api_health()
    if err:
        st.error(f"API non raggiungibile: {err}")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("API",      health.get("api", "?"))
        c2.metric("Postgres", health.get("postgres", "?"))
        c3.metric("Bruin",    health.get("bruin", "?"))
        sem_ok = health.get("semaphore", False)
        c4.metric("Semaforo", "✅ Presente" if sem_ok else "❌ Assente")

        # Gestione semaforo inline
        if not err:
            s1, s2 = st.columns([1, 1])
            if not sem_ok:
                if s1.button("➕ Crea semaforo (test)", use_container_width=True):
                    _, e = api_create_semaphore()
                    st.toast("Semaforo creato ✅" if not e else f"Errore: {e}")
                    st.rerun()
            else:
                if s2.button("🗑️ Rimuovi semaforo", use_container_width=True):
                    _, e = api_delete_semaphore()
                    st.toast("Semaforo rimosso" if not e else f"Errore: {e}")
                    st.rerun()

st.divider()

# ---------------------------------------------------------------------------
# Tab principali
# ---------------------------------------------------------------------------
tab_ctrl, tab_storico, tab_log_file = st.tabs([
    "▶️  Controllo",
    "🗂️  Storico run",
    "📄  Analisi log",
])

# ===========================================================================
# TAB 1 — CONTROLLO
# ===========================================================================
with tab_ctrl:

    # ── 1. Inbox SFTP ────────────────────────────────────────────────────
    st.subheader("📂 Inbox SFTP")

    inbox_data, err = api_inbox()

    if err:
        st.warning(f"Impossibile leggere la cartella: {err}")
    elif not inbox_data or inbox_data.get("count", 0) == 0:
        st.info("La cartella inbox è vuota.")
    else:
        files       = inbox_data["files"]
        total_kb    = sum(f["size_kb"] for f in files)
        sem_present = any(f["is_semaphore"] for f in files)

        i1, i2, i3 = st.columns(3)
        i1.metric("File presenti",   inbox_data["count"])
        i2.metric("Dimensione tot.", f"{total_kb:.1f} KB")
        i3.metric("Semaforo",        "✅ Presente" if sem_present else "❌ Assente")

        # Tabella con pulsante elimina per ogni file
        h1, h2, h3, h4, h5 = st.columns([3, 1, 1, 2, 1])
        h1.markdown("**Nome file**"); h2.markdown("**Tipo**")
        h3.markdown("**Dim. (KB)**"); h4.markdown("**Modificato**")
        h5.markdown("**Elimina**")
        st.divider()

        for f in files:
            c1, c2, c3, c4, c5 = st.columns([3, 1, 1, 2, 1])
            c1.write(f["name"])
            c2.write(f["type"])
            c3.write(f["size_kb"])
            c4.write(f["modified_at"].replace("T", " ")[:19])
            if c5.button("🗑️", key=f"del_{f['name']}", help=f"Elimina {f['name']}"):
                _, e = api_delete_file(f["name"])
                if e:
                    st.error(f"Errore: {e}")
                else:
                    st.toast(f"✅ '{f['name']}' eliminato.")
                    st.rerun()

        st.divider()
        # Pulsante cancella tutto
        col_ri, col_del, _ = st.columns([1, 1, 3])
        if col_ri.button("🔄 Aggiorna"):
            st.rerun()
        if col_del.button("🗑️ Svuota inbox", type="secondary",
                          help="Elimina tutti i file dalla cartella inbound"):
            if st.session_state.get("confirm_delete_all"):
                _, e = api_delete_all_files()
                if e:
                    st.error(f"Errore: {e}")
                else:
                    st.toast("✅ Inbox svuotata.")
                st.session_state.pop("confirm_delete_all", None)
                st.rerun()
            else:
                st.session_state["confirm_delete_all"] = True
                st.warning("⚠️ Clicca di nuovo **Svuota inbox** per confermare l'eliminazione di tutti i file.")

    if inbox_data and inbox_data.get("count", 0) == 0:
        col_ri, _ = st.columns([1, 4])
        if col_ri.button("🔄 Aggiorna inbox"):
            st.rerun()

    # ── Upload manuale ────────────────────────────────────────────────────
    with st.expander("⬆️ Upload manuale file nella inbox", expanded=False):
        st.caption("Carica uno o più file ZIP / XLSX direttamente nella cartella inbound senza usare un client SFTP.")
        uploaded_files = st.file_uploader(
            "Seleziona file da caricare",
            type=["zip", "xlsx", "csv", "txt"],
            accept_multiple_files=True,
            key="inbox_upload",
        )
        if uploaded_files:
            if st.button("⬆️ Carica nella inbox", type="primary"):
                ok_count = 0
                for uf in uploaded_files:
                    _, e = api_upload_file(uf.read(), uf.name)
                    if e:
                        st.error(f"Errore su '{uf.name}': {e}")
                    else:
                        ok_count += 1
                if ok_count:
                    st.success(f"✅ {ok_count} file caricati nella inbox.")
                    st.rerun()

    st.divider()

    # ── 2. Avvia pipeline ────────────────────────────────────────────────
    st.subheader("▶️ Avvia pipeline")

    force_mode = st.checkbox("Ignora file semaforo", value=False,
                             help="Avvia la pipeline solo se è presente un file semaforo nella inbox ('DATASET_READY.txt').")
    cleanup_mode = st.checkbox("Svuota inbox post-run", value=False,
                               help="Cancella tutti i file dalla cartella inbox se la pipeline termina con successo (exit code 0)")

    col_left, col_center, col_right = st.columns([2, 3, 2])
    with col_center:
        avvia = st.button("🚀 Avvia Pipeline MDG", use_container_width=True, type="primary")

    if avvia:
        result, err = api_run_pipeline(force=force_mode, cleanup=cleanup_mode)
        if err:
            st.error(f"Errore avvio: {err}")
        else:
            st.success(f"Pipeline avviata — Run ID: `{result['run_id']}`")
            if cleanup_mode:
                st.info("🧹 Cleanup inbox attivo: i file verranno eliminati al termine se Bruin ha successo.")
            st.session_state["active_run_id"] = result["run_id"]

    st.divider()

    # ── 3. Stato run corrente ─────────────────────────────────────────────
    st.subheader("📊 Stato run corrente")

    status_data, err = api_status()

    if err:
        st.warning(f"Impossibile recuperare lo stato: {err}")
    else:
        status = status_data.get("status", "idle")
        icon   = STATUS_COLORS.get(status, "⚪")
        st.markdown(f"### {icon} `{status.upper()}`")

        if status_data.get("run_id"):
            c1, c2, c3 = st.columns(3)
            c1.metric("Run ID",    status_data["run_id"])
            dur = status_data.get("duration_s")
            c2.metric("Durata",    f"{dur:.0f}s" if dur else "—")
            c3.metric("Exit code", status_data.get("exit_code", "—"))

        if status_data.get("error_msg"):
            st.error(status_data["error_msg"])

        if status == "running":
            st.info("⏳ Pipeline in esecuzione... aggiornamento automatico tra 5s")
            time.sleep(5)
            st.rerun()

# ===========================================================================
# TAB 2 — STORICO RUN
# ===========================================================================
with tab_storico:

    st.subheader("Storico run da database")

    col_n, col_filter = st.columns([2, 2])
    n_runs     = col_n.slider("Numero di run da mostrare", 5, 50, 20, step=5)
    show_api   = col_filter.checkbox("Mostra anche run API interni", value=False,
                                     help="Mostra le righe con pipeline_name='/project/bruin' inserite da FastAPI")

    history, err = api_runs_history(limit=n_runs)

    if err:
        st.warning(f"Storico non disponibile: {err}")
    elif not history:
        st.info("Nessun run registrato nel database.")
    else:
        df = pd.DataFrame(history)

        # Filtro: nascondi righe FastAPI (pipeline_name = /project/bruin)
        # a meno che l'utente non voglia vederle
        if not show_api:
            df = df[df["pipeline_name"] != "/project/bruin"]

        if df.empty:
            st.info("Nessun run Bruin registrato. Attiva 'Mostra run API interni' per vedere tutti i record.")
        else:
            # KPI aggregati
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Run totali",   len(df))
            k2.metric("✅ Successi",  (df["status"] == "success").sum())
            k3.metric("❌ Falliti",   (df["status"].isin(["failed","done_with_errors"])).sum())
            try:
                df["_start"] = pd.to_datetime(df["started_at"])
                df["_end"]   = pd.to_datetime(df["finished_at"])
                df["_dur_s"] = (df["_end"] - df["_start"]).dt.total_seconds()
                avg_dur = df["_dur_s"].mean()
            except Exception:
                avg_dur = None
            k4.metric("⏱️ Durata media", f"{avg_dur:.0f}s" if avg_dur else "—")

            st.divider()

            display = df.copy()
            display["status"] = display["status"].map(
                lambda s: f"{STATUS_COLORS.get(s, '🟣')} {s}"
            )
            # Formatta date senza decimali
            for col in ["started_at", "finished_at"]:
                if col in display.columns:
                    display[col] = pd.to_datetime(display[col]).dt.strftime("%d/%m/%Y %H:%M:%S")
            display = display.drop(columns=[c for c in ["_start","_end","_dur_s"] if c in display.columns])
            display = display.rename(columns={
                "run_id":         "ID",
                "pipeline_name":  "Pipeline",
                "started_at":     "Avvio",
                "finished_at":    "Fine",
                "status":         "Stato",
                "records_loaded": "Record",
                "checks_run":     "Check totali",
                "checks_error":   "❌ Errori",
                "checks_warning": "⚠️ Warning",
                "notes":          "Note",
            })
            st.dataframe(display, use_container_width=True, hide_index=True)

# ===========================================================================
# TAB 2 — ANALISI LOG DA FILE
# ===========================================================================
with tab_log_file:

    st.subheader("Analisi log da file")
    st.caption(f"Cartella: `{LOGS_DIR}`")

    if not LOGS_DIR.exists():
        st.warning("⚠️ Cartella log non trovata. Verifica il volume Docker.")
        st.stop()

    log_files = sorted(
        [f for f in LOGS_DIR.glob("*mdg_migration_pipeline.log")],
        reverse=True,
    )

    if not log_files:
        st.info("Nessun file di log trovato. Avvia la pipeline Bruin almeno una volta.")
        st.stop()

    # Selezione file
    options = {format_log_label(f): f for f in log_files}
    col_sel, _ = st.columns([3, 3])
    with col_sel:
        selected_label = st.selectbox("Seleziona run", list(options.keys()), index=0)
    selected_file = options[selected_label]

    # Filtri
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        level_filter = st.multiselect(
            "Livello",
            options=["INFO", "WARNING", "ERROR", "PASS", "FAIL"],
            default=[],
            placeholder="Tutti i livelli",
        )
    with col_f2:
        asset_filter = st.text_input("Filtra per asset", placeholder="es. ck001, ingestion")
    with col_f3:
        text_filter  = st.text_input("Cerca nel testo",  placeholder="es. orfani, duplicati")

    # Lettura
    try:
        raw_text = selected_file.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        st.error(f"Errore lettura file: {e}")
        st.stop()

    lines = raw_text.splitlines()

    # Applica filtri
    filtered = []
    for line in lines:
        if level_filter and not any(lvl in line for lvl in level_filter):
            continue
        if asset_filter and asset_filter.lower() not in line.lower():
            continue
        if text_filter and text_filter.lower() not in line.lower():
            continue
        filtered.append(line)

    # KPI
    pass_count = sum(1 for l in lines if "PASS" in l and "assets" not in l.lower())
    fail_count = sum(1 for l in lines if "FAIL" in l and "assets" not in l.lower())
    warn_count = sum(1 for l in lines if "[WARNING]" in l or "[WARN]" in l)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Righe totali", len(lines))
    k2.metric("✅ PASS",      pass_count)
    k3.metric("❌ FAIL",      fail_count)
    k4.metric("⚠️ WARNING",   warn_count)

    st.divider()

    # Caption filtro
    label = f"Righe visualizzate: {len(filtered)}"
    if level_filter or asset_filter or text_filter:
        label += f" (filtrate da {len(lines)} totali)"
    st.caption(label)

    # Visualizzazione (max 2000 righe)
    display_lines = filtered[-2000:] if len(filtered) > 2000 else filtered
    if len(filtered) > 2000:
        st.warning(f"Mostrate le ultime 2000 righe su {len(filtered)}. Usa i filtri per restringere.")

    render_log_html(display_lines)

    # Download
    st.markdown("<br>", unsafe_allow_html=True)
    st.download_button(
        label="⬇️ Scarica log completo",
        data=raw_text.encode("utf-8"),
        file_name=selected_file.name,
        mime="text/plain",
    )
