"""
MDG — Migration Data Governance
Pipeline Log — visualizza il log dell'ultimo run Bruin
"""

import os
import re
import streamlit as st
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Pipeline Log",
    page_icon="📄",
    layout="wide",
)

LOGS_DIR = Path("/app/logs")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("📄 Pipeline Log")
st.caption("Log degli ultimi run della pipeline Bruin MDG")
st.divider()

# ---------------------------------------------------------------------------
# Trova tutti i log disponibili
# ---------------------------------------------------------------------------
if not LOGS_DIR.exists():
    st.warning("⚠️ Cartella log non trovata. Verifica il volume Docker.")
    st.stop()

log_files = sorted(
    [f for f in LOGS_DIR.glob("*mdg_migration_pipeline.log")],
    reverse=True  # più recente prima
)

if not log_files:
    st.info("Nessun file di log trovato. Avvia la pipeline Bruin.")
    st.stop()

# ---------------------------------------------------------------------------
# Selezione log
# ---------------------------------------------------------------------------
def format_log_label(path: Path) -> str:
    """Trasforma il nome file in etichetta leggibile."""
    name = path.stem  # es. 2026_04_05_09_17_14__mdg_migration_pipeline
    match = re.match(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})", name)
    if match:
        y, mo, d, h, mi, s = match.groups()
        size_kb = path.stat().st_size // 1024
        return f"{d}/{mo}/{y} {h}:{mi}:{s}  —  {size_kb} KB"
    return path.name

options = {format_log_label(f): f for f in log_files}

col_sel, col_info = st.columns([3, 5])
with col_sel:
    selected_label = st.selectbox(
        "Seleziona run",
        options=list(options.keys()),
        index=0,
    )

selected_file = options[selected_label]

# ---------------------------------------------------------------------------
# Filtri sul contenuto
# ---------------------------------------------------------------------------
col_f1, col_f2, col_f3, _ = st.columns([2, 2, 2, 2])

with col_f1:
    level_filter = st.multiselect(
        "Livello",
        options=["INFO", "WARNING", "ERROR", "PASS", "FAIL"],
        default=[],
        placeholder="Tutti i livelli",
    )

with col_f2:
    asset_filter = st.text_input(
        "Filtra per asset",
        placeholder="es. ck001, ingestion",
    )

with col_f3:
    text_filter = st.text_input(
        "Cerca nel testo",
        placeholder="es. orfani, Error",
    )

# ---------------------------------------------------------------------------
# Lettura e parsing del log
# ---------------------------------------------------------------------------
try:
    raw_text = selected_file.read_text(encoding="utf-8", errors="replace")
except Exception as e:
    st.error(f"Errore lettura file: {e}")
    st.stop()

lines = raw_text.splitlines()

# Applica filtri
filtered = []
for line in lines:
    if level_filter:
        if not any(lvl in line for lvl in level_filter):
            continue
    if asset_filter:
        if asset_filter.lower() not in line.lower():
            continue
    if text_filter:
        if text_filter.lower() not in line.lower():
            continue
    filtered.append(line)

# ---------------------------------------------------------------------------
# Statistiche rapide
# ---------------------------------------------------------------------------
pass_count = sum(1 for l in lines if "PASS" in l and "assets" not in l.lower())
fail_count = sum(1 for l in lines if "FAIL" in l and "assets" not in l.lower())
warn_count = sum(1 for l in lines if "[WARNING]" in l or "[WARN]" in l)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Righe totali",   len(lines))
c2.metric("✅ PASS",        pass_count)
c3.metric("❌ FAIL",        fail_count)
c4.metric("⚠️ WARNING",     warn_count)

st.divider()

# ---------------------------------------------------------------------------
# Visualizzazione log
# ---------------------------------------------------------------------------
n_filtered = len(filtered)
label = f"Righe visualizzate: {n_filtered}"
if level_filter or asset_filter or text_filter:
    label += f" (filtrate da {len(lines)} totali)"
st.caption(label)

# Colorazione sintattica con HTML
def colorize(line: str) -> str:
    if "FAIL" in line or "[ERROR]" in line:
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

# Mostra max 2000 righe per performance
display_lines = filtered[-2000:] if len(filtered) > 2000 else filtered
if len(filtered) > 2000:
    st.warning(f"Mostrate le ultime 2000 righe su {len(filtered)}. Usa i filtri per restringere.")

html_lines = "<br>".join(colorize(l) for l in display_lines)
st.markdown(
    f'<div style="background:#1a1a2e; padding:16px; border-radius:8px; '
    f'font-family:monospace; font-size:12px; line-height:1.6; '
    f'overflow-x:auto; max-height:600px; overflow-y:auto;">'
    f'{html_lines}'
    f'</div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
st.markdown("<br>", unsafe_allow_html=True)
st.download_button(
    label="⬇️ Scarica log completo",
    data=raw_text.encode("utf-8"),
    file_name=selected_file.name,
    mime="text/plain",
)
