"""
MDG — Migration Data Governance
Check Results — dettaglio completo per un check
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from mdg_auth import require_login, render_sidebar_menu

st.set_page_config(
    page_title="Check Results",
    page_icon="🔎",
    layout="wide",
)

require_login()
render_sidebar_menu()

def get_db_params():
    return {
        "host":     os.environ["POSTGRES_HOST"],
        "port":     os.environ["POSTGRES_PORT"],
        "dbname":   os.environ["POSTGRES_DB"],
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }

def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg2.connect(**get_db_params(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()

CHECK_DESCRIPTIONS = {
    "CK001": "Fornitori: codice paese (COUNTRY) presente in T005S",
    "CK002": "Fornitori: coppia COUNTRY+REGION presente in T005S",
    "CK003": "Clienti: codice paese COUNTRY(*) presente in T005S",
    "CK004": "Clienti: coppia COUNTRY(*)+REGION presente in T005S",
    "CK201": "Fornitori: partita IVA mancante per soggetti UE/ExtraUE",
    "CK202": "Fornitori: codice fiscale duplicato tra BP diversi",
    "CK203": "Clienti: partita IVA mancante per soggetti UE/ExtraUE",
    "CK204": "Clienti: codice fiscale duplicato tra BP diversi",
    "CK401": "Orfani flusso 01-ZBP-Vettori: LIFNR assente nella master",
    "CK402": "Orfani flusso 04-ZBP-Fornitori: LIFNR assente nella master",
    "CK403": "Orfani flusso 02-ZDM-Clienti: KUNNR assente nella master",
    "CK404": "Orfani flusso 03-ZBP-Clienti: KUNNR assente nella master",
}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🔎 Check Results")
st.divider()

# ---------------------------------------------------------------------------
# Carica lista check disponibili nel DB
# ---------------------------------------------------------------------------
try:
    df_checks_available = run_query("""
        SELECT DISTINCT cr.check_id, cr.source_table
        FROM stg.check_results cr
        ORDER BY cr.check_id, cr.source_table
    """)
except Exception:
    st.info("⚠️ Nessun dato disponibile. Avvia la pipeline Bruin per inizializzare il database.")
    st.stop()

if df_checks_available.empty:
    st.info("Nessun risultato disponibile. Avvia la pipeline Bruin.")
    st.stop()

# Costruisci opzioni dropdown: "CHK01_SUPPL — S_SUPPL_GEN#ZBP_DatiGenerali"
options = [
    f"{row['check_id']}  —  {row['source_table']}"
    for _, row in df_checks_available.iterrows()
]

# Default: usa session_state se disponibile, altrimenti primo elemento
default_idx = 0
ss_check_id     = st.session_state.get("detail_check_id", None)
ss_source_table = st.session_state.get("detail_source_table", None)

if ss_check_id and ss_source_table:
    target = f"{ss_check_id}  —  {ss_source_table}"
    if target in options:
        default_idx = options.index(target)

selected = st.selectbox(
    "Seleziona il controllo",
    options=options,
    index=default_idx,
    key="check_selector",
)

# Estrai check_id e source_table dalla selezione
check_id, source_table = [s.strip() for s in selected.split("  —  ", 1)]

# Aggiorna session_state
st.session_state["detail_check_id"]     = check_id
st.session_state["detail_source_table"] = source_table

st.divider()

# ---------------------------------------------------------------------------
# Header check selezionato
# ---------------------------------------------------------------------------
st.markdown(
    f'<h2 style="margin-bottom:4px;">'
    f'<span style="color:#85B7EB;">{check_id}</span></h2>',
    unsafe_allow_html=True,
)
description = CHECK_DESCRIPTIONS.get(check_id, "—")
st.markdown(
    f'<p style="color:#EF9F27; font-size:18px; font-weight:500; margin:4px 0;">'
    f'{description}</p>',
    unsafe_allow_html=True,
)
st.markdown(
    f'<p style="font-size:16px; margin-top:4px;">Tabella sorgente: '
    f'<code style="font-size:15px; background:#2a2a2a; padding:3px 8px; '
    f'border-radius:4px;">{source_table}</code></p>',
    unsafe_allow_html=True,
)
st.divider()

# ---------------------------------------------------------------------------
# KPI
# ---------------------------------------------------------------------------
df_kpi = run_query("""
    SELECT
        COUNT(*) FILTER (WHERE cr.status = 'Error')   AS num_error,
        COUNT(*) FILTER (WHERE cr.status = 'Warning') AS num_warning,
        COUNT(*) FILTER (WHERE cr.status = 'Ok')      AS num_ok,
        COUNT(*)                                       AS total,
        pr.run_id,
        pr.started_at,
        pr.finished_at
    FROM stg.check_results cr
    JOIN stg.pipeline_runs pr ON pr.run_id = cr.run_id
    WHERE cr.check_id = %s AND cr.source_table = %s
    GROUP BY pr.run_id, pr.started_at, pr.finished_at
    ORDER BY pr.run_id DESC
    LIMIT 1
""", (check_id, source_table))

num_error = num_warning = num_ok = total = 0
if not df_kpi.empty:
    num_error   = int(df_kpi["num_error"].iloc[0])
    num_warning = int(df_kpi["num_warning"].iloc[0])
    num_ok      = int(df_kpi["num_ok"].iloc[0])
    total       = int(df_kpi["total"].iloc[0])
    run_id      = df_kpi["run_id"].iloc[0]
    started_at  = df_kpi["started_at"].iloc[0]
    finished_at = df_kpi["finished_at"].iloc[0]
    pct_error   = round(num_error / total * 100, 1) if total > 0 else 0

    started_str  = started_at.strftime("%d/%m/%Y %H:%M:%S")
    finished_str = finished_at.strftime("%d/%m/%Y %H:%M:%S") if finished_at else "in corso"
    st.markdown(
        f'<p style="font-size:15px; margin-bottom:4px;">'
        f'<strong>Ultimo run:</strong> '
        f'<code style="background:#1a472a; color:#4ade80; padding:2px 7px; border-radius:4px;">#{run_id}</code>'
        f'&nbsp;&nbsp;avviato il {started_str}&nbsp;&nbsp;—&nbsp;&nbsp;completato il {finished_str}</p>',
        unsafe_allow_html=True,
    )
    st.divider()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Totale controllati", total)
    c2.metric("✅ Ok",              num_ok)
    c3.metric("⚠️ Warning",         num_warning)
    c4.metric("❌ Errori",          num_error)
    c5.metric("% errori",           f"{pct_error}%")

st.divider()

# ---------------------------------------------------------------------------
# Filtri
# ---------------------------------------------------------------------------
col_status, col_search, _ = st.columns([2, 3, 3])

with col_status:
    status_filter = st.multiselect(
        "Filtra per stato",
        options=["Error", "Warning", "Ok"],
        default=["Error", "Warning"],
    )

with col_search:
    search_text = st.text_input(
        "Cerca per codice oggetto o messaggio",
        placeholder="es. 08010231"
    )

# ---------------------------------------------------------------------------
# Query risultati
# ---------------------------------------------------------------------------
where_parts = ["check_id = %s", "source_table = %s"]
params      = [check_id, source_table]

if status_filter and len(status_filter) < 3:
    placeholders = ", ".join(["%s"] * len(status_filter))
    where_parts.append(f"status IN ({placeholders})")
    params.extend(status_filter)

if search_text:
    where_parts.append("(object_key ILIKE %s OR message ILIKE %s)")
    params.extend([f"%{search_text}%", f"%{search_text}%"])

where_clause = " AND ".join(where_parts)

df_detail = run_query(f"""
    SELECT
        object_key  AS "Codice oggetto",
        status      AS "Stato",
        message     AS "Messaggio",
        zip_source  AS "File ZIP"
    FROM stg.check_results
    WHERE {where_clause}
    ORDER BY message, object_key
""", tuple(params))

# ---------------------------------------------------------------------------
# Tabella
# ---------------------------------------------------------------------------
n_shown = len(df_detail)
label   = f"Record: {n_shown}"
if status_filter and len(status_filter) < 3:
    label += f" ({', '.join(status_filter)})"
if search_text:
    label += f" — filtro: '{search_text}'"

st.subheader(label)

if df_detail.empty:
    st.info("Nessun record trovato con i filtri selezionati.")
else:
    def highlight_status(row):
        if row["Stato"] == "Error":
            return ["background-color: rgba(226,75,74,0.12)"] * len(row)
        if row["Stato"] == "Warning":
            return ["background-color: rgba(186,117,23,0.15)"] * len(row)
        return ["background-color: rgba(99,153,34,0.08)"] * len(row)

    styled = df_detail.style.apply(highlight_status, axis=1)

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Codice oggetto": st.column_config.TextColumn(width="small"),
            "Stato":          st.column_config.TextColumn(width="small"),
            "Messaggio":      st.column_config.TextColumn(width="large"),
            "File ZIP":       st.column_config.TextColumn(width="small"),
        }
    )

    col_exp1, col_exp2, _ = st.columns([2, 2, 4])
    with col_exp1:
        csv_all = df_detail.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Scarica tutti CSV",
            data=csv_all,
            file_name=f"{check_id}_tutti.csv",
            mime="text/csv",
        )
    with col_exp2:
        df_issues = df_detail[df_detail["Stato"].isin(["Error", "Warning"])]
        if not df_issues.empty:
            csv_issues = df_issues.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Scarica Error+Warning CSV",
                data=csv_issues,
                file_name=f"{check_id}_issues.csv",
                mime="text/csv",
            )
