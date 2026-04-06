"""
MDG — Migration Data Governance
Check Catalog — visualizza e gestisce i controlli attivi della pipeline
"""

import os
import json
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from mdg_auth import require_login, render_sidebar_menu

st.set_page_config(
    page_title="Check Catalog",
    page_icon="📋",
    layout="wide",
)

require_login()
render_sidebar_menu()

STATES_PATH = "/app/config/check_states.json"

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

def save_state(check_id: str, new_state: bool):
    """Aggiorna DB e JSON in modo sincrono."""
    # 1. Aggiorna DB
    conn = psycopg2.connect(**get_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE stg.check_catalog
                SET is_active = %s, updated_at = NOW()
                WHERE check_id = %s
            """, (new_state, check_id))
        conn.commit()
    finally:
        conn.close()

    # 2. Aggiorna JSON
    try:
        if os.path.exists(STATES_PATH):
            with open(STATES_PATH, "r") as f:
                states = json.load(f)
        else:
            states = {}
        states[check_id] = new_state
        with open(STATES_PATH, "w") as f:
            json.dump(states, f, indent=2)
    except Exception as e:
        st.warning(f"DB aggiornato ma impossibile scrivere {STATES_PATH}: {e}")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("📋 Check Catalog")
st.caption("Elenco dei controlli di qualità configurati nella pipeline MDG")
st.divider()

# ---------------------------------------------------------------------------
# Filtri
# ---------------------------------------------------------------------------
col_type, col_sev, col_active, _ = st.columns([2, 2, 2, 2])

with col_type:
    type_filter = st.selectbox(
        "Tipo controllo",
        options=["Tutti", "SAP_REF", "EXISTENCE", "CROSS_TABLE"],
        index=0,
    )

with col_sev:
    sev_filter = st.selectbox(
        "Severità",
        options=["Tutti", "Error", "Warning"],
        index=0,
    )

with col_active:
    active_filter = st.selectbox(
        "Stato",
        options=["Tutti", "Attivi", "Inattivi"],
        index=0,
    )

# ---------------------------------------------------------------------------
# Query catalogo
# ---------------------------------------------------------------------------
where_parts = ["1=1"]
params = []

if type_filter != "Tutti":
    where_parts.append("check_type = %s")
    params.append(type_filter)
if sev_filter != "Tutti":
    where_parts.append("severity = %s")
    params.append(sev_filter)
if active_filter == "Attivi":
    where_parts.append("is_active = TRUE")
elif active_filter == "Inattivi":
    where_parts.append("is_active = FALSE")

where_clause = " AND ".join(where_parts)

try:
    df = run_query(f"""
        SELECT check_id, check_desc, check_type, severity,
               target_table, target_field, ref_table, is_active, updated_at
        FROM stg.check_catalog
        WHERE {where_clause}
        ORDER BY check_type, check_id
    """, tuple(params) if params else None)
except Exception:
    st.info("⚠️ Nessun dato disponibile. Avvia la pipeline Bruin per inizializzare il database.")
    st.stop()

if df.empty:
    st.info("Nessun check trovato con i filtri selezionati.")
    st.stop()

# ---------------------------------------------------------------------------
# KPI
# ---------------------------------------------------------------------------
total    = len(df)
attivi   = int(df["is_active"].sum())
inattivi = total - attivi

c1, c2, c3 = st.columns(3)
c1.metric("Check totali", total)
c2.metric("✅ Attivi",    attivi)
c3.metric("⏸️ Inattivi",  inattivi)
st.divider()

# ---------------------------------------------------------------------------
# Legenda
# ---------------------------------------------------------------------------
with st.expander("ℹ️ Legenda tipologie controllo", expanded=False):
    st.markdown("""
    | Tipo | Descrizione |
    |------|-------------|
    | **SAP_REF** | Coerenza con le tabelle di riferimento SAP (es. T005S per paese/regione) |
    | **EXISTENCE** | Esistenza del dato obbligatorio o duplicazione non ammessa |
    | **CROSS_TABLE** | Coerenza referenziale tra tabelle dello stesso archivio ZIP |
    """)

# ---------------------------------------------------------------------------
# Card per ogni check con toggle
# ---------------------------------------------------------------------------
TYPE_COLORS = {
    "SAP_REF":     "#4A90D9",
    "EXISTENCE":   "#E8A838",
    "CROSS_TABLE": "#7B68EE",
}
TYPE_LABELS = {
    "SAP_REF":     "🔵 SAP_REF",
    "EXISTENCE":   "🟡 EXISTENCE",
    "CROSS_TABLE": "🟣 CROSS_TABLE",
}

for check_type in ["SAP_REF", "EXISTENCE", "CROSS_TABLE"]:
    df_type = df[df["check_type"] == check_type]
    if df_type.empty:
        continue

    st.markdown(
        f'<h3 style="color:{TYPE_COLORS[check_type]}; margin-top:16px;">'
        f'{TYPE_LABELS[check_type]}</h3>',
        unsafe_allow_html=True,
    )

    for _, row in df_type.iterrows():
        check_id  = row["check_id"]
        is_active = bool(row["is_active"])
        severity  = row["severity"]
        sev_color = "#e24b4a" if severity == "Error" else "#E8A838"
        updated_at = row["updated_at"]

        with st.container(border=True):
            col_info, col_meta, col_toggle = st.columns([4, 3, 1])

            with col_info:
                st.markdown(
                    f'<span style="font-size:18px; font-weight:700; color:#85B7EB;">'
                    f'{check_id}</span>'
                    f'&nbsp;&nbsp;<span style="font-size:12px; color:{sev_color}; '
                    f'font-weight:600;">{severity}</span>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<span style="color:#EF9F27; font-size:14px;">'
                    f'{row["check_desc"]}</span>',
                    unsafe_allow_html=True,
                )

            with col_meta:
                st.markdown("<br>", unsafe_allow_html=True)
                st.caption(f"Tabella: `{row['target_table']}`")
                st.caption(f"Campo: `{row['target_field']}`")
                if row["ref_table"]:
                    st.caption(f"Ref: `{row['ref_table']}`")

            with col_toggle:
                st.markdown("<br><br>", unsafe_allow_html=True)
                new_state = st.toggle(
                    "Attivo",
                    value=is_active,
                    key=f"toggle_{check_id}",
                )
                if new_state != is_active:
                    save_state(check_id, new_state)
                    action = "attivato ✅" if new_state else "disattivato ⏸️"
                    st.toast(f"{check_id} {action}", icon="💾")
                    st.rerun()

        updated_str = updated_at.strftime('%d/%m/%Y %H:%M') if hasattr(updated_at, 'strftime') else str(updated_at)[:16]
        st.caption(f"Aggiornato il: {updated_str}")
