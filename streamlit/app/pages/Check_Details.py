"""
MDG — Migration Data Governance
Check Details — dettaglio completo Ok + Error per un check
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

st.set_page_config(
    page_title="Check Details",
    page_icon="🔎",
    layout="wide",
)

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
    "CHK01": "Codice paese (COUNTRY) valorizzato e presente in T005S",
    "CHK02": "Coppia paese/regione (COUNTRY+REGION) presente in T005S",
    "CHK03": "Partita IVA mancante per soggetti UE/ExtraUE",
}

# ---------------------------------------------------------------------------
# Navigazione
# ---------------------------------------------------------------------------
if st.button("← Torna alla Dashboard"):
    st.switch_page("Dashboard.py")

st.divider()

check_id     = st.session_state.get("detail_check_id", None)
source_table = st.session_state.get("detail_source_table", None)

if not check_id:
    st.warning("Nessun check selezionato. Torna alla Dashboard e clicca 'Check details'.")
    st.stop()

# ---------------------------------------------------------------------------
# Header con stili
# ---------------------------------------------------------------------------
# Check ID in azzurro chiaro
st.markdown(
    f'<h1 style="margin-bottom:4px;">🔎 Check Details — '
    f'<span style="color:#85B7EB;">{check_id}</span></h1>',
    unsafe_allow_html=True,
)

# Descrizione in giallo
description = CHECK_DESCRIPTIONS.get(check_id, "—")
st.markdown(
    f'<p style="color:#EF9F27; font-size:18px; font-weight:500; margin:4px 0;">'
    f'{description}</p>',
    unsafe_allow_html=True,
)

# Nome tabella più evidente
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
        COUNT(*) FILTER (WHERE status = 'Error') AS num_error,
        COUNT(*) FILTER (WHERE status = 'Ok')    AS num_ok,
        COUNT(*)                                  AS total,
        MAX(run_id)                               AS run_id
    FROM stg.check_results
    WHERE check_id = %s AND source_table = %s
""", (check_id, source_table))

num_error = num_ok = total = 0
run_id = "—"

if not df_kpi.empty:
    num_error = int(df_kpi["num_error"].iloc[0])
    num_ok    = int(df_kpi["num_ok"].iloc[0])
    total     = int(df_kpi["total"].iloc[0])
    run_id    = df_kpi["run_id"].iloc[0]
    pct_error = round(num_error / total * 100, 1) if total > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Totale controllati", total)
    c2.metric("✅ Ok",              num_ok)
    c3.metric("❌ Errori",          num_error)
    c4.metric("% errori",           f"{pct_error}%")
    st.caption(f"Run: `#{run_id}`")

st.divider()

# ---------------------------------------------------------------------------
# Filtri
# ---------------------------------------------------------------------------
col_status, col_search, _ = st.columns([2, 3, 3])

with col_status:
    status_filter = st.selectbox(
        "Filtra per stato",
        options=["Tutti", "Error", "Ok"],
        index=1,
    )

with col_search:
    search_text = st.text_input(
        "Cerca per codice oggetto o messaggio",
        placeholder="es. 08010231"
    )

# ---------------------------------------------------------------------------
# Query con join per _zip_source
# ---------------------------------------------------------------------------
where_parts = ["cr.check_id = %s", "cr.source_table = %s"]
params      = [check_id, source_table]

if status_filter != "Tutti":
    where_parts.append("cr.status = %s")
    params.append(status_filter)

if search_text:
    where_parts.append("(cr.object_key ILIKE %s OR cr.message ILIKE %s)")
    params.extend([f"%{search_text}%", f"%{search_text}%"])

where_clause = " AND ".join(where_parts)

df_detail = run_query(f"""
    SELECT
        cr.object_key       AS "Codice oggetto",
        cr.status           AS "Stato",
        cr.message          AS "Messaggio",
        raw."_zip_source"   AS "File ZIP",
        cr.created_at       AS "Rilevato il"
    FROM stg.check_results cr
    LEFT JOIN raw."{source_table}" raw
        ON raw."LIFNR(k/*)" = cr.object_key
    WHERE {where_clause}
    ORDER BY cr.status DESC, cr.object_key
""", tuple(params))

# ---------------------------------------------------------------------------
# Tabella
# ---------------------------------------------------------------------------
n_shown = len(df_detail)
label   = f"Record: {n_shown}"
if status_filter != "Tutti":
    label += f" ({status_filter})"
if search_text:
    label += f" — filtro: '{search_text}'"

st.subheader(label)

if df_detail.empty:
    st.info("Nessun record trovato con i filtri selezionati.")
else:
    def highlight_status(row):
        if row["Stato"] == "Error":
            return ["background-color: rgba(226,75,74,0.12)"] * len(row)
        return ["background-color: rgba(99,153,34,0.08)"] * len(row)

    styled = df_detail.style.apply(highlight_status, axis=1)

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Codice oggetto": st.column_config.TextColumn(width="medium"),
            "Stato":          st.column_config.TextColumn(width="small"),
            "Messaggio":      st.column_config.TextColumn(width="large"),
            "File ZIP":       st.column_config.TextColumn(width="medium"),
            "Rilevato il":    st.column_config.DatetimeColumn(
                format="DD/MM/YYYY HH:mm:ss", width="medium"
            ),
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
        df_errors_only = df_detail[df_detail["Stato"] == "Error"]
        if not df_errors_only.empty:
            csv_err = df_errors_only.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Scarica solo errori CSV",
                data=csv_err,
                file_name=f"{check_id}_errori.csv",
                mime="text/csv",
            )
