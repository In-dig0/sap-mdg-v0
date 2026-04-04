"""
MDG — Migration Data Governance
Dashboard qualità dati — Home page
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

st.set_page_config(
    page_title="Dashboard",
    page_icon="🔍",
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
# Header
# ---------------------------------------------------------------------------
st.title("🔍 MDG — Data Quality Dashboard")
st.caption("Migrazione ERP legacy → SAP S/4HANA — Risultati controlli qualità dati")

df_run = run_query("""
    SELECT MAX(run_id) AS last_run, MAX(started_at) AS last_run_at
    FROM stg.pipeline_runs WHERE status != 'running'
""")
if not df_run.empty and df_run["last_run"].iloc[0]:
    st.markdown(
        f"**Ultimo run:** `#{df_run['last_run'].iloc[0]}`"
        f"  —  {df_run['last_run_at'].iloc[0]}"
    )
st.divider()

# ---------------------------------------------------------------------------
# Filtro Categoria
# ---------------------------------------------------------------------------
df_categories = run_query(
    "SELECT DISTINCT category FROM stg.check_results ORDER BY category"
)
categories = (
    ["Tutti"] + list(df_categories["category"])
    if not df_categories.empty else ["Tutti"]
)

col_filter, _ = st.columns([2, 6])
with col_filter:
    selected_category = st.selectbox(
        "Filtra per categoria", options=categories, index=0
    )

category_filter = None if selected_category == "Tutti" else selected_category

# ---------------------------------------------------------------------------
# KPI globali
# ---------------------------------------------------------------------------
sql_kpi = """
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE status = 'Error') AS errors,
           COUNT(*) FILTER (WHERE status = 'Ok')    AS ok
    FROM stg.check_results
    {where}
""".format(where="WHERE category = %s" if category_filter else "")

df_kpi = run_query(sql_kpi, (category_filter,) if category_filter else None)

if not df_kpi.empty and int(df_kpi["total"].iloc[0]) > 0:
    total  = int(df_kpi["total"].iloc[0])
    errors = int(df_kpi["errors"].iloc[0])
    ok     = int(df_kpi["ok"].iloc[0])
    pct_ok = round(ok / total * 100, 1)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Controlli totali", total)
    c2.metric("✅ Ok",            ok)
    c3.metric("❌ Errori",        errors)
    c4.metric("% successo",       f"{pct_ok}%")
    st.divider()

# ---------------------------------------------------------------------------
# Card per ogni CHECK ID
# ---------------------------------------------------------------------------
sql_checks = """
    SELECT check_id, source_table, category,
           COUNT(*) FILTER (WHERE status = 'Ok')    AS num_ok,
           COUNT(*) FILTER (WHERE status = 'Error') AS num_error,
           COUNT(*)                                  AS total
    FROM stg.check_results
    {where}
    GROUP BY check_id, source_table, category
    ORDER BY check_id
""".format(where="WHERE category = %s" if category_filter else "")

df_checks = run_query(
    sql_checks, (category_filter,) if category_filter else None
)

if df_checks.empty:
    st.info("Nessun risultato. Avvia la pipeline Bruin.")
else:
    label = (
        f"Categoria: {selected_category}"
        if selected_category != "Tutti"
        else "Tutti i controlli"
    )
    st.subheader(f"Risultati per controllo — {label}")

    for _, row in df_checks.iterrows():
        check_id     = row["check_id"]
        source_table = row["source_table"]
        category     = row["category"]
        num_ok       = int(row["num_ok"])
        num_error    = int(row["num_error"])
        total        = int(row["total"])
        pct_error    = round(num_error / total * 100, 1) if total > 0 else 0
        description  = CHECK_DESCRIPTIONS.get(check_id, "—")

        with st.container(border=True):
            col_info, col_ok, col_err, col_btn = st.columns([4, 1, 1, 1])

            with col_info:
                # Check ID in azzurro chiaro
                st.markdown(
                    f'<span style="font-size:26px; font-weight:700; '
                    f'color:#85B7EB;">{check_id}</span>',
                    unsafe_allow_html=True,
                )
                # Descrizione in giallo
                st.markdown(
                    f'<span style="color:#EF9F27; font-size:14px;">'
                    f'{description}</span>',
                    unsafe_allow_html=True,
                )
                # Tabella e categoria sulla stessa riga
                st.markdown(
                    f'<span style="font-size:12px; color:gray;">Tabella: </span>'
                    f'<code style="font-size:12px;">{source_table}</code>'
                    f'&nbsp;&nbsp;&nbsp;'
                    f'<span style="font-size:12px; color:gray;">Categoria: </span>'
                    f'<span style="font-size:14px; font-weight:600;">{category}</span>',
                    unsafe_allow_html=True,
                )

            with col_ok:
                st.metric("✅ Ok", num_ok)

            with col_err:
                st.metric(
                    "❌ Errori", num_error,
                    delta=f"{pct_error}%" if num_error > 0 else None,
                    delta_color="inverse",
                )

            with col_btn:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button(
                    "Check details",
                    key=f"btn_{check_id}_{category}",
                    type="primary" if num_error > 0 else "secondary",
                    use_container_width=True,
                ):
                    st.session_state["detail_check_id"]     = check_id
                    st.session_state["detail_source_table"] = source_table
                    st.switch_page("pages/Check_Details.py")
