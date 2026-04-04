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
    "CHK01_SUPPL": "Fornitori: codice paese (COUNTRY) valorizzato e presente in T005S",
    "CHK02_SUPPL": "Fornitori: coppia paese/regione (COUNTRY+REGION) presente in T005S",
    "CHK03_SUPPL": "Fornitori: Partita IVA mancante per soggetti UE/ExtraUE",
    "CHK04_SUPPL": "Fornitori: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)",
    "CHK01_CUST": "Clienti: codice paese (COUNTRY) valorizzato e presente in T005S",
    "CHK02_CUST": "Clienti: coppia paese/regione (COUNTRY+REGION) presente in T005S",
    "CHK03_CUST": "Clienti: partita IVA mancante per soggetti UE/ExtraUE",
    "CHK04_CUST": "Clienti: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)",
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
    run_id_val = df_run["last_run"].iloc[0]
    run_at_val = df_run["last_run_at"].iloc[0]
    run_at_str = run_at_val.strftime("%d/%m/%Y %H:%M:%S") if hasattr(run_at_val, "strftime") else str(run_at_val)[:19]
    st.markdown(
        f'<p style="font-size:15px; margin-bottom:4px;">'
        f'<strong>Ultimo run:</strong> '
        f'<code style="background:#1a472a; color:#4ade80; padding:2px 7px; border-radius:4px;">#{run_id_val}</code>'
        f'&nbsp;&nbsp;{run_at_str}</p>',
        unsafe_allow_html=True,
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
where = "WHERE category = %s" if category_filter else ""
params_cat = (category_filter,) if category_filter else None

# ---------------------------------------------------------------------------
# KPI globali
# ---------------------------------------------------------------------------
df_kpi = run_query(f"""
    SELECT
        COUNT(*)                                        AS total,
        COUNT(*) FILTER (WHERE status = 'Ok')           AS ok,
        COUNT(*) FILTER (WHERE status = 'Error')        AS errors,
        COUNT(*) FILTER (WHERE status = 'Warning')      AS warnings
    FROM stg.check_results {where}
""", params_cat)

if not df_kpi.empty and int(df_kpi["total"].iloc[0]) > 0:
    total    = int(df_kpi["total"].iloc[0])
    ok       = int(df_kpi["ok"].iloc[0])
    errors   = int(df_kpi["errors"].iloc[0])
    warnings = int(df_kpi["warnings"].iloc[0])
    pct_ok   = round(ok / total * 100, 1)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Controlli totali", total)
    c2.metric("✅ Ok",            ok)
    c3.metric("⚠️ Warning",       warnings)
    c4.metric("❌ Errori",        errors)
    c5.metric("% successo",       f"{pct_ok}%")
    st.divider()

# ---------------------------------------------------------------------------
# Card per ogni CHECK ID
# ---------------------------------------------------------------------------
df_checks = run_query(f"""
    SELECT
        check_id,
        source_table,
        category,
        COUNT(*) FILTER (WHERE status = 'Ok')      AS num_ok,
        COUNT(*) FILTER (WHERE status = 'Warning')  AS num_warning,
        COUNT(*) FILTER (WHERE status = 'Error')    AS num_error,
        COUNT(*)                                     AS total
    FROM stg.check_results
    {where}
    GROUP BY check_id, source_table, category
    ORDER BY check_id
""", params_cat)

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
        check_id    = row["check_id"]
        source_table = row["source_table"]
        category    = row["category"]
        num_ok      = int(row["num_ok"])
        num_warning = int(row["num_warning"])
        num_error   = int(row["num_error"])
        total       = int(row["total"])
        pct_error   = round(num_error / total * 100, 1) if total > 0 else 0
        description = CHECK_DESCRIPTIONS.get(check_id, "—")

        with st.container(border=True):
            col_info, col_ok, col_warn, col_err, col_btn = st.columns([4, 1, 1, 1, 1])

            with col_info:
                st.markdown(
                    f'<span style="font-size:26px; font-weight:700; color:#85B7EB;">'
                    f'{check_id}</span>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<span style="color:#EF9F27; font-size:14px;">{description}</span>',
                    unsafe_allow_html=True,
                )
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

            with col_warn:
                st.metric("⚠️ Warning", num_warning)

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
                    key=f"btn_{check_id}_{source_table}_{category}",
                    type="primary" if (num_error > 0 or num_warning > 0) else "secondary",
                    use_container_width=True,
                ):
                    st.session_state["detail_check_id"]     = check_id
                    st.session_state["detail_source_table"] = source_table
                    st.switch_page("pages/Check_Details.py")
