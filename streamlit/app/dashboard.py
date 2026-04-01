"""
MDG - Migration Data Governance
Dashboard qualità dati | v0 - scheletro iniziale
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

st.set_page_config(
    page_title="MDG — Data Quality Dashboard",
    page_icon="🔍",
    layout="wide"
)

# ---------------------------------------------------------------------------
# Connessione DB
# ---------------------------------------------------------------------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        cursor_factory=RealDictCursor
    )

def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🔍 MDG — Migration Data Governance")
st.caption("Qualità dati migrazione ERP → SAP S/4HANA")

st.divider()

# ---------------------------------------------------------------------------
# KPI principali
# ---------------------------------------------------------------------------
try:
    df_kpi = run_query("""
        SELECT
            COUNT(*)                                        AS total_checks,
            COUNT(*) FILTER (WHERE status = 'Error')       AS errors,
            COUNT(*) FILTER (WHERE status = 'Warning')     AS warnings,
            COUNT(*) FILTER (WHERE status = 'OK')          AS ok
        FROM stg.check_results
    """)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Controlli totali",  int(df_kpi["total_checks"][0]))
    col2.metric("🔴 Errori",         int(df_kpi["errors"][0]))
    col3.metric("🟡 Warning",        int(df_kpi["warnings"][0]))
    col4.metric("🟢 OK",             int(df_kpi["ok"][0]))

except Exception as e:
    st.info("Nessun dato di controllo ancora disponibile. Avvia la pipeline Bruin per popolare i risultati.")
    st.caption(f"Dettaglio: {e}")

st.divider()
st.info("Dashboard in costruzione — i grafici dettagliati saranno disponibili negli step successivi.")
