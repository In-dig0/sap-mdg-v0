"""
MDG — Migration Data Governance
Dashboard qualità dati — Pagina dettaglio errori
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

st.set_page_config(
    page_title="MDG — Dettaglio errori",
    page_icon="❌",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Connessione DB — nuova connessione ad ogni query (evita "connection closed")
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Recupera parametri dalla session state
# ---------------------------------------------------------------------------
check_id     = st.session_state.get("detail_check_id", None)
source_table = st.session_state.get("detail_source_table", None)

if st.button("← Torna alla dashboard"):
    st.switch_page("dashboard.py")

st.divider()

if not check_id:
    st.warning("Nessun check selezionato. Torna alla dashboard e clicca 'Vedi dettaglio'.")
    st.stop()

# ---------------------------------------------------------------------------
# Header pagina
# ---------------------------------------------------------------------------
CHECK_DESCRIPTIONS = {
    "CHK01": "Codice paese (COUNTRY) valorizzato e presente in T005S",
    "CHK02": "Coppia paese/regione (COUNTRY+REGION) presente in T005S",
    "CHK03": "Partita IVA mancante per soggetti UE/ExtraUE",
}

st.title(f"❌ Dettaglio errori — {check_id}")
st.markdown(f"**{CHECK_DESCRIPTIONS.get(check_id, '')}**")
st.caption(f"Tabella sorgente: `{source_table}`")
st.divider()

# ---------------------------------------------------------------------------
# KPI errori del check selezionato
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

num_error = 0
num_ok    = 0
total     = 0
run_id    = "—"

if not df_kpi.empty:
    num_error = int(df_kpi["num_error"].iloc[0])
    num_ok    = int(df_kpi["num_ok"].iloc[0])
    total     = int(df_kpi["total"].iloc[0])
    run_id    = df_kpi["run_id"].iloc[0]
    pct_error = round(num_error / total * 100, 1) if total > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Totale record controllati", total)
    c2.metric("✅ Ok",                     num_ok)
    c3.metric("❌ In errore",              num_error)
    c4.metric("% errori",                  f"{pct_error}%")
    st.caption(f"Run: `{run_id}`")

st.divider()

# ---------------------------------------------------------------------------
# Tabella dettaglio record in errore
# ---------------------------------------------------------------------------
st.subheader(f"Record con errori ({num_error})")

df_errors = run_query("""
    SELECT
        object_key  AS "Codice oggetto",
        message     AS "Messaggio errore",
        created_at  AS "Rilevato il"
    FROM stg.check_results
    WHERE check_id     = %s
      AND source_table = %s
      AND status       = 'Error'
    ORDER BY object_key
""", (check_id, source_table))

if df_errors.empty:
    st.success("Nessun errore trovato per questo controllo.")
else:
    st.dataframe(
        df_errors,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Codice oggetto":   st.column_config.TextColumn(width="small"),
            "Messaggio errore": st.column_config.TextColumn(width="large"),
            "Rilevato il":      st.column_config.DatetimeColumn(
                format="DD/MM/YYYY HH:mm:ss", width="medium"
            ),
        }
    )

    csv = df_errors.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Scarica errori CSV",
        data=csv,
        file_name=f"{check_id}_errori.csv",
        mime="text/csv",
    )
