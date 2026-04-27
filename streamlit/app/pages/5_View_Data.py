"""
POSIZIONE: mdg-v0/streamlit/app/pages/5_View_Data.py

View Data — visualizzazione tabelle per schema DB
Filtri: schema → tabella → ricerca testo
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from mdg_auth import require_login, render_sidebar_menu

st.set_page_config(
    page_title="View Data | MDG",
    page_icon="🗄️",
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


# Schemi disponibili (ordine logico della pipeline)
SCHEMAS = ["raw", "stg", "ref", "prd"]

# Tabelle di sistema da escludere
SYSTEM_TABLES = {"check_results", "check_catalog", "pipeline_runs", "check_states"}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown(
    '<h1 style="color:#38BDF8;">🗄️ MDG — View Data</h1>',
    unsafe_allow_html=True,
)
st.caption(":yellow[Visualizzazione contenuto tabelle per schema database.]")
st.divider()

# ---------------------------------------------------------------------------
# Filtri: schema → tabella
# ---------------------------------------------------------------------------
col_schema, col_tab = st.columns([1, 3])

with col_schema:
    selected_schema = st.selectbox(
        "🗂️ Schema",
        options=SCHEMAS,
        index=0,
        key="schema_sel",
    )

# Carica tabelle dello schema selezionato
try:
    df_tables = run_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (selected_schema,))
    tabelle = [t for t in df_tables["table_name"].tolist() if t not in SYSTEM_TABLES]
except Exception as e:
    st.error(f"Errore connessione DB: {e}")
    st.stop()

if not tabelle:
    st.info(f"Nessuna tabella trovata nello schema **{selected_schema}**.")
    st.stop()

with col_tab:
    selected_table = st.selectbox(
        "📋 Tabella",
        options=["— Seleziona —"] + tabelle,
        key="table_sel",
    )

st.divider()

# ---------------------------------------------------------------------------
# Carica e mostra dati
# ---------------------------------------------------------------------------
if not selected_table or selected_table == "— Seleziona —":
    st.info("👆 Seleziona una tabella per visualizzarne il contenuto.")
    st.stop()

# Conteggio righe (aggiornato dopo che filter_col e filter_values sono noti)
try:
    df_count = run_query(f'SELECT COUNT(*) AS n FROM {selected_schema}."{selected_table}"')
    n_rows = int(df_count["n"].iloc[0])
except Exception as e:
    st.error(f"Errore: {e}")
    st.stop()

# Carica colonne disponibili per il filtro dinamico
try:
    df_cols = run_query("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (selected_schema, selected_table))
    available_columns = df_cols["column_name"].tolist()
except Exception:
    available_columns = []

# Metriche + controlli
col_r, col_c, col_max, col_search = st.columns([1, 1, 1, 3])
col_r.metric("Righe totali", n_rows)

max_rows = col_max.number_input(
    "Righe da mostrare", min_value=10, max_value=100000,
    value=n_rows, step=100, key="max_rows"
)

search_text = col_search.text_input(
    "🔍 Cerca nel contenuto",
    placeholder="Testo libero su tutte le colonne...",
    key="search_text",
)

# ---------------------------------------------------------------------------
# Filtro dinamico colonna + multivalore
# ---------------------------------------------------------------------------
with st.expander("🔎 Filtro per colonna", expanded=False):
    fc1, fc2, fc3 = st.columns([2, 4, 1])

    with fc1:
        filter_col = st.selectbox(
            "Colonna",
            options=["— nessun filtro —"] + available_columns,
            key="filter_col",
        )

    filter_values = []
    filter_mode = "include"

    if filter_col and filter_col != "— nessun filtro —":
        # Legge i valori distinti della colonna dal DB
        try:
            df_vals = run_query(
                f'SELECT DISTINCT "{filter_col}" AS val '
                f'FROM {selected_schema}."{selected_table}" '
                f'WHERE "{filter_col}" IS NOT NULL '
                f'ORDER BY 1 '
                f'LIMIT 500',
            )
            distinct_values = df_vals["val"].astype(str).tolist()
        except Exception:
            distinct_values = []

        with fc2:
            selected_values = st.multiselect(
                f"Valori di `{filter_col}`",
                options=["(tutti)"] + distinct_values,
                default=[],
                key="filter_values_ms",
                placeholder="Seleziona uno o più valori...",
            )
            # Se l'utente sceglie "(tutti)" oppure non sceglie nulla → nessun filtro
            if "(tutti)" in selected_values or not selected_values:
                filter_values = []
            else:
                filter_values = selected_values

        with fc3:
            st.markdown("<br>", unsafe_allow_html=True)
            filter_mode = st.radio(
                "Modalità",
                options=["include", "exclude"],
                format_func=lambda x: "✅ Includi" if x == "include" else "❌ Escludi",
                key="filter_mode",
            )

        if filter_values:
            mode_label = "inclusi" if filter_mode == "include" else "esclusi"
            st.caption(
                f"Filtro attivo: **{filter_col}** {mode_label} → "
                + ", ".join(f"`{v}`" for v in filter_values[:10])
                + (f" ... +{len(filter_values)-10} altri" if len(filter_values) > 10 else "")
            )

# Carica dati — il filtro colonna viene applicato direttamente nella query SQL
# per evitare che il LIMIT tagli i record prima del filtro stesso.
try:
    where_clause = ""
    query_params: list = [max_rows]

    if filter_col and filter_col != "— nessun filtro —" and filter_values:
        placeholders = ", ".join(["%s"] * len(filter_values))
        if filter_mode == "include":
            where_clause = f'WHERE "{filter_col}" IN ({placeholders})'
        else:
            where_clause = f'WHERE "{filter_col}" NOT IN ({placeholders})'
        query_params = filter_values + [max_rows]

    df = run_query(
        f'SELECT * FROM {selected_schema}."{selected_table}" {where_clause} LIMIT %s',
        tuple(query_params),
    )
except Exception as e:
    st.error(f"Errore lettura tabella: {e}")
    st.stop()

col_c.metric("Colonne", len(df.columns))

# Applica filtro testo libero (sempre su DataFrame, è una ricerca full-text)
if search_text and not df.empty:
    mask = df.apply(
        lambda col: col.astype(str).str.contains(search_text, case=False, na=False)
    ).any(axis=1)
    df_show = df[mask]
    st.caption(f"Righe visualizzate: {len(df_show)} (filtrate da {len(df)} caricate)")
else:
    df_show = df
    st.caption(f"Righe visualizzate: {len(df_show)}")

if df_show.empty:
    st.info("Nessun record trovato con il filtro applicato.")
else:
    df_show = df_show.reset_index(drop=True)
    df_show.index = df_show.index + 1  # parte da 1
    st.dataframe(df_show, use_container_width=True, hide_index=False)

    csv = df_show.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"⬇️ Scarica CSV ({len(df_show)} righe)",
        data=csv,
        file_name=f"{selected_schema}_{selected_table}.csv",
        mime="text/csv",
    )
