"""
POSIZIONE: mdg-v0/streamlit/app/pages/5_View_Data.py

View Data — visualizzazione tabelle schema raw
Filtri: nome_archivio (prefisso ZIP) e nome_tabella
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

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🗄️ View Data")
st.caption("Visualizzazione contenuto tabelle schema **raw**")
st.divider()

# ---------------------------------------------------------------------------
# Carica lista tabelle raw
# ---------------------------------------------------------------------------
try:
    df_tables = run_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
except Exception as e:
    st.error(f"Errore connessione DB: {e}")
    st.stop()

if df_tables.empty:
    st.info("Nessuna tabella trovata nello schema raw. Avvia la pipeline per caricare i dati.")
    st.stop()

# Carica valori distinti di _zip_source da tutte le tabelle raw
@st.cache_data(ttl=60)
def load_zip_sources():
    queries = []
    for t in df_tables["table_name"].tolist():
        queries.append(f'SELECT DISTINCT "_zip_source" FROM raw."{t}" WHERE "_zip_source" IS NOT NULL')
    if not queries:
        return []
    try:
        df_zips = run_query(" UNION ".join(queries) + " ORDER BY 1")
        return sorted(df_zips["_zip_source"].dropna().unique().tolist())
    except Exception:
        return []

zip_sources = load_zip_sources()

# ---------------------------------------------------------------------------
# Filtri
# ---------------------------------------------------------------------------
col_arch, col_tab = st.columns([2, 3])

with col_arch:
    selected_zip = st.selectbox(
        "📦 Archivio (zip_source)",
        options=["— Tutti —"] + zip_sources,
        index=0,
        key="arch_sel",
    )

# Filtra tabelle: se zip selezionato, mostra solo quelle che contengono quel zip
if selected_zip == "— Tutti —":
    tabelle_filtrate = df_tables["table_name"].tolist()
else:
    tabelle_filtrate = []
    for t in df_tables["table_name"].tolist():
        try:
            df_check = run_query(
                f'SELECT 1 FROM raw."{t}" WHERE "_zip_source" = %s LIMIT 1',
                (selected_zip,)
            )
            if not df_check.empty:
                tabelle_filtrate.append(t)
        except Exception:
            pass

with col_tab:
    selected_table = st.selectbox(
        "📋 Tabella",
        options=["— Seleziona —"] + (tabelle_filtrate if tabelle_filtrate else []),
        key="table_sel",
    )

st.divider()

# ---------------------------------------------------------------------------
# Carica e mostra dati
# ---------------------------------------------------------------------------
if not selected_table or selected_table == "— Seleziona —" or selected_table == "— nessuna tabella —":
    st.info("👆 Seleziona una tabella per visualizzarne il contenuto.")
    st.stop()

# Conteggio righe
try:
    df_count = run_query(f'SELECT COUNT(*) AS n FROM raw."{selected_table}"')
    n_rows = int(df_count["n"].iloc[0])
except Exception as e:
    st.error(f"Errore: {e}")
    st.stop()

# Metriche + controlli sulla stessa riga
col_r, col_c, col_max, col_search = st.columns([1, 1, 1, 3])
col_r.metric("Righe totali", n_rows)

max_rows = col_max.number_input(
    "Righe da mostrare", min_value=10, max_value=5000,
    value=min(500, n_rows), step=100, key="max_rows"
)

search_text = col_search.text_input(
    "🔍 Cerca nel contenuto",
    placeholder="Testo libero su tutte le colonne...",
    key="search_text",
)

# Carica dati
try:
    if selected_zip == "— Tutti —":
        df = run_query(
            f'SELECT * FROM raw."{selected_table}" LIMIT %s',
            (max_rows,)
        )
    else:
        df = run_query(
            f'SELECT * FROM raw."{selected_table}" WHERE "_zip_source" = %s LIMIT %s',
            (selected_zip, max_rows)
        )
except Exception as e:
    st.error(f"Errore lettura tabella: {e}")
    st.stop()

col_c.metric("Colonne", len(df.columns))

# Applica filtro testo libero
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
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    csv = df_show.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"⬇️ Scarica CSV ({len(df_show)} righe)",
        data=csv,
        file_name=f"raw_{selected_table}.csv",
        mime="text/csv",
    )
