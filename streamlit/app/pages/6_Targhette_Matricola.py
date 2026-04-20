"""
POSIZIONE: mdg-v0/streamlit/app/pages/6_Targhette_Matricola.py

Targhette Matricola — Multi-Logo Checker
Identifica articoli con più loghi/etichette associati per plant.

Layout:
  Filtri riga 1  → plant, anno vendite
  Filtri riga 2  → TIPO_PROD, FLAG6, FANTASMA (multivalore)
  Filtri riga 3  → CODART (multivalore, valori distinti da A2F)
  KPI cards      → totale articoli archivio | art. con loghi multipli | totale loghi
  Sezione alta   → Q2: articoli con N loghi > 1  [+ export CSV]
  Sezione bassa  → Q1: drill-down sull'articolo selezionato [+ export CSV]

NOTE CACHE:
  Tutte le funzioni DB sono decorate con @st.cache_data(ttl=300).
  I parametri list vengono convertiti in tuple prima della chiamata
  per garantire l'hashability richiesta da Streamlit.
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from mdg_auth import require_login, render_sidebar_menu

st.set_page_config(
    page_title="Targhette Matricola | MDG",
    page_icon="🏷️",
    layout="wide",
)

require_login()
render_sidebar_menu()

# =============================================================================
# Configurazione plant
# =============================================================================
PLANTS = {
    "Bologna": "BO",
    "Faenza":  "FA",
}

# =============================================================================
# DB helpers — stesso pattern MDG
# =============================================================================
def get_db_params():
    return {
        "host":     os.environ["POSTGRES_HOST"],
        "port":     os.environ["POSTGRES_PORT"],
        "dbname":   os.environ["POSTGRES_DB"],
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }

def _run(sql: str, params: tuple = None) -> pd.DataFrame:
    """Esegue una query e ritorna un DataFrame. Parametri solo posizionali (%s)."""
    conn = psycopg2.connect(**get_db_params(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()

# =============================================================================
# Utility
# =============================================================================
def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";", decimal=",").encode("utf-8-sig")

def build_a2f_filter(
    tipo_prod: tuple, flag6: tuple, fantasma: tuple, codart: tuple
) -> tuple:
    """
    Costruisce le condizioni WHERE sui campi di A2F.
    Tutti i parametri sono tuple (hashable) per compatibilita con @st.cache_data.
    Ritorna (conditions_str, params_tuple).
    conditions_str inizia con AND se non vuota, pronta per essere
    appesa a un WHERE gia aperto.
    """
    parts  = []
    params = []

    if tipo_prod:
        ph = ", ".join(["%s"] * len(tipo_prod))
        parts.append(f'a."FLAG-TIPREC" IN ({ph})')
        params.extend(tipo_prod)

    if flag6:
        ph = ", ".join(["%s"] * len(flag6))
        parts.append(f'a."FLAG6" IN ({ph})')
        params.extend(flag6)

    if fantasma:
        ph = ", ".join(["%s"] * len(fantasma))
        parts.append(f'a."FANTASMA" IN ({ph})')
        params.extend(fantasma)

    if codart:
        ph = ", ".join(["%s"] * len(codart))
        parts.append(f'a."CODART" IN ({ph})')
        params.extend(codart)

    conditions = (" AND " + " AND ".join(parts)) if parts else ""
    return conditions, tuple(params)

# =============================================================================
# Query — valori distinti per i filtri (cached 5 min)
# =============================================================================
@st.cache_data(ttl=300)
def load_filter_values(suffix: str) -> dict:
    result = {}
    mappings = [
        ("tipo_prod", '"FLAG-TIPREC"'),
        ("flag6",     '"FLAG6"'),
        ("fantasma",  '"FANTASMA"'),
        ("codart",    '"CODART"'),
    ]
    for key, col in mappings:
        try:
            df = _run(
                f'SELECT DISTINCT {col} AS v FROM ref."A2F_{suffix}" '
                f'WHERE {col} IS NOT NULL ORDER BY 1'
            )
            result[key] = df["v"].tolist() if not df.empty else []
        except Exception:
            result[key] = []
    return result

# =============================================================================
# KPI 1 — totale articoli in A2F che rispettano i filtri (senza join)
# =============================================================================
@st.cache_data(ttl=300)
def load_kpi_articoli(
    suffix: str,
    tipo_prod: tuple, flag6: tuple, fantasma: tuple, codart: tuple
) -> int:
    conditions, params = build_a2f_filter(tipo_prod, flag6, fantasma, codart)
    sql = f'SELECT COUNT(*) AS n FROM ref."A2F_{suffix}" AS a WHERE 1=1{conditions}'
    df  = _run(sql, params if params else None)
    return int(df["n"].iloc[0]) if not df.empty else 0

# =============================================================================
# Query Q2 — articoli con N loghi > 1, con filtri dinamici (cached)
# =============================================================================
@st.cache_data(ttl=300)
def load_q2(
    suffix: str, date_from: str,
    tipo_prod: tuple, flag6: tuple, fantasma: tuple, codart: tuple
) -> pd.DataFrame:
    conditions, a2f_params = build_a2f_filter(tipo_prod, flag6, fantasma, codart)
    sql = f"""
        SELECT
            a."CODART",
            a."DESCRIZIONE"         AS "DESCART",
            a."FLAG-TIPREC"         AS "TIPO_PROD",
            a."FANTASMA",
            a."FLAG6"               AS "MATRICOLA",
            COUNT(DISTINCT
                COALESCE(c."CODET", 'IPH') || COALESCE(c."DESCLI", 'IPH')
            )                       AS "N_LOGHI"
        FROM ref."A2F_{suffix}"          AS a
        LEFT JOIN ref."PUNTFOR_{suffix}" AS b ON b."CODART" = a."CODART"
        LEFT JOIN ref."CLIM_{suffix}"    AS c ON c."CODCLI" = b."CODCLI"
        WHERE NULLIF(b."DATAV", '')::date > %s
          {conditions}
        GROUP BY
            a."CODART",
            a."DESCRIZIONE",
            a."FLAG-TIPREC",
            a."FANTASMA",
            a."FLAG6"
        HAVING COUNT(DISTINCT
            COALESCE(c."CODET", 'IPH') || COALESCE(c."DESCLI", 'IPH')
        ) > 1
        ORDER BY a."CODART"
    """
    params = (date_from,) + a2f_params
    return _run(sql, params)

# =============================================================================
# Query Q1 — dettaglio loghi per articolo selezionato (cached)
# =============================================================================
@st.cache_data(ttl=300)
def load_q1(
    suffix: str, date_from: str, codart_sel: str,
    tipo_prod: tuple, flag6: tuple, fantasma: tuple, codart: tuple
) -> pd.DataFrame:
    conditions, a2f_params = build_a2f_filter(tipo_prod, flag6, fantasma, codart)
    sql = f"""
        SELECT DISTINCT
            a."CODART",
            a."DESCRIZIONE"                  AS "DESCART",
            a."FLAG-TIPREC"                  AS "TIPO_PROD",
            a."FANTASMA",
            a."FLAG6"                        AS "MATRICOLA",
            COALESCE(c."CODET",  'IPH')      AS "CODET",
            COALESCE(c."DESCLI", 'IPH')      AS "LOGO"
        FROM ref."A2F_{suffix}"          AS a
        LEFT JOIN ref."PUNTFOR_{suffix}" AS b ON b."CODART" = a."CODART"
        LEFT JOIN ref."CLIM_{suffix}"    AS c ON c."CODCLI" = b."CODCLI"
        WHERE NULLIF(b."DATAV", '')::date > %s
          AND a."CODART" = %s
          {conditions}
        ORDER BY a."CODART"
    """
    params = (date_from, codart_sel) + a2f_params
    return _run(sql, params)

# =============================================================================
# Layout pagina
# =============================================================================
st.title("🏷️ Targhette Matricola — Multi-Logo Checker")
st.caption("Articoli con più loghi/etichette associati · drill-down per articolo")
st.divider()

# -----------------------------------------------------------------------------
# Riga 1 — Plant + Anno
# -----------------------------------------------------------------------------
col_plant, col_year, _ = st.columns([2, 3, 3])

with col_plant:
    plant_label  = st.selectbox("Plant", options=list(PLANTS.keys()))
    plant_suffix = PLANTS[plant_label]

with col_year:
    current_year = pd.Timestamp.now().year
    year_from    = st.slider(
        "Vendite registrate dal",
        min_value=2016, max_value=current_year,
        value=2016, step=1, format="%d",
    )

date_from = f"{year_from}-01-01"

# -----------------------------------------------------------------------------
# Riga 2 — Tipo Produzione, Matricola (FLAG6), Fantasma
# -----------------------------------------------------------------------------
filter_vals = load_filter_values(plant_suffix)

col_tipo, col_flag6, col_fantasma = st.columns(3)

with col_tipo:
    sel_tipo_prod = st.multiselect(
        "Tipo Produzione",
        options=filter_vals["tipo_prod"],
        default=[],
        placeholder="Tutti",
    )

with col_flag6:
    sel_flag6 = st.multiselect(
        "Matricola (FLAG6)",
        options=filter_vals["flag6"],
        default=["S"] if "S" in filter_vals["flag6"] else [],
        placeholder="Tutti",
    )

with col_fantasma:
    sel_fantasma = st.multiselect(
        "Fantasma",
        options=filter_vals["fantasma"],
        default=[],
        placeholder="Tutti",
    )

# -----------------------------------------------------------------------------
# Riga 3 — Codice Materiale (CODART)
# -----------------------------------------------------------------------------
sel_codart = st.multiselect(
    "Codice Materiale (CODART)",
    options=filter_vals["codart"],
    default=[],
    placeholder="Tutti — seleziona uno o più codici",
)

st.divider()

# -----------------------------------------------------------------------------
# Caricamento dati — tuple per hashability con @st.cache_data
# -----------------------------------------------------------------------------
t_tipo_prod = tuple(sel_tipo_prod)
t_flag6     = tuple(sel_flag6)
t_fantasma  = tuple(sel_fantasma)
t_codart    = tuple(sel_codart)

try:
    with st.spinner("Caricamento dati..."):
        kpi_articoli = load_kpi_articoli(
            plant_suffix, t_tipo_prod, t_flag6, t_fantasma, t_codart
        )
        df_q2 = load_q2(
            plant_suffix, date_from, t_tipo_prod, t_flag6, t_fantasma, t_codart
        )
except Exception as e:
    st.error(f"Errore nel caricamento dati: {e}")
    st.stop()

kpi_multi_art    = len(df_q2)
kpi_loghi        = int(df_q2["N_LOGHI"].sum()) if not df_q2.empty else 0
kpi_normalizzati = (kpi_articoli - kpi_multi_art) + kpi_loghi

# -----------------------------------------------------------------------------
# KPI cards
# -----------------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "📦 Articoli in archivio",
    f"{kpi_articoli:,}".replace(",", "."),
    help="Totale articoli in A2F con i filtri selezionati (senza join vendite/loghi)",
)
c2.metric(
    "🏷️ Con loghi multipli",
    f"{kpi_multi_art:,}".replace(",", "."),
    help="Articoli con N_LOGHI > 1 dopo le join con PUNTFOR e CLIM",
)
c3.metric(
    "🔢 Totale loghi (art. multipli)",
    f"{kpi_loghi:,}".replace(",", "."),
    help="Somma di N_LOGHI per gli articoli con loghi multipli",
)
c4.metric(
    "🔄 Articoli attesi post-normalizzazione",
    f"{kpi_normalizzati:,}".replace(",", "."),
    delta=f"+{kpi_normalizzati - kpi_articoli:,}".replace(",", "."),
    help=(
        "Stima codici dopo normalizzazione: gli articoli con loghi multipli "
        "diventano N codici distinti (uno per logo). "
        "Formula: (art. totali - art. multipli) + somma N_LOGHI"
    ),
)

st.divider()

# -----------------------------------------------------------------------------
# Tabella Q2
# -----------------------------------------------------------------------------
col_title_q2, col_export_q2 = st.columns([6, 1])
with col_title_q2:
    st.subheader(f"📋 Articoli con loghi multipli — Plant {plant_label}")

if df_q2.empty:
    st.info("✅ Nessun articolo con più loghi trovato con i filtri selezionati.")
    st.stop()

st.caption(f"**{kpi_multi_art} articoli** trovati con N_LOGHI > 1")

with col_export_q2:
    st.download_button(
        label="⬇️ Esporta",
        data=to_csv_bytes(df_q2),
        file_name=f"multi_logo_{plant_suffix}_{date_from}.csv",
        mime="text/csv",
        help="Scarica la tabella in formato CSV (separatore: ;)",
    )

event = st.dataframe(
    df_q2,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "CODART":    st.column_config.TextColumn("Cod. Articolo"),
        "DESCART":   st.column_config.TextColumn("Descrizione", width="large"),
        "TIPO_PROD": st.column_config.TextColumn("Tipo Prod."),
        "FANTASMA":  st.column_config.TextColumn("Fantasma"),
        "MATRICOLA": st.column_config.TextColumn("Matricola"),
        "N_LOGHI":   st.column_config.NumberColumn("# Loghi", format="%d"),
    },
)

# -----------------------------------------------------------------------------
# Drill-down Q1
# -----------------------------------------------------------------------------
selected_rows = event.selection.rows

if selected_rows:
    selected_codart = df_q2.iloc[selected_rows[0]]["CODART"]

    st.divider()
    col_title_q1, col_export_q1 = st.columns([6, 1])
    with col_title_q1:
        st.subheader(f"🔍 Dettaglio loghi — `{selected_codart}`")

    try:
        with st.spinner("Caricamento dettaglio..."):
            df_q1 = load_q1(
                plant_suffix, date_from, selected_codart,
                t_tipo_prod, t_flag6, t_fantasma, t_codart,
            )
    except Exception as e:
        st.error(f"Errore nella query di dettaglio: {e}")
        st.stop()

    if df_q1.empty:
        st.warning("Nessun dettaglio trovato per l'articolo selezionato.")
    else:
        st.caption(f"**{len(df_q1)} combinazioni** logo/etichetta trovate")

        with col_export_q1:
            st.download_button(
                label="⬇️ Esporta",
                data=to_csv_bytes(df_q1),
                file_name=f"dettaglio_{selected_codart}_{plant_suffix}_{date_from}.csv",
                mime="text/csv",
                help="Scarica il dettaglio in formato CSV (separatore: ;)",
            )

        st.dataframe(
            df_q1,
            use_container_width=True,
            hide_index=True,
            column_config={
                "CODART":    st.column_config.TextColumn("Cod. Articolo"),
                "DESCART":   st.column_config.TextColumn("Descrizione", width="large"),
                "TIPO_PROD": st.column_config.TextColumn("Tipo Prod."),
                "FANTASMA":  st.column_config.TextColumn("Fantasma"),
                "MATRICOLA": st.column_config.TextColumn("Matricola"),
                "CODET":     st.column_config.TextColumn("Cod. Etichetta"),
                "LOGO":      st.column_config.TextColumn("Logo"),
            },
        )
else:
    st.info("👆 Clicca su una riga per vedere il dettaglio dei loghi associati.")
