"""
POSIZIONE: mdg-v0/streamlit/app/pages/7_Targhette_Diba.py

Targhette Matricola — Multi-Logo Checker con DIBA ricorsiva
Espande la logica della pagina 6 includendo i componenti con MATRICOLA='S'
nella gerarchia DIBA, moltiplicando i loghi del padre per i discendenti
con matricola.

Logica:
  Per ogni articolo venduto (PUNTFOR):
    1. Calcola i loghi distinti (CLIM)
    2. Trova via DIBA ricorsiva tutti i discendenti con MATRICOLA='S'
       (incluso il padre se ha FLAG6='S' in A2F)
    3. Coppie finali = N_discendenti_matricola × N_loghi
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from mdg_auth import require_login, render_sidebar_menu

st.set_page_config(
    page_title="Targhette + DIBA | MDG",
    page_icon="🔩",
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
# DB helpers
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
    conn = psycopg2.connect(**get_db_params(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";", decimal=",").encode("utf-8-sig")

# =============================================================================
# Query principale con DIBA ricorsiva
# =============================================================================
@st.cache_data(ttl=300)
def load_diba(
    suffix: str, date_from: str,
    tipo_prod: tuple, flag6: tuple, fantasma: tuple, codart: tuple,
    solo_multipli: bool = False,
) -> pd.DataFrame:
    """
    Per ogni articolo venduto (A2F × PUNTFOR × CLIM) espande la gerarchia
    DIBA per trovare tutti i discendenti con MATRICOLA='S', incluso il padre
    se ha FLAG6='S'.

    Risultato: una riga per ogni coppia (CODART_CON_MATRICOLA, LOGO)
    aggregata per articolo padre.
    """

    # Filtri A2F
    a2f_parts  = []
    a2f_params = []
    if tipo_prod:
        ph = ", ".join(["%s"] * len(tipo_prod))
        a2f_parts.append(f'a."FLAG-TIPREC" IN ({ph})')
        a2f_params.extend(tipo_prod)
    if flag6:
        ph = ", ".join(["%s"] * len(flag6))
        a2f_parts.append(f'a."FLAG6" IN ({ph})')
        a2f_params.extend(flag6)
    if fantasma:
        ph = ", ".join(["%s"] * len(fantasma))
        a2f_parts.append(f'a."FANTASMA" IN ({ph})')
        a2f_params.extend(fantasma)
    if codart:
        ph = ", ".join(["%s"] * len(codart))
        a2f_parts.append(f'a."CODART" IN ({ph})')
        a2f_params.extend(codart)

    a2f_conditions = (" AND " + " AND ".join(a2f_parts)) if a2f_parts else ""
    having = "HAVING COUNT(DISTINCT logo) > 1" if solo_multipli else ""

    sql = f"""
    WITH
    -- Step 1: articoli venduti con i loro loghi
    vendite_loghi AS (
        SELECT
            a."CODART"                                      AS codart_rif,
            a."DESCRIZIONE"                                 AS descart,
            a."FLAG-TIPREC"                                 AS tipo_prod,
            a."FANTASMA"                                    AS fantasma,
            a."FLAG6"                                       AS flag6_padre,
            COALESCE(c."CODET",  'IPH') || '|' ||
            COALESCE(c."DESCLI", 'IPH')                     AS logo_key,
            COALESCE(c."DESCLI", 'IPH')                     AS logo
        FROM ref."A2F_{suffix}" AS a
        INNER JOIN ref."PUNTFOR_{suffix}" AS b ON b."CODART" = a."CODART"
        LEFT  JOIN ref."CLIM_{suffix}"    AS c ON c."CODCLI" = b."CODCLI"
        WHERE NULLIF(b."DATAV", '') IS NOT NULL
          AND b."DATAV"::date > %s
          {a2f_conditions}
    ),

    -- Step 2: loghi distinti per articolo padre
    loghi_per_padre AS (
        SELECT
            codart_rif,
            descart,
            tipo_prod,
            fantasma,
            flag6_padre,
            COUNT(DISTINCT logo_key)                        AS n_loghi,
            STRING_AGG(DISTINCT logo, ', ' ORDER BY logo)   AS loghi
        FROM vendite_loghi
        GROUP BY codart_rif, descart, tipo_prod, fantasma, flag6_padre
    ),

    -- Step 3: tutti i codici con MATRICOLA='S' per ogni padre
    --         (padre stesso se FLAG6='S' + figli DIBA con MATRICOLA='S')
    codici_matricola AS (
        -- Padre se ha FLAG6='S'
        SELECT
            lp.codart_rif                   AS codart_rif,
            lp.codart_rif                   AS codart_mat,
            'padre'                         AS tipo_nodo,
            0                               AS livello
        FROM loghi_per_padre lp
        WHERE lp.flag6_padre = 'S'

        UNION ALL

        -- Figli DIBA con MATRICOLA='S' a qualsiasi livello
        SELECT
            d."CODART_RIF"                  AS codart_rif,
            d."CODART_F"                    AS codart_mat,
            'figlio'                        AS tipo_nodo,
            d."LIVELLO"::int                AS livello
        FROM ref."DIBA_{suffix}" d
        INNER JOIN loghi_per_padre lp ON lp.codart_rif = d."CODART_RIF"
        WHERE d."MATRICOLA" = 'S'
    ),

    -- Step 4: aggregazione per padre
    agg_matricola AS (
        SELECT
            codart_rif,
            COUNT(DISTINCT codart_mat)                      AS n_codici_matricola,
            STRING_AGG(
                DISTINCT codart_mat || ' [' || tipo_nodo || ' L' || livello || ']',
                ', ' ORDER BY codart_mat || ' [' || tipo_nodo || ' L' || livello || ']'
            )                                               AS lista_codici_matricola
        FROM codici_matricola
        GROUP BY codart_rif
    )

    -- Step 5: risultato finale — una riga per articolo padre
    SELECT
        lp.codart_rif                                           AS "CODART",
        lp.descart                                              AS "DESCART",
        lp.tipo_prod                                            AS "TIPO_PROD",
        lp.fantasma                                             AS "FANTASMA",
        lp.flag6_padre                                          AS "MAT_PADRE",
        lp.n_loghi                                              AS "N_LOGHI",
        lp.loghi                                                AS "LOGHI",
        COALESCE(am.n_codici_matricola, 0)                      AS "N_COD_MATRICOLA",
        COALESCE(am.lista_codici_matricola, '—')                AS "LISTA_COD_MATRICOLA",
        lp.n_loghi * COALESCE(am.n_codici_matricola, 0)         AS "N_COPPIE_TOTALI"
    FROM loghi_per_padre lp
    LEFT JOIN agg_matricola am ON am.codart_rif = lp.codart_rif
    WHERE COALESCE(am.n_codici_matricola, 0) > 0
    {having}
    ORDER BY "N_COPPIE_TOTALI" DESC, "CODART"
    """

    params = (date_from,) + tuple(a2f_params)
    return _run(sql, params)


@st.cache_data(ttl=300)
def load_detail_diba(suffix: str, date_from: str, codart_sel: str) -> pd.DataFrame:
    """
    Dettaglio drill-down: una riga per ogni coppia
    (codice con matricola × logo distinto del padre).
    Mostra padre e figli DIBA separatamente con livello.
    """
    sql = f"""
    WITH
    -- Loghi distinti del padre: deduplicati su (codet, logo)
    loghi AS (
        SELECT DISTINCT
            COALESCE(c."CODET",  'IPH')     AS codet,
            COALESCE(c."DESCLI", 'IPH')     AS logo
        FROM ref."PUNTFOR_{suffix}" b
        LEFT JOIN ref."CLIM_{suffix}" c ON c."CODCLI" = b."CODCLI"
        WHERE NULLIF(b."DATAV", '') IS NOT NULL
          AND b."DATAV"::date > %s
          AND b."CODART" = %s
        GROUP BY
            COALESCE(c."CODET",  'IPH'),
            COALESCE(c."DESCLI", 'IPH')
    ),
    -- Codici con matricola: padre + figli DIBA, deduplicati su codart_mat
    codici_mat AS (
        SELECT DISTINCT ON (codart_mat)
            codart_mat, desc_mat, tipo_nodo, livello
        FROM (
            -- Padre se ha FLAG6='S'
            SELECT
                a."CODART"      AS codart_mat,
                a."DESCRIZIONE" AS desc_mat,
                'padre'         AS tipo_nodo,
                0               AS livello
            FROM ref."A2F_{suffix}" a
            WHERE a."CODART" = %s
              AND a."FLAG6" = 'S'

            UNION ALL

            -- Figli DIBA con MATRICOLA='S' — descrizione da A2F se disponibile
            SELECT
                d."CODART_F"                              AS codart_mat,
                COALESCE(a2."DESCRIZIONE", d."CODART_F")  AS desc_mat,
                'figlio'                                  AS tipo_nodo,
                d."LIVELLO"::int                          AS livello
            FROM ref."DIBA_{suffix}" d
            LEFT JOIN ref."A2F_{suffix}" a2 ON a2."CODART" = d."CODART_F"
            WHERE d."CODART_RIF" = %s
              AND d."MATRICOLA" = 'S'
        ) sub
        ORDER BY codart_mat, livello
    )
    -- Prodotto cartesiano: ogni codice con matricola × ogni logo distinto
    SELECT
        cm.codart_mat           AS "Codice articolo",
        cm.desc_mat             AS "Descrizione",
        cm.livello              AS "Livello DIBA",
        l.codet                 AS "Etichetta",
        l.logo                  AS "Logo"
    FROM codici_mat cm
    CROSS JOIN loghi l
    ORDER BY cm.livello, cm.codart_mat, l.logo
    """
    params = (date_from, codart_sel, codart_sel, codart_sel)
    return _run(sql, params)


# =============================================================================
# Layout pagina
# =============================================================================
st.title("🔩 Targhette Matricola + DIBA")
st.caption("Espansione loghi tramite gerarchia DIBA ricorsiva · coppie articolo-logo allargate ai componenti con matricola")
st.divider()

# Filtri
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

# Carica valori distinti filtri da A2F
@st.cache_data(ttl=300)
def load_filter_values(suffix: str) -> dict:
    result = {}
    for key, col in [("tipo_prod", '"FLAG-TIPREC"'), ("flag6", '"FLAG6"'),
                     ("fantasma", '"FANTASMA"'), ("codart", '"CODART"')]:
        try:
            df = _run(f'SELECT DISTINCT {col} AS v FROM ref."A2F_{suffix}" '
                      f'WHERE {col} IS NOT NULL ORDER BY 1')
            result[key] = df["v"].tolist() if not df.empty else []
        except Exception:
            result[key] = []
    return result

filter_vals = load_filter_values(plant_suffix)

col_tipo, col_flag6, col_fantasma = st.columns(3)
with col_tipo:
    sel_tipo_prod = st.multiselect("Tipo Produzione", options=filter_vals["tipo_prod"],
                                   default=[], placeholder="Tutti")
with col_flag6:
    sel_flag6 = st.multiselect("Matricola padre (FLAG6)", options=filter_vals["flag6"],
                                default=[], placeholder="Tutti")
with col_fantasma:
    sel_fantasma = st.multiselect("Fantasma", options=filter_vals["fantasma"],
                                   default=[], placeholder="Tutti")

sel_codart = st.multiselect("Codice Materiale (CODART)", options=filter_vals["codart"],
                             default=[], placeholder="Tutti — seleziona uno o più codici")

st.divider()

modalita = st.radio(
    "Modalità",
    options=["Solo A2F", "A2F + PUNTFOR + CLIM + DIBA"],
    index=1,
    horizontal=True,
    help=(
        "**Solo A2F**: tutti gli articoli dell'archivio senza join. "
        "**+ DIBA**: articoli venduti con loghi, espansi con i componenti matricola dalla distinta base."
    ),
)
solo_multipli = st.toggle("Mostra solo articoli con Tot. Etichette > 1", value=False)

# Caricamento dati
t_tipo_prod = tuple(sel_tipo_prod)
t_flag6     = tuple(sel_flag6)
t_fantasma  = tuple(sel_fantasma)
t_codart    = tuple(sel_codart)

try:
    with st.spinner("Caricamento dati..."):
        if modalita == "Solo A2F":
            a2f_parts, a2f_params_list = [], []
            if t_tipo_prod:
                ph = ", ".join(["%s"] * len(t_tipo_prod))
                a2f_parts.append(f'"FLAG-TIPREC" IN ({ph})')
                a2f_params_list.extend(t_tipo_prod)
            if t_flag6:
                ph = ", ".join(["%s"] * len(t_flag6))
                a2f_parts.append(f'"FLAG6" IN ({ph})')
                a2f_params_list.extend(t_flag6)
            if t_fantasma:
                ph = ", ".join(["%s"] * len(t_fantasma))
                a2f_parts.append(f'"FANTASMA" IN ({ph})')
                a2f_params_list.extend(t_fantasma)
            if t_codart:
                ph = ", ".join(["%s"] * len(t_codart))
                a2f_parts.append(f'"CODART" IN ({ph})')
                a2f_params_list.extend(t_codart)
            where = ("WHERE " + " AND ".join(a2f_parts)) if a2f_parts else ""
            sql_a2f = f"""
                SELECT "CODART", "DESCRIZIONE" AS "DESCART",
                       "FLAG-TIPREC" AS "TIPO_PROD", "FANTASMA",
                       "FLAG6" AS "MAT_PADRE",
                       0 AS "N_LOGHI", '' AS "LOGHI",
                       0 AS "N_COD_MATRICOLA", 0 AS "N_COPPIE_TOTALI",
                       '' AS "LISTA_COD_MATRICOLA"
                FROM ref."A2F_{plant_suffix}" {where} ORDER BY "CODART"
            """
            df = _run(sql_a2f, tuple(a2f_params_list) if a2f_params_list else None)
        else:
            df = load_diba(plant_suffix, date_from, t_tipo_prod, t_flag6,
                           t_fantasma, t_codart, solo_multipli=solo_multipli)
except Exception as e:
    st.error(f"Errore: {e}")
    st.stop()

# KPI
st.divider()
k1, k2, k3, k4 = st.columns(4)
tot_loghi    = int(df["N_LOGHI"].sum())        if not df.empty else 0
tot_coppie   = int(df["N_COPPIE_TOTALI"].sum()) if not df.empty else 0
delta_coppie = tot_coppie - tot_loghi
k1.metric("Articoli con cod. matricola",  f"{len(df):,}".replace(",", "."),
          help="Articoli padre con almeno un codice MATRICOLA='S' (incluso se stesso)")
k2.metric("Coppie articolo-logo (base)",  f"{tot_loghi:,}".replace(",", "."),
          help="Somma N_LOGHI — logica senza DIBA")
k3.metric("Coppie totali con DIBA",       f"{tot_coppie:,}".replace(",", "."),
          help="Somma N_LOGHI × N_COD_MATRICOLA")
k4.metric("Delta coppie aggiuntive",      f"+{delta_coppie:,}".replace(",", "."),
          help="Coppie aggiuntive grazie all'espansione DIBA")

st.divider()

# Tabella principale
col_title, col_export = st.columns([6, 1])
with col_title:
    titolo = f"📋 Archivio articoli — Plant {plant_label}" if modalita == "Solo A2F" \
             else f"📋 Articoli con componenti matricola — Plant {plant_label}"
    st.subheader(titolo)

if df.empty:
    st.info("✅ Nessun articolo trovato con i filtri selezionati.")
    st.stop()

st.caption(f"**{len(df)} articoli**")

# Filtro descrizione
filtro_desc = st.text_input("🔍 Filtra per descrizione", placeholder="Es. POMPA, KIT...",
                             key="filtro_desc_diba")

df_display = df.copy()
if filtro_desc:
    df_display = df_display[
        df_display["DESCART"].str.contains(filtro_desc, case=False, na=False)
    ]
    st.caption(f"Righe filtrate: **{len(df_display)}** su {len(df)}")
df_display = df_display.reset_index(drop=True)
df_display.insert(0, "N", range(1, len(df_display) + 1))

with col_export:
    st.download_button(
        label="⬇️ Esporta",
        data=to_csv_bytes(df_display.drop(columns=["N"])),
        file_name=f"targhette_diba_{plant_suffix}_{date_from}.csv",
        mime="text/csv",
    )

col_config_g1 = {
    "N":               st.column_config.NumberColumn("#",               format="%d", width=50),
    "CODART":          st.column_config.TextColumn("Cod. Articolo"),
    "DESCART":         st.column_config.TextColumn("Descrizione",       width="large"),
    "TIPO_PROD":       st.column_config.TextColumn("Make/Buy"),
    "FANTASMA":        st.column_config.TextColumn("Fantasma"),
    "MAT_PADRE":       st.column_config.TextColumn("Matricola"),
    "N_LOGHI":         st.column_config.NumberColumn("# Etichette/Loghi", format="%d"),
    "N_COD_MATRICOLA": st.column_config.NumberColumn("# Art. Matricola",  format="%d",
                           help="Nr articoli DIBA con MATRICOLA='S' (padre + figli a tutti i livelli)"),
    "N_COPPIE_TOTALI": st.column_config.NumberColumn("Tot. Etichette",    format="%d",
                           help="# Etichette/Loghi × # Art. Matricola"),
    "LOGHI":                None,
    "LISTA_COD_MATRICOLA":  None,
}

event = st.dataframe(
    df_display,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config=col_config_g1,
    column_order=["N", "CODART", "DESCART", "TIPO_PROD", "FANTASMA", "MAT_PADRE",
                  "N_LOGHI", "N_COD_MATRICOLA", "N_COPPIE_TOTALI"],
)

# Drill-down
selected_rows = event.selection.rows
if selected_rows:
    selected_codart = df_display.iloc[selected_rows[0]]["CODART"]
    n_coppie        = df_display.iloc[selected_rows[0]]["N_COPPIE_TOTALI"]
    st.divider()
    col_dt, col_de = st.columns([6, 1])
    with col_dt:
        st.subheader(f"🔍 Coppie codice-matricola × logo — `{selected_codart}`")
    try:
        with st.spinner("Caricamento dettaglio..."):
            df_detail = load_detail_diba(plant_suffix, date_from, selected_codart)
    except Exception as e:
        st.error(f"Errore: {e}")
        st.stop()

    if df_detail.empty:
        st.warning("Nessun dettaglio trovato.")
    else:
        st.caption(f"**{len(df_detail)} coppie** totali (atteso: {n_coppie})")
        with col_de:
            st.download_button(
                label="⬇️ Esporta",
                data=to_csv_bytes(df_detail),
                file_name=f"diba_detail_{selected_codart}_{plant_suffix}.csv",
                mime="text/csv",
            )
        df_det_display = df_detail.copy().reset_index(drop=True)
        df_det_display.insert(0, "N", range(1, len(df_det_display) + 1))
        st.dataframe(
            df_det_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "N":               st.column_config.NumberColumn("#",           format="%d", width=50),
                "Codice articolo": st.column_config.TextColumn("Cod. Articolo"),
                "Descrizione":     st.column_config.TextColumn("Descrizione",   width="large"),
                "Livello DIBA":    st.column_config.NumberColumn("Livello DIBA", format="%d"),
                "Etichetta":       st.column_config.TextColumn("Etichetta"),
                "Logo":            st.column_config.TextColumn("Logo"),
            },
        )
else:
    st.info("👆 Clicca su una riga per vedere il dettaglio delle coppie codice-logo.")
