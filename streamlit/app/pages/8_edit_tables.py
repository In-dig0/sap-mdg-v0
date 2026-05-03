"""
MDG — Migration Data Governance
POSIZIONE: mdg-v0/streamlit/app/pages/8_edit_tables.py

Edit Tables — Visualizzazione e modifica delle tabelle schema stg
"""

import re
import io
import os
import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode

from mdg_auth import require_role, render_sidebar_menu

st.set_page_config(
    page_title="Edit Tables",
    page_icon="✏️",
    layout="wide",
)
require_role("it_role")
render_sidebar_menu()

# ---------------------------------------------------------------------------
# Configurazione DB
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

STG_SCHEMA   = "stg"
AUDIT_COLS   = {"_source", "_loaded_at", "_xlsx_source", "_zip_source"}
SYSTEM_TABLES = {"check_results", "check_catalog", "pipeline_runs", "check_states"}


# ---------------------------------------------------------------------------
# Helpers DB
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_stg_tables() -> list[str]:
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (STG_SCHEMA,))
        tables = [r[0] for r in cur.fetchall() if r[0] not in SYSTEM_TABLES]
        cur.close()
        conn.close()
        return tables
    except Exception as e:
        st.error(f"Errore connessione DB: {e}")
        return []


def get_table_columns(table: str) -> list[str]:
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (STG_SCHEMA, table))
        cols = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return cols
    except Exception as e:
        st.error(f"Errore lettura colonne: {e}")
        return []


def get_key_columns(cols: list[str]) -> list[str]:
    return [c for c in cols if re.search(r"\(.*?k.*?\)", c)]


def load_table(table: str, status_filter: str, search: str) -> pd.DataFrame:
    try:
        fqt    = f'{STG_SCHEMA}.{q(table)}'
        conn   = get_connection()
        params = []
        conditions = []

        if status_filter != "Tutti":
            conditions.append('"_status" = %s')
            params.append(status_filter)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = f'SELECT * FROM {fqt} {where} ORDER BY 1'

        df = pd.read_sql(query, conn, params=params if params else None)
        conn.close()

        # Formatta _loaded_at
        if "_loaded_at" in df.columns:
            df["_loaded_at"] = pd.to_datetime(df["_loaded_at"], utc=True, errors="coerce") \
                                 .dt.strftime("%d/%m/%Y %H:%M:%S")

        # Ricerca testo
        if search:
            mask = df.apply(
                lambda col: col.astype(str).str.contains(search, case=False, na=False)
            ).any(axis=1)
            df = df[mask]

        df = df.fillna("").astype(str).replace("None", "").replace("nan", "")
        return df
    except Exception as e:
        st.error(f"Errore caricamento tabella: {e}")
        return pd.DataFrame()


def save_row(table: str, original_row: dict, updated_row: dict,
             key_cols: list[str]) -> bool:
    """Salva una singola riga modificata con UPDATE."""
    if not key_cols:
        st.warning("⚠️ Tabella senza colonne chiave — UPDATE non supportato.")
        return False

    fqt  = f'{STG_SCHEMA}.{q(table)}'
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # Determina le colonne cambiate
        changed = {
            col: updated_row[col]
            for col in updated_row
            if col not in (AUDIT_COLS | set(key_cols))
            and str(updated_row.get(col, "")) != str(original_row.get(col, ""))
        }
        if not changed:
            return False

        set_clause   = ", ".join(f'{q(c)} = %s' for c in changed)
        set_vals     = list(changed.values())
        set_vals.append(pd.Timestamp.now(tz="UTC"))
        where_clause = " AND ".join(f'{q(k)} = %s' for k in key_cols)
        where_vals   = [original_row[k] for k in key_cols]

        cur.execute(
            f'UPDATE {fqt} SET {set_clause}, "_loaded_at" = %s WHERE {where_clause}',
            set_vals + where_vals,
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        st.error(f"Errore UPDATE: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def delete_row(table: str, row: dict, key_cols: list[str]) -> bool:
    """Elimina una singola riga."""
    if not key_cols:
        st.warning("⚠️ Tabella senza colonne chiave — DELETE non supportato.")
        return False

    fqt  = f'{STG_SCHEMA}.{q(table)}'
    conn = get_connection()
    cur  = conn.cursor()
    try:
        where_clause = " AND ".join(f'{q(k)} = %s' for k in key_cols)
        where_vals   = [row[k] for k in key_cols]
        cur.execute(f'DELETE FROM {fqt} WHERE {where_clause}', where_vals)
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        st.error(f"Errore DELETE: {e}")
        return False
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.markdown(
    '<h1 style="color:#38BDF8;">✏️ MDG — Edit Tables</h1>',
    unsafe_allow_html=True,
)
st.caption(":yellow[Visualizzazione e modifica delle tabelle nello schema **stg**.]")
st.divider()

tables = get_stg_tables()
if not tables:
    st.info("⚠️ Nessuna tabella trovata nello schema stg. Avvia la pipeline Bruin per inizializzare il database.")
    st.stop()

# ── Filtri ──────────────────────────────────────────────────────────────────
col_t, col_s, col_q = st.columns([3, 1, 2])

with col_t:
    selected_table = st.selectbox(
        "Tabella", options=tables, index=0, key="edit_table_sel"
    )

with col_s:
    status_filter = st.selectbox(
        "Status",
        options=["Tutti", "NEW", "EXISTS", "DELETED"],
        index=0,
        key="edit_status_filter",
    )

with col_q:
    search_text = st.text_input(
        "Cerca nel testo", placeholder="Filtra righe...", key="edit_search"
    )

st.divider()

# ── Caricamento dati ─────────────────────────────────────────────────────────
all_cols = get_table_columns(selected_table)
key_cols = get_key_columns(all_cols)

# Resetta stato se cambia tabella o filtri
filter_key = f"{selected_table}_{status_filter}_{search_text}"
if st.session_state.get("_last_filter_key") != filter_key:
    st.session_state["_last_filter_key"] = filter_key
    st.session_state["_orig_df"]         = None
    st.session_state["_selected_row"]    = None

df = load_table(selected_table, status_filter, search_text)

if df.empty:
    st.info("Nessun record trovato con i filtri selezionati.")
    st.stop()

# Aggiunge colonna indice visibile (da 1)
df.insert(0, "#", range(1, len(df) + 1))

# Salva df originale
if st.session_state.get("_orig_df") is None:
    st.session_state["_orig_df"] = df.copy()

# ── Metriche ─────────────────────────────────────────────────────────────────
m1, _ = st.columns([1, 3])
m1.metric("Righe caricate", len(df))

audit_present = [c for c in df.columns if c in AUDIT_COLS]
key_present   = [c for c in df.columns if c in set(key_cols)]
st.caption(
    f"🔒 Sola lettura: **{', '.join(key_present + audit_present)}**  "
    f"| ✏️ Editabili: **{len([c for c in df.columns if c not in (AUDIT_COLS | set(key_cols))])}** colonne"
)
st.divider()

# ── Configurazione AG Grid ───────────────────────────────────────────────────
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(
    editable=True,
    resizable=True,
    sortable=True,
    filter=True,
    wrapText=False,
    autoHeight=False,
    minWidth=100,
)
gb.configure_selection(selection_mode="single", use_checkbox=False)

# Colonna indice — non editabile, pinned a sinistra, larghezza fissa
gb.configure_column(
    "#",
    editable=False,
    pinned="left",
    width=60,
    maxWidth=60,
    cellStyle={"backgroundColor": "#1e293b", "color": "#64748b", "textAlign": "center"},
)

# Colonne non editabili
for col in key_present + audit_present:
    gb.configure_column(
        col,
        editable=False,
        cellStyle={"backgroundColor": "#1e293b", "color": "#64748b"},
    )

# Colonna _status come dropdown
if "_status" in df.columns:
    gb.configure_column(
        "_status",
        editable=True,
        cellEditor="agSelectCellEditor",
        cellEditorParams={"values": ["NEW", "EXISTS", "DELETED"]},
    )

gb.configure_grid_options(
    rowHeight=32,
    headerHeight=36,
    suppressMovableColumns=True,
    stopEditingWhenCellsLoseFocus=True,
)

grid_options = gb.build()

# ── Rendering griglia ────────────────────────────────────────────────────────
grid_response = AgGrid(
    df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.VALUE_CHANGED | GridUpdateMode.SELECTION_CHANGED,
    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
    fit_columns_on_grid_load=False,
    theme="streamlit",
    height=450,
    allow_unsafe_jscode=True,
    key=f"aggrid_{selected_table}",
)

st.divider()

# ── Pulsanti azione ──────────────────────────────────────────────────────────
col_save, col_del, col_reset, _ = st.columns([1, 1, 1, 3])

with col_save:
    save_clicked = st.button("💾 Salva", type="primary", use_container_width=True)

with col_del:
    delete_clicked = st.button("🗑️ Elimina riga", use_container_width=True)

with col_reset:
    if st.button("↩️ Annulla", use_container_width=True):
        st.session_state["_orig_df"] = None
        st.rerun()

# ── Messaggio di ritorno dopo rerun ─────────────────────────────────────────
if "save_msg" in st.session_state:
    msg, msg_type = st.session_state.pop("save_msg")
    if msg_type == "success":
        st.success(msg)
    else:
        st.info(msg)

# ── Salvataggio ──────────────────────────────────────────────────────────────
if save_clicked:
    updated_df  = grid_response["data"]
    # Rimuove colonne interne aggiunte da AG Grid e la colonna indice
    internal_cols = [c for c in updated_df.columns if c.startswith("[::") or c == "#"]
    if internal_cols:
        updated_df = updated_df.drop(columns=internal_cols)
    original_df = st.session_state["_orig_df"].drop(columns=["#"], errors="ignore")
    n_saved = 0

    for i in range(min(len(updated_df), len(original_df))):
        orig = original_df.iloc[i].to_dict()
        upd  = updated_df.iloc[i].to_dict()
        if orig != upd:
            if save_row(selected_table, orig, upd, key_cols):
                n_saved += 1

    if n_saved:
        st.session_state["save_msg"] = (f"✅ {n_saved} righe salvate.", "success")
        st.session_state["_orig_df"] = None
    else:
        st.session_state["save_msg"] = ("Nessuna modifica rilevata.", "info")
    st.rerun()

# ── Eliminazione riga selezionata ────────────────────────────────────────────
if delete_clicked:
    selected = grid_response.get("selected_rows")
    if selected is not None and len(selected) > 0:
        row = selected[0] if isinstance(selected, list) else selected.iloc[0].to_dict()
        if delete_row(selected_table, row, key_cols):
            st.session_state["save_msg"] = ("🗑️ Riga eliminata.", "success")
            st.session_state["_orig_df"] = None
            st.rerun()
    else:
        st.warning("Seleziona una riga dalla griglia prima di eliminare.")

# ── Export ───────────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
with st.expander("⬇️ Esporta dati correnti", expanded=False):
    returned_df = grid_response["data"]
    export_cols = [c for c in df.columns if c not in {"#", "_status", "_source", "_loaded_at"} and c in returned_df.columns]
    df_export   = returned_df[export_cols].reset_index(drop=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Export")
    buf.seek(0)

    st.download_button(
        label=f"⬇️ Scarica {selected_table}.xlsx",
        data=buf,
        file_name=f"{selected_table}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
