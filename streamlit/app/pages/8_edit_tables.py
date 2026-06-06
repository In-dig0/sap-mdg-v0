"""
MDG — Migration Data Governance
POSIZIONE: mdg-v0/streamlit/app/pages/8_edit_tables.py

Edit Tables — Visualizzazione e modifica delle tabelle schema stg
"""

import re
import io
import os
import requests
from urllib.parse import quote
import pandas as pd
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
# Configurazione
# ---------------------------------------------------------------------------
STG_SCHEMA    = "stg"
AUDIT_COLS    = {"_source", "_loaded_at", "_xlsx_source", "_zip_source"}
SYSTEM_TABLES = {"check_catalog", "pipeline_runs", "check_states"}

API_BASE = "http://mdg_fastapi:8000"

def enc(name: str) -> str:
    """URL-encode nomi tabella/schema che contengono caratteri speciali (es. #)."""
    return quote(name, safe="")


# ---------------------------------------------------------------------------
# Client API
# ---------------------------------------------------------------------------

def get_stg_tables() -> list[str]:
    """Lista tabelle schema stg tramite FastAPI — GET /db/schema/stg/tables."""
    try:
        r = requests.get(f"{API_BASE}/db/schema/{enc(STG_SCHEMA)}/tables", timeout=5)
        r.raise_for_status()
        return r.json().get("tables", [])
    except Exception as e:
        st.error(f"Errore recupero tabelle: {e}")
        return []


def get_table_columns(table: str) -> list[str]:
    """Colonne di una tabella tramite FastAPI — GET /db/schema/stg/tables/{table}/columns."""
    try:
        r = requests.get(f"{API_BASE}/db/schema/{enc(STG_SCHEMA)}/tables/columns", params={"table": table}, timeout=5)
        r.raise_for_status()
        return r.json().get("columns", [])
    except Exception as e:
        st.error(f"Errore lettura colonne: {e}")
        return []


def get_key_columns(cols: list[str]) -> list[str]:
    return [c for c in cols if re.search(r"\(.*?k.*?\)", c)]


def load_table(table: str, status_filter: str, search: str) -> pd.DataFrame:
    """Righe di una tabella tramite FastAPI — GET /db/schema/stg/tables/{table}/rows."""
    try:
        params = {"status_filter": status_filter if status_filter != "Tutti" else None,
                  "search": search or None}
        params = {k: v for k, v in params.items() if v is not None}
        r = requests.get(
            f"{API_BASE}/db/schema/{enc(STG_SCHEMA)}/tables/rows",
            params={"table": table, **params},
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json().get("rows", [])
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        # Formatta _loaded_at
        if "_loaded_at" in df.columns:
            df["_loaded_at"] = pd.to_datetime(df["_loaded_at"], utc=True, errors="coerce") \
                                 .dt.strftime("%Y-%m-%d %H:%M:%S")
        df = df.fillna("").astype(str).replace("None", "").replace("nan", "")
        return df
    except Exception as e:
        st.error(f"Errore caricamento tabella: {e}")
        return pd.DataFrame()


def truncate_table(schema: str, table: str) -> bool:
    """Svuota una tabella tramite FastAPI — POST /db/tables/{schema}/{table}/truncate."""
    try:
        r = requests.post(f"{API_BASE}/db/tables/{enc(schema)}/truncate", params={"table": table}, timeout=10)
        r.raise_for_status()
        return True
    except requests.HTTPError as e:
        st.error(f"Errore TRUNCATE: {e.response.json().get('detail', str(e))}")
        return False
    except Exception as e:
        st.error(f"Errore TRUNCATE: {e}")
        return False


def save_row(table: str, original_row: dict, updated_row: dict,
             key_cols: list[str]) -> bool:
    """Aggiorna una riga tramite FastAPI — PATCH /db/schema/stg/tables/{table}/rows."""
    if not key_cols:
        st.warning("⚠️ Tabella senza colonne chiave — UPDATE non supportato.")
        return False
    try:
        r = requests.patch(
            f"{API_BASE}/db/schema/{enc(STG_SCHEMA)}/tables/rows",
            params={"table": table},
            json={
                "original_row": original_row,
                "updated_row":  updated_row,
                "key_cols":     key_cols,
                "audit_cols":   list(AUDIT_COLS),
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("updated", False)
    except requests.HTTPError as e:
        st.error(f"Errore UPDATE: {e.response.json().get('detail', str(e))}")
        return False
    except Exception as e:
        st.error(f"Errore UPDATE: {e}")
        return False


def get_user_schemas() -> list[str]:
    """Schemi utente tramite FastAPI — GET /db/schemas."""
    try:
        r = requests.get(f"{API_BASE}/db/schemas", timeout=5)
        r.raise_for_status()
        return r.json().get("schemas", [])
    except Exception as e:
        st.error(f"Errore lettura schemi: {e}")
        return []


def truncate_schema_tables(schema: str) -> tuple[int, list[str]]:
    """Svuota tutte le tabelle di uno schema tramite FastAPI — POST /db/schema/{schema}/truncate."""
    try:
        r = requests.post(f"{API_BASE}/db/schema/{enc(schema)}/truncate", timeout=30)
        r.raise_for_status()
        data = r.json()
        return len(data.get("truncated", [])), data.get("errors", [])
    except requests.HTTPError as e:
        detail = e.response.json().get("detail", str(e))
        return 0, [detail]
    except Exception as e:
        return 0, [str(e)]


def drop_table(schema: str, table: str) -> bool:
    """Elimina una tabella tramite FastAPI — DELETE /db/tables/{schema}/{table}."""
    try:
        r = requests.delete(f"{API_BASE}/db/tables/{enc(schema)}", params={"table": table}, timeout=10)
        r.raise_for_status()
        return True
    except requests.HTTPError as e:
        st.error(f"Errore DROP TABLE: {e.response.json().get('detail', str(e))}")
        return False
    except Exception as e:
        st.error(f"Errore DROP TABLE: {e}")
        return False


def delete_row(table: str, row: dict, key_cols: list[str]) -> bool:
    """Elimina una riga tramite FastAPI — DELETE /db/schema/stg/tables/{table}/rows."""
    if not key_cols:
        st.warning("⚠️ Tabella senza colonne chiave — DELETE non supportato.")
        return False
    try:
        r = requests.delete(
            f"{API_BASE}/db/schema/{enc(STG_SCHEMA)}/tables/rows",
            params={"table": table},
            json={"row": row, "key_cols": key_cols},
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("deleted", False)
    except requests.HTTPError as e:
        st.error(f"Errore DELETE: {e.response.json().get('detail', str(e))}")
        return False
    except Exception as e:
        st.error(f"Errore DELETE: {e}")
        return False


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

# FIX: quando filter_key cambia, il blocco sopra ha già azzerato _orig_df a None.
# Lo rigeneriamo qui con il df appena caricato. In questo modo AgGrid riceve
# sempre i dati aggiornati al filtro corrente e non rimane agganciato al
# dataset del filtro precedente.
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

# ── Messaggio di ritorno dopo rerun ─────────────────────────────────────────
if "save_msg" in st.session_state:
    msg, msg_type = st.session_state.pop("save_msg")
    if msg_type == "success":
        st.success(msg)
    else:
        st.info(msg)

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

# ── Zona pericolosa — operazioni irreversibili ──────────────────────────────
with st.expander("⚠️ Zona Admin — operazioni irreversibili", expanded=False):

    # ── TRUNCATE TABLE ───────────────────────────────────────────────────────
    st.warning(f"**TRUNCATE** svuota completamente la tabella. L'operazione è irreversibile.")
    user_schemas_trunc = get_user_schemas()
    col_trunc_schema, col_trunc_table, col_trunc_confirm = st.columns([2, 2, 3])
    with col_trunc_schema:
        selected_schema_trunc_tbl = st.selectbox(
            "Schema",
            options=user_schemas_trunc,
            index=user_schemas_trunc.index(STG_SCHEMA) if STG_SCHEMA in user_schemas_trunc else 0,
            key="trunc_tbl_schema_sel",
        )
    # Carica tabelle dello schema selezionato
    try:
        _rt = requests.get(f"{API_BASE}/db/tables", params={"schema": selected_schema_trunc_tbl}, timeout=5)
        _rt.raise_for_status()
        _trunc_tables = [t["name"] for t in _rt.json().get("tables", [])]
    except Exception:
        _trunc_tables = []
    with col_trunc_table:
        selected_table_trunc = st.selectbox(
            "Tabella da svuotare",
            options=_trunc_tables if _trunc_tables else ["—"],
            key="trunc_tbl_sel",
        )
    with col_trunc_confirm:
        trunc_confirm = st.text_input(
            "Digita il nome della tabella per confermare",
            placeholder=selected_table_trunc if _trunc_tables else "",
            key="trunc_confirm",
        )
    trunc_clicked = st.button(
        "🗑️ Svuota tabella",
        type="secondary",
        use_container_width=False,
        disabled=(not _trunc_tables or selected_table_trunc == "—"),
        key="btn_trunc_table",
    )
    if trunc_clicked:
        if trunc_confirm == selected_table_trunc:
            if truncate_table(selected_schema_trunc_tbl, selected_table_trunc):
                st.session_state["save_msg"] = (
                    f"🗑️ Tabella `{selected_schema_trunc_tbl}.{selected_table_trunc}` svuotata.", "success"
                )
                st.session_state["_orig_df"] = None
                st.rerun()
        else:
            st.error("Nome tabella non corretto — operazione annullata.")

    st.divider()

    # ── TRUNCATE SCHEMA ──────────────────────────────────────────────────────
    st.warning("**TRUNCATE SCHEMA** svuota tutte le tabelle di uno schema selezionato. Operazione irreversibile.")
    user_schemas = get_user_schemas()
    col_schema_sel, col_confirm_all = st.columns([2, 3])
    with col_schema_sel:
        selected_schema_trunc = st.selectbox(
            "Schema da svuotare",
            options=user_schemas,
            key="trunc_schema_sel",
        )
    with col_confirm_all:
        trunc_all_confirm = st.text_input(
            "Digita il nome dello schema per confermare",
            placeholder=selected_schema_trunc if user_schemas else "",
            key="trunc_all_confirm",
        )
    trunc_all_clicked = st.button(
        "🗑️ Svuota schema",
        type="secondary",
        use_container_width=False,
        disabled=not user_schemas,
        key="btn_trunc_schema",
    )
    if trunc_all_clicked:
        if trunc_all_confirm == selected_schema_trunc:
            with st.spinner(f"Truncate schema '{selected_schema_trunc}' in corso..."):
                n, errs = truncate_schema_tables(selected_schema_trunc)
            if errs:
                st.warning(f"Completato con {len(errs)} errori: {'; '.join(errs)}")
            else:
                st.session_state["save_msg"] = (f"🗑️ {n} tabelle svuotate nello schema `{selected_schema_trunc}`.", "success")
                st.session_state["_orig_df"] = None
                st.rerun()
        else:
            st.error("Nome schema non corretto — operazione annullata.")

    st.divider()

    # ── DROP TABLE ───────────────────────────────────────────────────────────
    st.warning("**DROP TABLE** elimina definitivamente una tabella (struttura + dati) da qualsiasi schema. Operazione irreversibile.")
    user_schemas_drop = get_user_schemas()
    col_drop_schema, col_drop_table, col_drop_confirm = st.columns([2, 2, 3])
    with col_drop_schema:
        selected_schema_drop = st.selectbox(
            "Schema",
            options=user_schemas_drop,
            key="drop_schema_sel",
        )
    # Carica le tabelle dello schema selezionato tramite FastAPI
    try:
        _r = requests.get(f"{API_BASE}/db/tables", params={"schema": selected_schema_drop}, timeout=5)
        _r.raise_for_status()
        _drop_tables = [t["name"] for t in _r.json().get("tables", [])]
    except Exception:
        _drop_tables = []
    with col_drop_table:
        selected_table_drop = st.selectbox(
            "Tabella da eliminare",
            options=_drop_tables if _drop_tables else ["—"],
            key="drop_table_sel",
        )
    with col_drop_confirm:
        drop_confirm = st.text_input(
            "Digita il nome della tabella per confermare",
            placeholder=selected_table_drop if _drop_tables else "",
            key="drop_confirm",
        )
    drop_clicked = st.button(
        "💣 DROP TABLE",
        type="secondary",
        use_container_width=False,
        disabled=(not _drop_tables or selected_table_drop == "—"),
        key="btn_drop_table",
    )
    if drop_clicked:
        if drop_confirm == selected_table_drop:
            if drop_table(selected_schema_drop, selected_table_drop):
                st.session_state["save_msg"] = (
                    f"💣 Tabella `{selected_schema_drop}.{selected_table_drop}` eliminata.", "success"
                )
                st.session_state["_orig_df"] = None
                st.rerun()
        else:
            st.error("Nome tabella non corretto — operazione annullata.")

# ── Salvataggio ──────────────────────────────────────────────────────────────
if save_clicked:
    updated_df  = grid_response["data"]
    # Rimuove colonne interne aggiunte da AG Grid e la colonna indice
    internal_cols = [c for c in updated_df.columns if c.startswith("[::") or c == "#"]
    if internal_cols:
        updated_df = updated_df.drop(columns=internal_cols)
    original_df = st.session_state["_orig_df"].drop(columns=["#"], errors="ignore")

    if not key_cols:
        st.warning("⚠️ Questa tabella non ha colonne chiave — salvataggio non supportato.")
    else:
        # Confronto sicuro per chiave, non per posizione
        # Costruisce un dizionario {chiave_tuple: riga_originale}
        def make_key(row):
            return tuple(str(row.get(k, "")) for k in key_cols)

        orig_by_key = {make_key(r): r for r in original_df.to_dict("records")}
        n_saved = 0

        for upd in updated_df.to_dict("records"):
            key = make_key(upd)
            orig = orig_by_key.get(key)
            if orig and orig != upd:
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
