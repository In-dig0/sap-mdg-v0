"""
MDG — Migration Data Governance
Edit Tables — Visualizzazione e modifica delle tabelle schema stg
"""

import re
import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
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
import os

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

STG_SCHEMA = "stg"
AUDIT_COLS = {"_source", "_loaded_at", "_xlsx_source", "_zip_source"}


# ---------------------------------------------------------------------------
# Helpers DB
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_stg_tables() -> list[str]:
    """Lista tutte le tabelle dello schema stg, escluse le tabelle di sistema."""
    SYSTEM_TABLES = {
        "check_results", "check_catalog", "pipeline_runs", "check_states"
    }
    try:
        conn = get_connection()
        cur = conn.cursor()
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
        cur = conn.cursor()
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


def load_table(table: str, status_filter: str, search: str,
               sort_by: str = "", sort_asc: bool = True) -> pd.DataFrame:
    try:
        fqt = f'{STG_SCHEMA}.{q(table)}'
        conn = get_connection()
        query = f'SELECT * FROM {fqt}'
        conditions = []
        params = []

        if status_filter != "Tutti":
            conditions.append('"_status" = %s')
            params.append(status_filter)
        if search:
            conditions.append(f"""
                EXISTS (
                    SELECT 1 FROM (
                        SELECT UNNEST(ARRAY[{", ".join(f"CAST({q(c)} AS TEXT)" for c in get_table_columns(table))}])
                    ) vals(v)
                    WHERE v ILIKE %s
                )
            """)
            params.append(f"%{search}%")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Ordinamento nella query SQL (corretto per tutti i tipi di dato)
        if sort_by:
            direction = "ASC" if sort_asc else "DESC"
            query += f' ORDER BY {q(sort_by)} {direction} NULLS LAST'
        else:
            query += " ORDER BY 1"
        query += " LIMIT 2000"

        df = pd.read_sql(query, conn, params=params if params else None)
        conn.close()

        # Formatta _loaded_at come stringa leggibile (dopo l'ordinamento SQL)
        if "_loaded_at" in df.columns:
            df["_loaded_at"] = pd.to_datetime(df["_loaded_at"], utc=True, errors="coerce") \
                                 .dt.strftime("%d/%m/%Y %H:%M:%S")

        # None → stringa vuota per evitare "None" in griglia
        df = df.fillna("").astype(str).replace("None", "").replace("nan", "")

        return df
    except Exception as e:
        st.error(f"Errore caricamento tabella: {e}")
        return pd.DataFrame()


def save_changes(table: str, original_df: pd.DataFrame,
                 editor_state: dict, key_cols: list[str]) -> tuple[int, int, int]:
    """
    Applica le modifiche usando lo stato interno di st.data_editor:
    - edited_rows:  {row_idx: {col: new_val, ...}}
    - added_rows:   [{col: val, ...}]
    - deleted_rows: [row_idx, ...]
    """
    fqt = f'{STG_SCHEMA}.{q(table)}'
    conn = get_connection()
    cur  = conn.cursor()
    n_upd = n_del = n_ins = 0

    edited_rows  = editor_state.get("edited_rows",  {})
    added_rows   = editor_state.get("added_rows",   [])
    deleted_rows = editor_state.get("deleted_rows", [])

    try:
        # ── DELETE ──────────────────────────────────────────────────────────
        for idx in deleted_rows:
            if idx < len(original_df):
                row = original_df.iloc[idx]
                if key_cols:
                    where = " AND ".join(f'{q(k)} = %s' for k in key_cols)
                    vals  = [row[k] for k in key_cols]
                    cur.execute(f'DELETE FROM {fqt} WHERE {where}', vals)
                    n_del += cur.rowcount

        # ── UPDATE ──────────────────────────────────────────────────────────
        for idx_str, changes in edited_rows.items():
            idx = int(idx_str)
            if idx >= len(original_df):
                continue
            orig_row = original_df.iloc[idx]
            if not changes or not key_cols:
                continue

            set_clause = ", ".join(f'{q(c)} = %s' for c in changes)
            set_vals   = [v if v is not None else "" for v in changes.values()]
            set_vals.append(pd.Timestamp.now(tz="UTC"))
            where_clause = " AND ".join(f'{q(k)} = %s' for k in key_cols)
            where_vals   = [orig_row[k] for k in key_cols]

            cur.execute(
                f'UPDATE {fqt} SET {set_clause}, "_loaded_at" = %s WHERE {where_clause}',
                set_vals + where_vals,
            )
            n_upd += cur.rowcount

        # ── INSERT ──────────────────────────────────────────────────────────
        for new_row in added_rows:
            if not new_row:
                continue
            insert_cols = [c for c in new_row if c and new_row[c] not in (None, "", "nan")]
            if not insert_cols:
                continue
            insert_vals = [new_row[c] for c in insert_cols]
            col_list    = ", ".join(q(c) for c in insert_cols) + ', "_loaded_at"'
            placeholders = ", ".join(["%s"] * len(insert_cols)) + ", NOW()"
            cur.execute("SAVEPOINT ins_sp")
            try:
                cur.execute(
                    f'INSERT INTO {fqt} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING',
                    insert_vals,
                )
                n_ins += cur.rowcount
                cur.execute("RELEASE SAVEPOINT ins_sp")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT ins_sp")

        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Errore salvataggio: {e}")
    finally:
        cur.close()
        conn.close()

    return n_upd, n_del, n_ins


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
    st.warning("Nessuna tabella trovata nello schema stg.")
    st.stop()

# ── Filtri ──────────────────────────────────────────────────────────────────
col_t, col_s, col_q = st.columns([3, 1, 2])

with col_t:
    selected_table = st.selectbox(
        "Tabella",
        options=tables,
        index=0,
        key="edit_table_sel",
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
        "Cerca nel testo",
        placeholder="Filtra righe...",
        key="edit_search",
    )

st.divider()

# ── Caricamento dati ─────────────────────────────────────────────────────────
all_cols  = get_table_columns(selected_table)
key_cols  = get_key_columns(all_cols)

# ── Ordinamento ──────────────────────────────────────────────────────────────
sort_col1, sort_col2, _ = st.columns([2, 1, 3])
with sort_col1:
    sort_by = st.selectbox(
        "Ordina per",
        options=["—"] + all_cols,
        index=0,
        key="edit_sort_col",
    )
with sort_col2:
    sort_asc = st.radio(
        "Direzione",
        options=["↑ Asc", "↓ Desc"],
        index=0,
        horizontal=True,
        key="edit_sort_dir",
    )

with st.spinner(f"Caricamento {selected_table}..."):
    df = load_table(
        selected_table, status_filter, search_text,
        sort_by=sort_by if sort_by != "—" else "",
        sort_asc=(sort_asc == "↑ Asc"),
    )

if df.empty:
    st.info("Nessun record trovato con i filtri selezionati.")
    st.stop()

# Colonne non editabili: chiavi e audit
disabled_cols = set(key_cols) | AUDIT_COLS
editable_cols = [c for c in df.columns if c not in disabled_cols]
audit_present = [c for c in df.columns if c in AUDIT_COLS]
key_present   = [c for c in df.columns if c in set(key_cols)]

# Info metriche
m1, _ = st.columns([1, 3])
m1.metric("Righe caricate", len(df))

st.caption(
    f"🔒 Colonne in sola lettura: **{', '.join(key_present + audit_present)}**  "
    f"| ✏️ Editabili: **{len(editable_cols)}** colonne"
)
st.divider()
# Configura colonne: chiavi e audit in read-only
col_config = {}
for c in df.columns:
    if c in disabled_cols:
        col_config[c] = st.column_config.TextColumn(c, disabled=True)
    elif c == "_status":
        col_config[c] = st.column_config.SelectboxColumn(
            c,
            options=["NEW", "EXISTS", "DELETED"],
            disabled=False,
        )
    else:
        col_config[c] = st.column_config.TextColumn(c, disabled=False)

# Salva df originale in session state per il confronto
orig_key = f"orig_df_{selected_table}"
if orig_key not in st.session_state or st.session_state.get(f"orig_table") != selected_table:
    st.session_state[orig_key]    = df.copy()
    st.session_state["orig_table"] = selected_table

# Indice da 1
df.index = range(1, len(df) + 1)

edited = st.data_editor(
    df,
    column_config=col_config,
    use_container_width=True,
    num_rows="dynamic",
    hide_index=False,
    key=f"editor_{selected_table}",
)

st.divider()

# ── Messaggio salvataggio (sopravvive al rerun) ───────────────────────────────
if "save_msg" in st.session_state:
    msg, msg_type = st.session_state.pop("save_msg")
    if msg_type == "success":
        st.success(msg)
    else:
        st.info(msg)

# ── Pulsante salvataggio ─────────────────────────────────────────────────────
col_save, col_reset, col_info = st.columns([1, 1, 4])

with col_save:
    save_clicked = st.button(
        "💾 Salva modifiche",
        type="primary",
        use_container_width=True,
        key="btn_save",
    )

with col_reset:
    if st.button("↩️ Annulla", use_container_width=True, key="btn_reset"):
        st.session_state.pop(orig_key, None)
        st.rerun()

if save_clicked:
    editor_state   = st.session_state.get(f"editor_{selected_table}", {})
    original_df    = st.session_state[orig_key]

    if not key_cols:
        st.warning(
            "⚠️ Questa tabella non ha colonne chiave `(k)` — "
            "UPDATE e DELETE non sono supportati. Solo INSERT è possibile."
        )

    with st.spinner("Salvataggio in corso..."):
        n_upd, n_del, n_ins = save_changes(
            selected_table, original_df, editor_state, key_cols
        )

    parts = []
    if n_upd: parts.append(f"✅ {n_upd} righe aggiornate")
    if n_del: parts.append(f"🗑️ {n_del} righe eliminate")
    if n_ins: parts.append(f"➕ {n_ins} righe inserite")

    if parts:
        st.session_state["save_msg"] = (" | ".join(parts), "success")
        st.session_state.pop(orig_key, None)
        st.rerun()
    else:
        st.session_state["save_msg"] = ("Nessuna modifica rilevata.", "info")
        st.rerun()

# ── Download ─────────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
with st.expander("⬇️ Esporta dati correnti", expanded=False):
    import io
    export_cols = [c for c in df.columns if c not in {"_status", "_source", "_loaded_at"}]
    df_export   = df[export_cols].reset_index(drop=True)

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
