"""
MDG — Migration Data Governance
Check Catalog — visualizza e gestisce i controlli attivi della pipeline
"""

import os
import json
from datetime import datetime
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from mdg_auth import require_login, render_sidebar_menu

st.set_page_config(
    page_title="Check Catalog",
    page_icon="📋",
    layout="wide",
)

require_login()
render_sidebar_menu()

STATES_PATH = "/app/config/check_states.json"

# ---------------------------------------------------------------------------
# Funzioni DB
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

def save_state(check_id: str, new_state: bool):
    """Aggiorna DB e JSON in modo sincrono."""
    conn = psycopg2.connect(**get_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE stg.check_catalog
                SET is_active = %s, updated_at = NOW()
                WHERE check_id = %s
            """, (new_state, check_id))
        conn.commit()
    finally:
        conn.close()
    try:
        if os.path.exists(STATES_PATH):
            with open(STATES_PATH, "r") as f:
                states = json.load(f)
        else:
            states = {}
        states[check_id] = new_state
        with open(STATES_PATH, "w") as f:
            json.dump(states, f, indent=2)
    except Exception as e:
        st.warning(f"DB aggiornato ma impossibile scrivere {STATES_PATH}: {e}")

def export_catalog() -> list:
    """Esporta tutti i record di stg.check_catalog come lista di dict."""
    conn = psycopg2.connect(**get_db_params(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT check_id, check_desc, category, check_type, severity,
                       target_table, target_field, ref_table, is_active
                FROM stg.check_catalog
                ORDER BY check_type, check_id
            """)
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def get_check_types() -> list[str]:
    """
    Legge i tipi di controllo distinti da stg.check_catalog.
    Restituisce la lista nell'ordine logico predefinito; i tipi non mappati
    vengono accodati in ordine alfabetico.
    """
    LOGICAL_ORDER = ["SAP_REF", "EXISTENCE", "CROSS_TABLE", "EXT_REF"]
    conn = psycopg2.connect(**get_db_params(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT check_type
                FROM stg.check_catalog
                WHERE check_type IS NOT NULL
                ORDER BY check_type
            """)
            db_types = [r["check_type"] for r in cur.fetchall()]
    finally:
        conn.close()
    # Prima i tipi con ordine logico noto, poi gli eventuali nuovi alfabeticamente
    known   = [t for t in LOGICAL_ORDER if t in db_types]
    unknown = sorted(t for t in db_types if t not in LOGICAL_ORDER)
    return known + unknown


def upsert_catalog(records: list) -> tuple[int, int]:
    conn = psycopg2.connect(**get_db_params())
    inserted = updated = 0
    try:
        with conn.cursor() as cur:
            for r in records:
                cur.execute("""
                    INSERT INTO stg.check_catalog
                        (check_id, check_desc, category, check_type, severity,
                         target_table, target_field, ref_table, is_active, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (check_id) DO UPDATE SET
                        check_desc   = EXCLUDED.check_desc,
                        category     = EXCLUDED.category,
                        check_type   = EXCLUDED.check_type,
                        severity     = EXCLUDED.severity,
                        target_table = EXCLUDED.target_table,
                        target_field = EXCLUDED.target_field,
                        ref_table    = EXCLUDED.ref_table,
                        is_active    = EXCLUDED.is_active,
                        updated_at   = NOW()
                    RETURNING (xmax = 0) AS is_insert
                """, (
                    r["check_id"], r["check_desc"], r.get("category", "BP"),
                    r["check_type"], r["severity"],
                    r["target_table"], r["target_field"], r.get("ref_table"),
                    r.get("is_active", True),
                ))
                row = cur.fetchone()
                if row and row[0]:
                    inserted += 1
                else:
                    updated += 1
        conn.commit()
    finally:
        conn.close()
    return inserted, updated

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown(
    '<h1 style="color:#38BDF8;">📋 MDG — Check Catalog </h1>',
    unsafe_allow_html=True,
)
st.caption(":yellow[Elenco dei controlli di qualità configurati nella pipeline MDG.]")
st.divider()

# ---------------------------------------------------------------------------
# Tab
# ---------------------------------------------------------------------------
tab_controllo, tab_backup = st.tabs(["📋 Controllo", "💾 Import / Export"])

# ===========================================================================
# TAB 1 — Controllo
# ===========================================================================
with tab_controllo:

    # Filtri
    col_type, col_sev, col_active, _ = st.columns([2, 2, 2, 2])
    with col_type:
        check_types_db = get_check_types()
        type_filter = st.selectbox(
            "Tipo controllo",
            options=["Tutti"] + check_types_db,
            index=0,
        )
    with col_sev:
        sev_filter = st.selectbox(
            "Severità",
            options=["Tutti", "Error", "Warning"],
            index=0,
        )
    with col_active:
        active_filter = st.selectbox(
            "Stato",
            options=["Tutti", "Attivi", "Inattivi"],
            index=0,
        )

    # Query
    where_parts = ["1=1"]
    params = []
    if type_filter != "Tutti":
        where_parts.append("check_type = %s")
        params.append(type_filter)
    if sev_filter != "Tutti":
        where_parts.append("severity = %s")
        params.append(sev_filter)
    if active_filter == "Attivi":
        where_parts.append("is_active = TRUE")
    elif active_filter == "Inattivi":
        where_parts.append("is_active = FALSE")

    where_clause = " AND ".join(where_parts)

    try:
        df = run_query(f"""
            SELECT check_id, check_desc, check_type, severity,
                   target_table, target_field, ref_table, is_active, updated_at
            FROM stg.check_catalog
            WHERE {where_clause}
            ORDER BY check_type, check_id
        """, tuple(params) if params else None)
    except Exception:
        st.info("⚠️ Nessun dato disponibile. Avvia la pipeline Bruin per inizializzare il database.")
        st.stop()

    if df.empty:
        st.info("Nessun check trovato con i filtri selezionati.")
        st.stop()

    # KPI
    total    = len(df)
    attivi   = int(df["is_active"].sum())
    inattivi = total - attivi
    c1, c2, c3 = st.columns(3)
    c1.metric("Check totali", total)
    c2.metric("✅ Attivi",    attivi)
    c3.metric("⏸️ Inattivi",  inattivi)
    st.divider()

    # Legenda
    TYPE_DESCRIPTIONS_MAP = {
        "SAP_REF":      "Coerenza con le tabelle di riferimento SAP (es. T005S per paese/regione)",
        "EXISTENCE":    "Esistenza del dato obbligatorio o duplicazione non ammessa",
        "CROSS_TABLE":  "Coerenza referenziale tra tabelle dello stesso archivio ZIP",
        "CROSS_SOURCE": "Coerenza referenziale tra tabelle di archivi ZIP o sorgenti diversi",
        "EXT_REF":      "Verifica tramite servizi esterni (es. VIES EU, HMRC UK)",
    }
    with st.expander("ℹ️ Legenda tipologie controllo", expanded=False):
        header = "| Tipo | Descrizione |\n|------|-------------|"
        rows   = "\n".join(
            f"| **{ct}** | {TYPE_DESCRIPTIONS_MAP.get(ct, 'Tipo di controllo personalizzato')} |"
            for ct in check_types_db
        )
        st.markdown(f"{header}\n{rows}")

    # Card per ogni check con toggle
    # Palette colori e label per tipo — estendibile senza modificare il codice:
    # se arriva un tipo non mappato, viene usato il colore/label di fallback.
    TYPE_COLORS_MAP = {
        "SAP_REF":      "#4A90D9",
        "EXISTENCE":    "#E8A838",
        "CROSS_TABLE":  "#7B68EE",
        "CROSS_SOURCE": "#E05252",
        "EXT_REF":      "#2ECC71",
    }
    TYPE_ICONS_MAP = {
        "SAP_REF":      "🔵",
        "EXISTENCE":    "🟡",
        "CROSS_TABLE":  "🟣",
        "CROSS_SOURCE": "🔴",
        "EXT_REF":      "🟢",
    }
    FALLBACK_COLOR = "#AAAAAA"
    FALLBACK_ICON  = "⚪"

    for check_type in check_types_db:
        color = TYPE_COLORS_MAP.get(check_type, FALLBACK_COLOR)
        icon  = TYPE_ICONS_MAP.get(check_type, FALLBACK_ICON)
        label = f"{icon} {check_type}"
        df_type = df[df["check_type"] == check_type].sort_values("check_id")
        if df_type.empty:
            continue

        st.markdown(
            f'<h3 style="color:{color}; margin-top:16px;">'
            f'{label}</h3>',
            unsafe_allow_html=True,
        )

        for _, row in df_type.iterrows():
            check_id  = row["check_id"]
            is_active = bool(row["is_active"])
            severity  = row["severity"]
            sev_color = "#e24b4a" if severity == "Error" else "#E8A838"
            updated_at = row["updated_at"]

            with st.container(border=True):
                col_info, col_meta, col_toggle = st.columns([4, 3, 1])
                with col_info:
                    st.markdown(
                        f'<span style="font-size:18px; font-weight:700; color:#85B7EB;">'
                        f'{check_id}</span>'
                        f'&nbsp;&nbsp;<span style="font-size:12px; color:{sev_color}; '
                        f'font-weight:600;">{severity}</span>',
                        unsafe_allow_html=True,
                    )
                    st.markdown(
                        f'<span style="color:#EF9F27; font-size:14px;">'
                        f'{row["check_desc"]}</span>',
                        unsafe_allow_html=True,
                    )
                with col_meta:
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.caption(f"Tabella: `{row['target_table']}`")
                    st.caption(f"Campo: `{row['target_field']}`")
                    if row["ref_table"]:
                        st.caption(f"Ref: `{row['ref_table']}`")
                with col_toggle:
                    st.markdown("<br><br>", unsafe_allow_html=True)
                    new_state = st.toggle(
                        "Attivo",
                        value=is_active,
                        key=f"toggle_{check_id}",
                    )
                    if new_state != is_active:
                        save_state(check_id, new_state)
                        action = "attivato ✅" if new_state else "disattivato ⏸️"
                        st.toast(f"{check_id} {action}", icon="💾")
                        st.rerun()

            updated_str = updated_at.strftime('%d/%m/%Y %H:%M') if hasattr(updated_at, 'strftime') else str(updated_at)[:16]
            st.caption(f"Aggiornato il: {updated_str}")

# ===========================================================================
# TAB 2 — Import / Export
# ===========================================================================
with tab_backup:
    st.subheader("💾 Backup e ripristino del catalogo")
    st.caption("Esporta la configurazione dei check su file JSON oppure ripristinala da un backup.")
    st.divider()

    # -----------------------------------------------------------------------
    # EXPORT
    # -----------------------------------------------------------------------
    st.markdown("#### ⬇️ Export")
    st.write("Scarica tutti i check in formato JSON — utile per backup o per inizializzare un nuovo ambiente.")

    if st.button("Genera file di export", use_container_width=False):
        try:
            records = export_catalog()
            json_bytes = json.dumps(records, indent=2, default=str).encode("utf-8")

            # Mostra anteprima dettagliata
            st.success(f"✅ {len(records)} record pronti per il download.")
            df_export = pd.DataFrame(records)[
                ["check_id", "category", "check_type", "severity", "check_desc",
                 "target_table", "target_field", "ref_table", "is_active"]
            ]
            df_export.columns = [
                "Check ID", "Categoria", "Tipo", "Severità", "Descrizione",
                "Tabella", "Campo", "Rif.", "Attivo"
            ]
            st.dataframe(df_export, use_container_width=True, hide_index=True)

            st.download_button(
                label=f"📥 Scarica check_catalog.json ({len(records)} record)",
                data=json_bytes,
                file_name="check_catalog.json",
                mime="application/json",
            )
        except Exception as e:
            st.error(f"Errore export: {e}")

    st.divider()

    # -----------------------------------------------------------------------
    # IMPORT
    # -----------------------------------------------------------------------
    st.markdown("#### ⬆️ Import")
    st.write("Carica un file JSON esportato in precedenza. I record esistenti vengono aggiornati, i nuovi inseriti.")

    uploaded = st.file_uploader(
        "Seleziona check_catalog.json",
        type=["json"],
        key="catalog_upload",
    )

    if uploaded:
        try:
            records = json.loads(uploaded.read().decode("utf-8"))

            # Recupera i check_id già presenti nel DB per distinguere insert da update
            try:
                df_existing = run_query("SELECT check_id FROM stg.check_catalog")
                existing_ids = set(df_existing["check_id"].tolist()) if not df_existing.empty else set()
            except Exception:
                existing_ids = set()

            new_records = [r for r in records if r["check_id"] not in existing_ids]
            upd_records = [r for r in records if r["check_id"] in existing_ids]

            st.info(f"File caricato: **{len(records)} record** — 🟢 {len(new_records)} nuovi, 🟡 {len(upd_records)} da aggiornare.")

            # Anteprima con evidenza nuovi vs aggiornati
            rows_preview = []
            for r in records:
                rows_preview.append({
                    "Esito atteso":  "🟢 Nuovo" if r["check_id"] not in existing_ids else "🟡 Aggiornamento",
                    "Check ID":      r["check_id"],
                    "Categoria":     r.get("category", ""),
                    "Tipo":          r.get("check_type", ""),
                    "Severità":      r.get("severity", ""),
                    "Descrizione":   r.get("check_desc", ""),
                    "Tabella":       r.get("target_table", ""),
                    "Campo":         r.get("target_field", ""),
                    "Attivo":        r.get("is_active", True),
                })

            df_preview = pd.DataFrame(rows_preview)
            st.dataframe(df_preview, use_container_width=True, hide_index=True)

            if st.button("✅ Conferma import nel database", use_container_width=False):
                ins, upd = upsert_catalog(records)
                st.success(f"✅ Import completato — **{ins}** inseriti, **{upd}** aggiornati.")
                st.cache_data.clear()

        except json.JSONDecodeError:
            st.error("File JSON non valido.")
        except Exception as e:
            st.error(f"Errore import: {e}")
