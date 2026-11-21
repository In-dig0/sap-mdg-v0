"""
MDG — Migration Data Governance
POSIZIONE: mdg-v0/streamlit/app/pages/11_environments.py

Ambienti SAP — registro degli ambienti (sistema + mandante, es. DS4-200): elenco, creazione per
copia da un ambiente esistente, modifica e disattivazione. Il registro è in FastAPI
(/environments, database comune); ogni ambiente ha il suo database e le sue cartelle.
"""

import pandas as pd
import requests
import streamlit as st
from mdg_auth import require_role, render_sidebar_menu
from mdg_env_ui import API_BASE, COLOR_MAP, DEFAULT_ENV, list_environments

st.set_page_config(page_title="Ambienti SAP", page_icon="🌐", layout="wide")
require_role("it_role")
render_sidebar_menu()

st.title("🌐 Ambienti SAP")
st.caption("Ogni ambiente (sistema SAP + mandante) ha il suo database, le sue cartelle SFTP "
           "(sottocartella con il nome dell'ambiente) e il suo stato dei controlli. "
           "L'ambiente su cui lavorano le pagine si sceglie nella barra laterale.")

COPY_MODES = {
    "structure": "Solo struttura — tabelle vuote (correzioni e elenchi da caricare da zero)",
    "staging":   "Struttura + staging — correzioni _STG, elenchi, catalogo controlli, cache VIES",
    "all":       "Copia completa — tutti i dati, esiti e storico (migrazione)",
}
COLORS = ["gray", "blue", "green", "orange", "red", "purple", "yellow"]


def api_error(e: Exception) -> str:
    if isinstance(e, requests.HTTPError) and e.response is not None:
        try:
            return e.response.json().get("detail", str(e))
        except ValueError:
            return e.response.text or str(e)
    return str(e)


envs = list_environments()

# ── Elenco ─────────────────────────────────────────────────────────────────
st.subheader("Ambienti registrati")
if envs:
    df = pd.DataFrame(envs)[["env_id", "description", "color", "active", "db_name", "created_at"]]
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.strftime("%d/%m/%Y %H:%M")
    st.dataframe(df.rename(columns={"env_id": "Ambiente", "description": "Descrizione", "color": "Colore",
                                    "active": "Attivo", "db_name": "Database", "created_at": "Creato il"}),
                 use_container_width=True, hide_index=True)
else:
    st.info("Nessun ambiente registrato: le pagine lavorano sull'ambiente predefinito (database mdg).")

# ── Nuovo ambiente ─────────────────────────────────────────────────────────
st.divider()
st.subheader("➕ Nuovo ambiente")
with st.form("new_env"):
    c1, c2, c3 = st.columns([1, 1, 2])
    system = c1.text_input("Sistema SAP", placeholder="DS4", help="Es. DS4 (sviluppo), QS4 (quality), PS4 (produzione)")
    client = c2.text_input("Mandante", placeholder="200")
    description = c3.text_input("Descrizione", placeholder="Sviluppo, mandante 200")
    c4, c5 = st.columns([1, 3])
    color = c4.selectbox("Colore", COLORS, help="Colore del badge dell'ambiente nella barra laterale")
    sources = [DEFAULT_ENV] + [e["env_id"] for e in envs if e.get("active")]
    copy_from = c5.selectbox("Copia da", sources,
                             format_func=lambda e: "Predefinito (mdg)" if e == DEFAULT_ENV else e,
                             help="Il nuovo database nasce come copia di questo ambiente: la pipeline "
                                  "richiede che tutte le tabelle esistano.")
    copy_data = st.radio("Cosa copiare", list(COPY_MODES), format_func=COPY_MODES.get, index=1)
    if st.form_submit_button("Crea ambiente", type="primary"):
        if not system.strip() or not client.strip():
            st.error("Indica sistema e mandante.")
        else:
            with st.spinner("Creazione del database e delle cartelle in corso..."):
                try:
                    r = requests.post(f"{API_BASE}/environments", timeout=1800, json={
                        "sap_system": system, "sap_client": client, "description": description or None,
                        "color": color, "copy_from": copy_from, "copy_data": copy_data})
                    r.raise_for_status()
                    list_environments.clear()
                    st.success(r.json().get("message", "Ambiente creato"))
                except Exception as e:
                    st.error(f"Creazione non riuscita: {api_error(e)}")

# ── Modifica ───────────────────────────────────────────────────────────────
if envs:
    st.divider()
    st.subheader("✏️ Modifica ambiente")
    sel = st.selectbox("Ambiente", [e["env_id"] for e in envs], key="edit_env")
    cur = next(e for e in envs if e["env_id"] == sel)
    with st.form("edit_env_form"):
        c1, c2, c3 = st.columns([2, 1, 1])
        new_desc = c1.text_input("Descrizione", value=cur.get("description") or "")
        cur_color = cur.get("color") or "gray"
        new_color = c2.selectbox("Colore", COLORS if cur_color in COLORS else COLORS + [cur_color],
                                 index=(COLORS + [cur_color]).index(cur_color))
        new_active = c3.checkbox("Attivo", value=bool(cur.get("active")),
                                 help="Un ambiente disattivato non compare nel selettore e non si può "
                                      "avviare la pipeline; database e cartelle restano.")
        st.caption("Cartelle: " + ", ".join(cur.get("folders", [])))
        if st.form_submit_button("Salva"):
            try:
                r = requests.patch(f"{API_BASE}/environments/{sel}", timeout=10, json={
                    "description": new_desc, "color": new_color, "active": new_active})
                r.raise_for_status()
                list_environments.clear()
                st.success(f"Ambiente {sel} aggiornato")
                st.rerun()
            except Exception as e:
                st.error(f"Modifica non riuscita: {api_error(e)}")

st.caption(f"Colori disponibili: {', '.join(COLOR_MAP)}. Gli ambienti non si cancellano: si disattivano.")
