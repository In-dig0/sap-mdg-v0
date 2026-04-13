"""
POSIZIONE: mdg-v0/streamlit/app/pages/0_Profilo.py

Pagina profilo utente — accessibile a tutti i ruoli.
Mostra i dati dell'utente loggato e permette il cambio password.
"""

import os
import requests
import streamlit as st
from mdg_auth import render_sidebar_menu

st.set_page_config(page_title="Il mio profilo | MDG", page_icon="👤", layout="centered")

# Controllo login manuale — NON blocca per must_change_password
# perché questa è esattamente la pagina dove l'utente deve agire
if "mdg_user" not in st.session_state:
    st.warning("Sessione scaduta. Effettua di nuovo il login.")
    st.page_link("Dashboard.py", label="👉 Vai al login")
    st.stop()

render_sidebar_menu()

AUTH_API_URL = os.getenv("AUTH_API_URL", "http://mdg_auth:8001")

ROLE_LABEL = {
    "admin_role":    ("🔴", "Admin"),
    "it_role":       ("🟡", "IT"),
    "business_role": ("🟢", "Business"),
}

def _headers():
    return {"Authorization": f"Bearer {st.session_state['mdg_token']}"}

def validate_password(pwd: str) -> list[str]:
    errors = []
    if len(pwd) < 8:
        errors.append("almeno 8 caratteri")
    if not any(c.isupper() for c in pwd):
        errors.append("almeno una lettera maiuscola")
    if not any(c.isdigit() for c in pwd):
        errors.append("almeno un numero")
    if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?/" for c in pwd):
        errors.append("almeno un carattere speciale (!@#$%...)")
    return errors

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("👤 Il mio profilo")

if st.session_state.get("must_change_password", False):
    st.warning("⚠️ **Primo accesso.** Imposta una nuova password personale per sbloccare l'accesso alla dashboard.")

st.divider()

user = st.session_state.get("mdg_user", {})
role = user.get("role", "")
emoji, role_name = ROLE_LABEL.get(role, ("⚪", role))

# --- Dati utente ---
st.markdown(f"""
<table style="width:100%; font-size:15px; border-collapse:collapse;">
  <tr>
    <td style="padding:8px 16px 8px 0; color:#9ca3af; width:120px;">Nome</td>
    <td style="padding:8px 0; font-weight:500;">{user.get("full_name") or "—"}</td>
    <td style="padding:8px 16px 8px 24px; color:#9ca3af; width:80px;">Email</td>
    <td style="padding:8px 0; font-weight:500;">{user.get("email", "—")}</td>
  </tr>
  <tr>
    <td style="padding:8px 16px 8px 0; color:#9ca3af;">Ruolo</td>
    <td style="padding:8px 0; font-weight:500;">{emoji} {role_name}</td>
    <td style="padding:8px 16px 8px 24px; color:#9ca3af;">Stato</td>
    <td style="padding:8px 0; font-weight:500;">{"✅ Attivo" if user.get("is_active", True) else "⛔ Disattivo"}</td>
  </tr>
</table>
""", unsafe_allow_html=True)

st.divider()

# --- Cambio password ---
st.subheader("🔑 Cambia password")

with st.form("change_password_form"):
    new_pwd     = st.text_input("Nuova password", type="password")
    confirm_pwd = st.text_input("Conferma password", type="password")
    submitted   = st.form_submit_button("💾 Aggiorna password", use_container_width=True)

# Messaggio di esito — mostrato FUORI dal form, persiste tra i rerun
if submitted:
    if not new_pwd or not confirm_pwd:
        st.warning("Compila entrambi i campi.")
    elif new_pwd != confirm_pwd:
        st.error("Le password non coincidono.")
    else:
        errors = validate_password(new_pwd)
        if errors:
            st.error("Password non valida: " + ", ".join(errors) + ".")
        else:
            user_id = user.get("id")
            r = requests.post(
                f"{AUTH_API_URL}/admin/users/{user_id}/password",
                json={"password": new_pwd},
                headers=_headers(),
                timeout=5,
            )
            if r.status_code == 200:
                st.session_state["must_change_password"] = False
                st.session_state["profilo_success_msg"]  = "✅ Password aggiornata con successo. Ora puoi navigare liberamente."
            else:
                st.error(f"Errore: {r.json().get('detail', r.text)}")

# Mostra il messaggio persistente dopo il form — NON seguito da rerun()
if st.session_state.get("profilo_success_msg"):
    st.success(st.session_state.pop("profilo_success_msg"))
