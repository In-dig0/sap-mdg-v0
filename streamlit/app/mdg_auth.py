"""
POSIZIONE: mdg-v0/streamlit/app/mdg_auth.py

Helper di autenticazione per tutte le pagine Streamlit MDG.
Gestisce login, logout, persistenza token JWT e RBAC (admin / user).

Uso in ogni pagina:
    from mdg_auth import require_login, require_role, render_sidebar_menu

    require_login()           # blocca se non autenticato
    require_role("it_role")     # blocca se ruolo insufficiente (admin_role > it_role > business_role)
    render_sidebar_menu()     # menu adattivo in base al ruolo
"""

import os
import requests
import streamlit as st

AUTH_API_URL = os.getenv("AUTH_API_URL", "http://mdg_auth:8001")

# ---------------------------------------------------------------------------
# Helpers interni
# ---------------------------------------------------------------------------

def _login(email: str, password: str) -> dict | None:
    """Chiama il login JWT e restituisce i dati utente, oppure None se fallisce."""
    try:
        r = requests.post(
            f"{AUTH_API_URL}/auth/jwt/login",
            data={"username": email, "password": password},
            timeout=5,
        )
        if r.status_code != 200:
            return None

        token = r.json().get("access_token")

        r2 = requests.get(
            f"{AUTH_API_URL}/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        if r2.status_code != 200:
            return None

        user = r2.json()
        user["token"] = token
        return user

    except requests.exceptions.ConnectionError:
        st.error("⚠️ Auth API non raggiungibile. Controlla che il container `mdg_auth` sia attivo.")
        return None


def _render_login_form():
    """Mostra la form di login e gestisce il submit."""
    st.markdown(
        '<h1 style="color:#38BDF8;">🔐MDG — Login Migration Data Governance</h1>',
        unsafe_allow_html=True,
    )
    st.caption(":yellow[Inserisci le credenziali personali per accedere alla webapp.]")
    st.divider()    

    with st.form("login_form", clear_on_submit=False):
        email    = st.text_input("Email", placeholder="admin@mdg.local")
        password = st.text_input("Password", type="password")
        submit   = st.form_submit_button("Accedi", use_container_width=True)

    if submit:
        if not email or not password:
            st.warning("Inserisci email e password.")
            return

        with st.spinner("Autenticazione in corso..."):
            user = _login(email, password)

        if user:
            st.session_state["mdg_user"]              = user
            st.session_state["mdg_token"]             = user["token"]
            st.session_state["mdg_role"]              = user["role"]
            st.session_state["must_change_password"]  = user.get("must_change_password", False)
            st.rerun()
        else:
            st.error("Credenziali non valide. Riprova.")


# ---------------------------------------------------------------------------
# API pubblica
# ---------------------------------------------------------------------------

def require_login():
    """
    Blocca la pagina corrente se l'utente non è autenticato.
    Da chiamare subito dopo st.set_page_config() in ogni pagina.
    """
    if "mdg_user" not in st.session_state:
        _render_login_form()
        st.stop()

    # Se l'utente deve cambiare la password, redirect automatico a 0_User_Profile
    if st.session_state.get("must_change_password", False):
        try:
            ctx = st.runtime.scriptrunner.get_script_run_ctx()
            script_path = ctx.main_script_path if ctx else ""
            on_profilo = "0_User_Profile" in script_path or "Profilo" in script_path
        except Exception:
            on_profilo = False

        if not on_profilo:
            st.switch_page("pages/0_User_Profile.py")


def require_role(role: str):
    """
    Blocca la pagina se l'utente non ha il ruolo richiesto.
    Gerarchia: admin > user

    Chiama automaticamente require_login() se non già autenticato.
    """
    require_login()
    hierarchy  = {"admin_role": 3, "it_role": 2, "business_role": 1}
    user_role  = st.session_state.get("mdg_role", "business_role")

    if hierarchy.get(user_role, 0) < hierarchy.get(role, 99):
        st.error(f"🚫 Accesso negato. Questa pagina richiede il ruolo **{role}**.")
        st.stop()


def logout():
    """Cancella la sessione e torna alla login."""
    for key in ["mdg_user", "mdg_token", "mdg_role"]:
        st.session_state.pop(key, None)
    st.rerun()


def render_user_badge():
    """
    Mostra nel sidebar le info dell'utente loggato e il pulsante logout.
    """
    user      = st.session_state.get("mdg_user", {})
    role      = user.get("role", "?")
    email     = user.get("email", "?")
    full_name = user.get("full_name") or email
    badge     = "🔴 Admin" if role == "admin_role" else ("🟡 IT" if role == "it_role" else "🟢 Business")

    st.sidebar.markdown(f"**{full_name}**  \n`{badge}`")
    st.sidebar.caption(email)
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        logout()


def render_sidebar_menu():
    """
    Renderizza il menu laterale adattato al ruolo dell'utente.
    admin_role    = super admin
    it_role       = IT user → vede tutte le pagine
    business_role = Business user → vede solo Info, Dashboard, Check Results
    """
    render_user_badge()
    st.sidebar.divider()

    role = st.session_state.get("mdg_role", "business_role")

    # Pagine comuni a tutti i ruoli
    st.sidebar.page_link("pages/9_Info.py",                label="ℹ️ Info")
    st.sidebar.page_link("Dashboard.py",                   label="📊 Dashboard")
    st.sidebar.page_link("pages/1_Check_Results.py",       label="✅ Check Results")
    st.sidebar.page_link("pages/5_View_Data.py",           label="🗄️ View Data")
    st.sidebar.page_link("pages/0_User_Profile.py",        label="👤 My Profile")

    # Pagine riservate agli IT user (admin)
    if role == "it_role":
        st.sidebar.divider()
        st.sidebar.caption("IT Role — Funzionalità avanzate")
        st.sidebar.page_link("pages/2_Check_Catalog.py",  label="📋 Check Catalog")
        st.sidebar.page_link("pages/3_Pipeline_Admin.py", label="⚙️ Pipeline Admin")      
        st.sidebar.page_link("pages/4_Admin_Users.py",    label="👥 Users")
    elif role == "admin_role":
        st.sidebar.divider()
        st.sidebar.caption("Admin Role — Funzionalità avanzate")
        st.sidebar.page_link("pages/2_Check_Catalog.py",  label="📋 Check Catalog")
        st.sidebar.page_link("pages/3_Pipeline_Admin.py", label="⚙️ Pipeline Admin")      
        st.sidebar.page_link("pages/4_Admin_Users.py",    label="👥 Users")
        st.sidebar.page_link("pages/8_Edit_Tables.py",    label="✏️ Edit Tables")
        st.sidebar.page_link("pages/7_Targhette_Diba.py", label="🔩 Targhette + DIBA")                                     
