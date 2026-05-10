"""
POSIZIONE: mdg-v0/streamlit/app/mdg_auth.py

Helper di autenticazione per tutte le pagine Streamlit MDG.
Gestisce login, logout, persistenza token JWT e RBAC (admin / user).

Uso in ogni pagina:
    from mdg_auth import require_login, require_role, render_sidebar_menu

    require_login()           # blocca se non autenticato
    require_role("it_role")   # blocca se ruolo insufficiente (admin_role > it_role > business_role)
    render_sidebar_menu()     # menu adattivo in base al ruolo
"""

import os
import time
import random
import logging
import threading
import requests
import psycopg2
import streamlit as st
from datetime import datetime, timezone

AUTH_API_URL  = os.getenv("AUTH_API_URL",  "http://mdg_auth:8001")
PIPELINE_API_URL = os.getenv("PIPELINE_API_URL", "http://mdg_fastapi:8000")

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

ROLE_HIERARCHY = {"admin_role": 3, "it_role": 2, "business_role": 1}

MAX_LOGIN_ATTEMPTS = 5       # Tentativi prima del lockout
LOCKOUT_SECONDS    = 300     # 5 minuti di lockout
REQUEST_TIMEOUT    = 10      # Timeout chiamate HTTP in secondi

# ---------------------------------------------------------------------------
# Math CAPTCHA
# ---------------------------------------------------------------------------

def _generate_captcha() -> tuple[str, int]:
    """Genera una domanda matematica semplice e restituisce (domanda, risposta)."""
    a = random.randint(1, 15)
    b = random.randint(1, 15)
    op = random.choice(["+", "-", "*"])
    if op == "+":
        answer = a + b
    elif op == "-":
        # Evita risultati negativi
        a, b = max(a, b), min(a, b)
        answer = a - b
    else:
        a = random.randint(1, 9)
        b = random.randint(1, 9)
        answer = a * b
    question = f"{a} {op} {b} = ?"
    return question, answer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("mdg_auth")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] mdg_auth — %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _persist_log(email: str, success: bool, reason: str, role: str):
    """Scrive il log direttamente su Postgres in un thread separato (non blocca l'UI)."""
    try:
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = int(os.environ.get("POSTGRES_PORT", 5432))
        db   = os.environ.get("POSTGRES_DB", "mdg")
        user = os.environ.get("POSTGRES_USER", "mdg_user")
        pwd  = os.environ.get("POSTGRES_PASSWORD", "")
        conn = psycopg2.connect(
            host=host, port=port, dbname=db, user=user, password=pwd,
            connect_timeout=3,
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO usr.access_log (email, success, reason, role, logged_at) "
                "VALUES (%s, %s, %s, %s, NOW())",
                (email, success, reason or None, role or None),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Log persistence failed: {e}")


def _log_access(email: str, success: bool, reason: str = "", role: str = ""):
    """Scrive il log su stdout e persiste su DB in background (non blocca l'UI)."""
    ts = datetime.now(timezone.utc).isoformat()
    if success:
        logger.info(f"LOGIN_OK   | user={email} | role={role} | ts={ts}")
    else:
        logger.warning(f"LOGIN_FAIL | user={email} | reason={reason} | ts={ts}")

    # Persiste in un thread separato per non bloccare Streamlit
    t = threading.Thread(target=_persist_log, args=(email, success, reason, role), daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# Helpers interni
# ---------------------------------------------------------------------------

def _check_lockout() -> bool:
    """
    Restituisce True se l'utente è attualmente in lockout.
    Mostra anche un messaggio con i secondi rimanenti.
    """
    locked_until = st.session_state.get("login_locked_until")
    if locked_until and time.time() < locked_until:
        remaining = int(locked_until - time.time())
        st.error(f"🔒 Troppi tentativi falliti. Riprova tra **{remaining}** secondi.")
        return True
    # Lockout scaduto: resetta il contatore
    if locked_until and time.time() >= locked_until:
        st.session_state["login_attempts"]    = 0
        st.session_state["login_locked_until"] = None
    return False


def _register_failed_attempt(email: str, reason: str):
    """Incrementa il contatore tentativi e applica lockout se necessario."""
    st.session_state["login_attempts"] = st.session_state.get("login_attempts", 0) + 1
    attempts = st.session_state["login_attempts"]
    _log_access(email, success=False, reason=reason)

    if attempts >= MAX_LOGIN_ATTEMPTS:
        st.session_state["login_locked_until"] = time.time() + LOCKOUT_SECONDS
        logger.warning(f"LOCKOUT | user={email} | attempts={attempts}")
        st.error(f"🔒 Account bloccato per {LOCKOUT_SECONDS // 60} minuti dopo {attempts} tentativi falliti.")
    else:
        remaining_attempts = MAX_LOGIN_ATTEMPTS - attempts
        st.error(f"Credenziali non valide. Tentativi rimanenti: **{remaining_attempts}**")


def _login(email: str, password: str) -> dict | None:
    """Chiama il login JWT e restituisce i dati utente, oppure None se fallisce."""
    try:
        r = requests.post(
            f"{AUTH_API_URL}/auth/jwt/login",
            data={"username": email, "password": password},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return None

        token = r.json().get("access_token")

        r2 = requests.get(
            f"{AUTH_API_URL}/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=REQUEST_TIMEOUT,
        )
        if r2.status_code != 200:
            return None

        user = r2.json()
        user["token"] = token
        return user

    except requests.exceptions.Timeout:
        st.error("⚠️ Auth API non risponde (timeout). Riprova tra qualche secondo.")
        return None
    except requests.exceptions.ConnectionError:
        st.error("⚠️ Auth API non raggiungibile. Controlla che il container `mdg_auth` sia attivo.")
        return None


def _verify_token(token: str) -> bool:
    """Verifica che il token JWT sia ancora valido chiamando /me."""
    try:
        r = requests.get(
            f"{AUTH_API_URL}/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=REQUEST_TIMEOUT,
        )
        return r.status_code == 200
    except Exception:
        return False


def _render_login_form():
    """Mostra la form di login e gestisce il submit."""
    st.markdown(
        '<h1 style="color:#38BDF8;">🔐MDG — Login Migration Data Governance</h1>',
        unsafe_allow_html=True,
    )
    st.caption(":yellow[Inserisci le credenziali personali per accedere alla webapp.]")
    st.divider()

    # Inizializza contatori in session_state
    if "login_attempts" not in st.session_state:
        st.session_state["login_attempts"] = 0
    if "login_locked_until" not in st.session_state:
        st.session_state["login_locked_until"] = None

    # FIX: pattern "next captcha"
    # Al submit errato prepariamo il NUOVO captcha in captcha_next_*, ma NON lo
    # attiviamo subito: lo promuoviamo a captcha_question/answer solo all'inizio
    # del render SUCCESSIVO (quello in cui l'utente deve già leggere la domanda
    # e compilare il form). Così domanda mostrata e risposta attesa sono sempre
    # sincronizzate, anche con clear_on_submit=True.
    if "captcha_next_q" in st.session_state:
        # Promuovi il captcha pre-generato al render precedente
        st.session_state["captcha_question"] = st.session_state.pop("captcha_next_q")
        st.session_state["captcha_answer"]   = st.session_state.pop("captcha_next_a")
    elif "captcha_question" not in st.session_state:
        # Prima apertura: genera il captcha iniziale
        q, a = _generate_captcha()
        st.session_state["captcha_question"] = q
        st.session_state["captcha_answer"]   = a

    # Blocca se in lockout
    if _check_lockout():
        st.stop()

    with st.form("login_form", clear_on_submit=True):
        email    = st.text_input("Email", placeholder="admin@mdg.local")
        password = st.text_input("Password", type="password")
        st.markdown(
            f'<p style="font-size:14px; margin-bottom:4px;">🔢 Verifica: '
            f'<strong>{st.session_state["captcha_question"]}</strong></p>',
            unsafe_allow_html=True,
        )
        captcha_input = st.text_input("Risposta", placeholder="es. 12")
        submit   = st.form_submit_button("Accedi", use_container_width=True)

    if submit:
        if not email or not password:
            st.warning("Inserisci email e password.")
            return

        # Valida captcha
        try:
            captcha_value = int(captcha_input.strip())
        except (ValueError, AttributeError):
            captcha_value = None

        if captcha_value != st.session_state.get("captcha_answer"):
            # Pre-genera il prossimo captcha: sarà promosso al render successivo
            q, a = _generate_captcha()
            st.session_state["captcha_next_q"] = q
            st.session_state["captcha_next_a"] = a
            st.error("❌ Risposta al CAPTCHA non corretta. Riprova.")
            return

        with st.spinner("Autenticazione in corso..."):
            user = _login(email, password)

        # Dopo ogni tentativo (ok o ko) pre-genera il prossimo captcha
        q, a = _generate_captcha()
        st.session_state["captcha_next_q"] = q
        st.session_state["captcha_next_a"] = a

        if user:
            # Login riuscito: resetta contatori e salva sessione
            st.session_state["login_attempts"]       = 0
            st.session_state["login_locked_until"]   = None
            st.session_state["mdg_user"]             = user
            st.session_state["mdg_token"]            = user["token"]
            st.session_state["mdg_role"]             = user["role"]
            st.session_state["must_change_password"] = user.get("must_change_password", False)
            # Imposta last_checked = now così il primo require_login non ri-verifica subito
            st.session_state["token_last_checked"]   = time.time()
            _log_access(email, success=True, role=user.get("role", "?"))
            st.rerun()
        else:
            _register_failed_attempt(email, reason="Credenziali non valide")


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

    # Verifica periodica validità token (ogni 5 minuti)
    last_check = st.session_state.get("token_last_checked", 0)
    if time.time() - last_check > 300:
        token = st.session_state.get("mdg_token", "")
        if not _verify_token(token):
            logger.warning(f"TOKEN_EXPIRED | user={st.session_state.get('mdg_user', {}).get('email', '?')}")
            for key in ["mdg_user", "mdg_token", "mdg_role", "must_change_password"]:
                st.session_state.pop(key, None)
            st.warning("⚠️ Sessione scaduta. Effettua nuovamente il login.")
            st.rerun()
        st.session_state["token_last_checked"] = time.time()

    # Se l'utente deve cambiare la password, redirect automatico a 0_User_Profile
    if st.session_state.get("must_change_password", False):
        try:
            ctx = st.runtime.scriptrunner.get_script_run_ctx()
            script_path = ctx.main_script_path if ctx else ""
            on_profilo = "0_User_Profile" in script_path or "Profilo" in script_path
        except Exception:
            on_profilo = False

        if not on_profilo:
            st.switch_page("pages/0_user_profile.py")


def require_role(role: str):
    """
    Blocca la pagina se l'utente non ha il ruolo richiesto.
    Chiama automaticamente require_login() se non già autenticato.
    """
    require_login()
    user_role = st.session_state.get("mdg_role", "business_role")
    if ROLE_HIERARCHY.get(user_role, 0) < ROLE_HIERARCHY.get(role, 99):
        st.error(f"🚫 Accesso negato. Questa pagina richiede il ruolo **{role}**.")
        st.stop()


def logout():
    """Invalida il token sul backend e cancella la sessione locale."""
    token = st.session_state.get("mdg_token")
    email = st.session_state.get("mdg_user", {}).get("email", "?")

    # Tenta revoca token sul backend
    if token:
        try:
            requests.post(
                f"{AUTH_API_URL}/auth/jwt/logout",
                headers={"Authorization": f"Bearer {token}"},
                timeout=REQUEST_TIMEOUT,
            )
        except Exception:
            pass

    logger.info(f"LOGOUT | user={email} | ts={datetime.now(timezone.utc).isoformat()}")

    for key in ["mdg_user", "mdg_token", "mdg_role", "must_change_password",
                "token_last_checked", "login_attempts", "login_locked_until"]:
        st.session_state.pop(key, None)
    st.rerun()


def render_user_badge():
    """Mostra nel sidebar le info dell'utente loggato e il pulsante logout."""
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
    st.sidebar.page_link("pages/9_info.py",                label="ℹ️ Info")
    st.sidebar.page_link("dashboard.py",                   label="📊 Cockpit controlli")
    st.sidebar.page_link("pages/1_check_results.py",       label="✅ Dettaglio esito controlli")
    st.sidebar.page_link("pages/5_view_data.py",           label="🗄️ Visualizza Dati")
    st.sidebar.page_link("pages/0_user_profile.py",        label="👤 Il mio profilo")

    # Pagine riservate agli IT user
    if role == "it_role":
        st.sidebar.divider()
        st.sidebar.caption("IT Role — Funzionalità avanzate")
        st.sidebar.page_link("pages/2_check_catalog.py",  label="📋 Catalogo controlli")
        st.sidebar.page_link("pages/3_pipeline_admin.py", label="⚙️ Gestion Pipeline")
        st.sidebar.page_link("pages/4_admin_users.py",    label="👥 Utenti")

    # Pagine riservate agli Admin
    elif role == "admin_role":
        st.sidebar.divider()
        st.sidebar.caption("Admin Role — Funzionalità avanzate")
        st.sidebar.page_link("pages/2_check_catalog.py",  label="📋 Catalogo controlli")
        st.sidebar.page_link("pages/3_pipeline_admin.py", label="⚙️ Gestion Pipeline")
        st.sidebar.page_link("pages/4_admin_users.py",    label="👥 Utenti")
        st.sidebar.page_link("pages/8_edit_tables.py",    label="✏️ Modifica Tabelle")
        st.sidebar.page_link("pages/7_targhette_diba.py", label="🔩 Targhette + DIBA")
        st.sidebar.page_link("pages/10_access_log.py",    label="🔐 Log Accessi")
