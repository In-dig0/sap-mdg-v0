"""
Ambiente SAP scelto nella barra laterale (DS4-200, QS4-200, PS4-100, ...): vale per tutte le pagine.

- render_env_selector(): selettore colorato nella barra laterale (chiamato da render_sidebar_menu).
- current_env(): ambiente scelto (DEFAULT = database e cartelle di sempre).
- db_params(): parametri di connessione al database dell'ambiente (<POSTGRES_DB>_<ambiente>).
- check_states_path(): file dello stato dei controlli dell'ambiente (bruin/config).
- api: sessione HTTP verso FastAPI con l'intestazione X-MDG-Env dell'ambiente scelto
  (stessa interfaccia di requests: api.get, api.post, api.put, api.delete).
Il registro degli ambienti è in FastAPI (/environments, database comune). Stessa regola dei nomi
di bruin/assets/mdg_env.py.
"""
import os
import time

import requests
import streamlit as st

API_BASE    = "http://mdg_fastapi:8000"
ENV_HEADER  = "X-MDG-Env"
DEFAULT_ENV = "DEFAULT"
CONFIG_DIR  = "/app/config"
SESSION_KEY = "mdg_env"
WIDGET_KEY  = "_mdg_env_selector"   # chiave del widget (azzerata da Streamlit al cambio pagina)
# Colori del badge (campo color del registro): nomi semplici o codici esadecimali
COLOR_MAP = {"red": "#d32f2f", "orange": "#ef6c00", "yellow": "#f9a825", "green": "#2e7d32",
             "blue": "#1565c0", "purple": "#6a1b9a", "gray": "#616161", "grey": "#616161"}


@st.cache_data(ttl=30, show_spinner=False)
def list_environments() -> list[dict]:
    """Ambienti registrati in FastAPI (tutti, anche disattivati); vuoto se l'API non risponde."""
    try:
        r = requests.get(f"{API_BASE}/environments", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


def current_env() -> str:
    return st.session_state.get(SESSION_KEY, DEFAULT_ENV)


def env_info(env: str | None = None) -> dict:
    env = env or current_env()
    if env == DEFAULT_ENV:
        return {"env_id": DEFAULT_ENV, "description": "Database e cartelle predefiniti", "color": "gray"}
    return next((e for e in list_environments() if e["env_id"] == env),
                {"env_id": env, "description": "", "color": "gray"})


def env_db_name(env: str | None = None) -> str:
    env = env or current_env()
    base = os.environ.get("POSTGRES_DB", "mdg")
    return base if env == DEFAULT_ENV else f"{base}_{env.lower().replace('-', '_')}"


def db_params(env: str | None = None) -> dict:
    """Parametri psycopg2 del database dell'ambiente scelto."""
    return {
        "host":     os.environ.get("POSTGRES_HOST", "postgres"),
        "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
        "dbname":   env_db_name(env),
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


def check_states_path(env: str | None = None) -> str:
    env = env or current_env()
    name = "check_states.json" if env == DEFAULT_ENV else f"check_states_{env}.json"
    return os.path.join(CONFIG_DIR, name)


def env_color(env: str | None = None) -> str:
    color = (env_info(env).get("color") or "gray").strip().lower()
    return COLOR_MAP.get(color, color if color.startswith("#") else COLOR_MAP["gray"])


def render_env_selector():
    """Selettore dell'ambiente nella barra laterale, con badge colorato dell'ambiente scelto."""
    active = [e["env_id"] for e in list_environments() if e.get("active")]
    options = [DEFAULT_ENV] + active
    if current_env() not in options:
        st.session_state[SESSION_KEY] = DEFAULT_ENV
    labels = {DEFAULT_ENV: "Predefinito (mdg)"}
    labels.update({e["env_id"]: f'{e["env_id"]} — {e.get("description") or ""}'.rstrip(" —")
                   for e in list_environments() if e.get("active")})
    # Il valore di un widget viene azzerato a ogni cambio di pagina: l'ambiente scelto resta in
    # SESSION_KEY (variabile di sessione normale) e il selettore lo legge e lo aggiorna
    def _store():
        st.session_state[SESSION_KEY] = st.session_state[WIDGET_KEY]

    st.sidebar.selectbox("🌐 Ambiente SAP", options, index=options.index(current_env()),
                         key=WIDGET_KEY, on_change=_store,
                         format_func=lambda e: labels.get(e, e),
                         help="Tutte le pagine (esiti, catalogo, pipeline, cartelle SFTP, dati) "
                              "lavorano sul database e sulle cartelle dell'ambiente scelto.")
    env = current_env()
    st.sidebar.markdown(
        f'<div style="background:{env_color(env)};color:#fff;padding:4px 10px;border-radius:6px;'
        f'font-weight:600;text-align:center">{env}</div>', unsafe_allow_html=True)


class _EnvApi:
    """requests con l'intestazione dell'ambiente scelto aggiunta a ogni chiamata."""

    def _call(self, method: str, url: str, **kwargs):
        headers = dict(kwargs.pop("headers", None) or {})
        headers[ENV_HEADER] = current_env()
        return requests.request(method, url, headers=headers, **kwargs)

    def get(self, url, **kw):    return self._call("GET", url, **kw)
    def post(self, url, **kw):   return self._call("POST", url, **kw)
    def put(self, url, **kw):    return self._call("PUT", url, **kw)
    def patch(self, url, **kw):  return self._call("PATCH", url, **kw)
    def delete(self, url, **kw): return self._call("DELETE", url, **kw)


api = _EnvApi()

