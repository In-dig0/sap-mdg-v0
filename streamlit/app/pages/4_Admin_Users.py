"""
POSIZIONE: mdg-v0/streamlit/app/pages/4_Admin_Users.py
"""

import os
import json
import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from mdg_auth import require_role, render_sidebar_menu

st.set_page_config(page_title="Gestione Utenti | MDG", page_icon="👤", layout="wide")

require_role("it_role")
render_sidebar_menu()

AUTH_API_URL = os.getenv("AUTH_API_URL", "http://mdg_auth:8001")

ROLE_BADGE = {
    "admin_role":    "🔴 Admin",
    "it_role":       "🟡 IT",
    "business_role": "🟢 Business",
}
ROLE_OPTIONS = ["business_role", "it_role", "admin_role"]


def _headers():
    return {"Authorization": f"Bearer {st.session_state['mdg_token']}"}


@st.cache_data(ttl=10)
def fetch_users(token: str):
    try:
        r = requests.get(
            f"{AUTH_API_URL}/admin/users",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def patch_user(user_id: str, payload: dict) -> bool:
    r = requests.patch(
        f"{AUTH_API_URL}/admin/users/{user_id}",
        json=payload,
        headers=_headers(),
        timeout=5,
    )
    return r.status_code == 200


def reset_password(user_id: str, new_password: str) -> bool:
    r = requests.post(
        f"{AUTH_API_URL}/admin/users/{user_id}/password",
        json={"password": new_password},
        headers=_headers(),
        timeout=5,
    )
    return r.status_code == 200


def validate_password(pwd: str) -> list[str]:
    errors = []
    if len(pwd) < 8:
        errors.append("almeno 8 caratteri")
    if not any(c.isupper() for c in pwd):
        errors.append("almeno una lettera maiuscola")
    if not any(c.isdigit() for c in pwd):
        errors.append("almeno un numero")
    if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?/" for c in pwd):
        errors.append("almeno un carattere speciale")
    return errors


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("👤 Gestione Utenti")
st.caption("Sezione riservata agli amministratori MDG.")
st.divider()

tab_gestione, tab_backup = st.tabs(["👥 Gestione", "💾 Import / Export"])

# ===========================================================================
# TAB 1 — Gestione
# ===========================================================================
with tab_gestione:

    # --- Crea nuovo utente ---
    with st.expander("➕ Crea nuovo utente", expanded=False):
        with st.form("new_user_form"):
            col1, col2 = st.columns(2)
            new_email    = col1.text_input("Email")
            new_name     = col2.text_input("Nome completo")
            new_password = col1.text_input("Password", type="password")
            new_role     = col2.selectbox("Ruolo", ROLE_OPTIONS)
            submitted    = st.form_submit_button("Crea utente")

        if submitted:
            payload = {
                "email":     new_email,
                "password":  new_password,
                "full_name": new_name,
                "is_active": True,
            }
            r = requests.post(
                f"{AUTH_API_URL}/auth/register",
                json=payload,
                headers=_headers(),
                timeout=5,
            )
            if r.status_code == 201:
                user_id = r.json().get("id")
                # Imposta ruolo
                requests.patch(
                    f"{AUTH_API_URL}/admin/users/{user_id}",
                    json={"role": new_role},
                    headers=_headers(),
                    timeout=5,
                )
                # Imposta flag cambio password obbligatorio al primo accesso
                requests.patch(
                    f"{AUTH_API_URL}/admin/users/{user_id}/force-password-change",
                    json={"must_change_password": True},
                    headers=_headers(),
                    timeout=5,
                )
                st.success(f"Utente **{new_email}** creato con ruolo `{new_role}`. Al primo accesso dovrà cambiare la password.")
                st.cache_data.clear()
            else:
                st.error(f"Errore: {r.json().get('detail', r.text)}")

    # --- Lista utenti ---
    st.divider()
    st.subheader("Utenti registrati")

    users = fetch_users(st.session_state["mdg_token"])

    if st.session_state.get("success_msg"):
        st.success(st.session_state.pop("success_msg"))

    if not users:
        st.info("Nessun utente trovato.")
    else:
        h1, h2, h3, h4, h5, h6 = st.columns([2, 3, 2, 1, 1, 1])
        h1.markdown("**Nome**")
        h2.markdown("**Email**")
        h3.markdown("**Ruolo**")
        h4.markdown("**Stato**")
        h5.markdown("**Psw**")
        h6.markdown("**Modifica**")
        st.divider()

        for u in users:
            c1, c2, c3, c4, c5, c6 = st.columns([2, 3, 2, 1, 1, 1])
            c1.write(u.get("full_name") or "—")
            c2.write(f"`{u['email']}`")
            c3.write(ROLE_BADGE.get(u.get("role", ""), u.get("role", "—")))
            c4.write("✅" if u.get("is_active") else "⛔")
            c5.write(
                "⏳ Da cambiare" if u.get("must_change_password") else "✅ Ok",
            )

            edit_key = f"edit_{u['id']}"
            current_role = st.session_state.get("mdg_role", "")
            can_edit = not (current_role == "it_role" and u.get("role") == "admin_role")
            if can_edit:
                if c6.button("✏️", key=f"btn_{u['id']}", help="Modifica utente",
                             use_container_width=True):
                    st.session_state[edit_key] = not st.session_state.get(edit_key, False)
            else:
                c6.markdown("<span title='Solo admin può modificare questo utente'>🔒</span>",
                            unsafe_allow_html=True)

            if st.session_state.get(edit_key, False):
                with st.container(border=True):
                    st.markdown(f"**Modifica:** `{u['email']}`")
                    ef1, ef2, ef3 = st.columns(3)
                    new_role_val = ef1.selectbox(
                        "Ruolo", options=ROLE_OPTIONS,
                        index=ROLE_OPTIONS.index(u.get("role", "business_role")),
                        key=f"role_{u['id']}",
                    )
                    new_pwd = ef2.text_input(
                        "Nuova password",
                        type="password",
                        placeholder="lascia vuoto per non cambiare",
                        key=f"pwd_{u['id']}",
                    )
                    is_active_val = ef3.checkbox(
                        "Utente attivo",
                        value=u.get("is_active", True),
                        key=f"active_{u['id']}",
                    )
                    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                    bs, bc, _ = st.columns([1, 1, 6])

                    if bs.button("💾 Salva", key=f"save_{u['id']}", use_container_width=True):
                        errors = []
                        if new_pwd:
                            errors = validate_password(new_pwd)
                            if errors:
                                st.error("Password non valida: " + ", ".join(errors) + ".")
                        if not errors:
                            ok = True
                            updates = {}
                            if new_role_val != u.get("role"):
                                updates["role"] = new_role_val
                            if is_active_val != u.get("is_active"):
                                updates["is_active"] = is_active_val
                            if updates:
                                ok = patch_user(u["id"], updates)
                                if ok:
                                    st.toast("Dati utente aggiornati", icon="✅")
                            if new_pwd:
                                ok = reset_password(u["id"], new_pwd)
                                if not ok:
                                    st.error("Errore nel cambio password.")
                            if ok:
                                msgs = []
                                if updates:
                                    msgs.append("dati aggiornati")
                                if new_pwd:
                                    msgs.append("password reimpostata")
                                st.session_state["success_msg"] = (
                                    f"✅ Utente `{u['email']}`: {' · '.join(msgs)}."
                                )
                                st.session_state[edit_key] = False
                                st.cache_data.clear()
                                st.rerun()

                    if bc.button("✖ Annulla", key=f"cancel_{u['id']}", use_container_width=True):
                        st.session_state[edit_key] = False
                        st.rerun()

    if st.session_state.get("success_msg"):
        st.divider()
        st.success(st.session_state.pop("success_msg"))

# ===========================================================================
# TAB 2 — Import / Export
# ===========================================================================
with tab_backup:
    st.subheader("💾 Backup e ripristino utenti")
    st.caption("Esporta la lista utenti su JSON o ripristinala su un nuovo ambiente. Le password NON vengono esportate.")
    st.divider()

    # -----------------------------------------------------------------------
    # EXPORT
    # -----------------------------------------------------------------------
    st.markdown("#### ⬇️ Export")
    st.write("Scarica la lista utenti in formato JSON — include email, nome e ruolo. Le password hash non vengono incluse.")

    if st.button("Genera file di export", use_container_width=False):
        users_exp = fetch_users(st.session_state["mdg_token"])
        if not users_exp:
            st.warning("Nessun utente da esportare.")
        else:
            export_data = [
                {
                    "email":     u["email"],
                    "full_name": u.get("full_name", ""),
                    "role":      u.get("role", "business_role"),
                    "is_active": u.get("is_active", True),
                }
                for u in users_exp
            ]
            json_bytes = json.dumps(export_data, indent=2).encode("utf-8")
            st.success(f"✅ {len(export_data)} utenti pronti per il download.")

            df_exp = pd.DataFrame(export_data)
            df_exp.columns = ["Email", "Nome", "Ruolo", "Attivo"]
            df_exp["Ruolo"] = df_exp["Ruolo"].map(lambda r: ROLE_BADGE.get(r, r))
            df_exp["Attivo"] = df_exp["Attivo"].map(lambda a: "✅" if a else "⛔")
            st.dataframe(df_exp, use_container_width=True, hide_index=True)

            st.download_button(
                label=f"📥 Scarica users_{datetime.now().strftime('%Y%d%m')}.json ({len(export_data)} utenti)",
                data=json_bytes,
                file_name=f"users_{datetime.now().strftime('%Y%d%m')}.json",
                mime="application/json",
            )

    st.divider()

    # -----------------------------------------------------------------------
    # IMPORT
    # -----------------------------------------------------------------------
    st.markdown("#### ⬆️ Import")
    st.write("Carica un file JSON esportato. Gli utenti esistenti vengono aggiornati (ruolo e stato), i nuovi creati con password temporanea.")

    DEFAULT_PWD = st.text_input(
        "Password temporanea per i nuovi utenti",
        type="password",
        placeholder="Es. Temp1234!",
        help="Verrà assegnata solo agli utenti nuovi. Gli utenti esistenti non cambieranno password.",
    )

    uploaded = st.file_uploader("Seleziona users_*.json", type=["json"], key="users_upload")

    if uploaded:
        try:
            records = json.loads(uploaded.read().decode("utf-8"))

            # Recupera email esistenti
            existing_users = fetch_users(st.session_state["mdg_token"])
            existing_emails = {u["email"]: u for u in existing_users}

            new_records = [r for r in records if r["email"] not in existing_emails]
            upd_records = [r for r in records if r["email"] in existing_emails]

            st.info(f"File caricato: **{len(records)} utenti** — 🟢 {len(new_records)} nuovi, 🟡 {len(upd_records)} da aggiornare.")

            rows_preview = []
            for r in records:
                rows_preview.append({
                    "Esito atteso": "🟢 Nuovo" if r["email"] not in existing_emails else "🟡 Aggiornamento",
                    "Email":        r["email"],
                    "Nome":         r.get("full_name", ""),
                    "Ruolo":        ROLE_BADGE.get(r.get("role", ""), r.get("role", "")),
                    "Attivo":       "✅" if r.get("is_active", True) else "⛔",
                })
            st.dataframe(pd.DataFrame(rows_preview), use_container_width=True, hide_index=True)

            if st.button("✅ Conferma import nel database", use_container_width=False):
                if new_records and not DEFAULT_PWD:
                    st.error("Inserisci una password temporanea per i nuovi utenti.")
                else:
                    errors_list = []
                    ok_new = ok_upd = 0

                    # Nuovi utenti
                    for r in new_records:
                        res = requests.post(
                            f"{AUTH_API_URL}/auth/register",
                            json={"email": r["email"], "password": DEFAULT_PWD,
                                  "full_name": r.get("full_name", ""), "is_active": r.get("is_active", True)},
                            headers=_headers(), timeout=5,
                        )
                        if res.status_code == 201:
                            uid = res.json().get("id")
                            requests.patch(f"{AUTH_API_URL}/admin/users/{uid}",
                                           json={"role": r.get("role", "business_role")},
                                           headers=_headers(), timeout=5)
                            requests.patch(f"{AUTH_API_URL}/admin/users/{uid}/force-password-change",
                                           json={"must_change_password": True},
                                           headers=_headers(), timeout=5)
                            ok_new += 1
                        else:
                            errors_list.append(f"{r['email']}: {res.text}")

                    # Utenti esistenti — aggiorna ruolo e stato
                    for r in upd_records:
                        uid = existing_emails[r["email"]]["id"]
                        patch_user(uid, {
                            "role":      r.get("role", "business_role"),
                            "is_active": r.get("is_active", True),
                        })
                        ok_upd += 1

                    st.success(f"✅ Import completato — **{ok_new}** creati, **{ok_upd}** aggiornati.")
                    if errors_list:
                        st.warning("Errori su alcuni utenti:\n" + "\n".join(errors_list))
                    st.cache_data.clear()

        except json.JSONDecodeError:
            st.error("File JSON non valido.")
        except Exception as e:
            st.error(f"Errore import: {e}")
