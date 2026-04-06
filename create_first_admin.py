"""
POSIZIONE: mdg-v0/create_first_admin.py

Crea l'utente admin iniziale e imposta il ruolo admin_role direttamente su Postgres,
poiché fastapi-users ignora il campo role durante la registrazione pubblica.
"""

import os
import requests
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

AUTH_API_URL   = os.getenv("AUTH_API_URL",   "http://localhost:8001")
ADMIN_EMAIL    = os.getenv("ADMIN_EMAIL",    "admin@mdg.it")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "Admin1234!")
ADMIN_NAME     = os.getenv("ADMIN_NAME",     "Amministratore MDG")

POSTGRES_HOST  = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT  = os.getenv("POSTGRES_PORT",     "5432")
POSTGRES_DB    = os.getenv("POSTGRES_DB",       "mdg")
POSTGRES_USER  = os.getenv("POSTGRES_USER",     "mdg_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")


def set_role_in_db(email: str, role: str):
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE usr.users SET role = %s WHERE email = %s",
                (role, email)
            )
            updated = cur.rowcount
        conn.commit()
        return updated
    finally:
        conn.close()


def main():
    print(f"Creazione utente admin: {ADMIN_EMAIL}")
    payload = {
        "email":     ADMIN_EMAIL,
        "password":  ADMIN_PASSWORD,
        "full_name": ADMIN_NAME,
        "is_active": True,
    }
    r = requests.post(f"{AUTH_API_URL}/auth/register", json=payload, timeout=10)

    if r.status_code == 201:
        print("✅ Utente registrato.")
    elif r.status_code == 400 and ("already exists" in r.text or "ALREADY_EXISTS" in r.text):
        print(f"ℹ️  Utente {ADMIN_EMAIL} già esistente — aggiorno solo il ruolo.")
    else:
        print(f"❌ Errore registrazione {r.status_code}: {r.text}")
        return

    # Imposta il ruolo admin_role direttamente su Postgres
    updated = set_role_in_db(ADMIN_EMAIL, "admin_role")
    if updated:
        print(f"✅ Ruolo 'admin_role' impostato su usr.users per {ADMIN_EMAIL}")
    else:
        print(f"❌ Utente non trovato in usr.users — controlla il DB.")
        return

    print()
    print(f"   Email:    {ADMIN_EMAIL}")
    print(f"   Password: {ADMIN_PASSWORD}")
    print(f"   Ruolo:    admin_role")
    print()
    print("⚠️  Cambia la password al primo accesso!")


if __name__ == "__main__":
    main()
