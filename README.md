# MDG — Migration Data Governance

Pipeline di qualità dati per la migrazione ERP legacy → SAP S/4HANA.

## Stack v0 (sviluppo locale)

| Servizio    | Immagine                        | Porta  | Scopo                                          |
|-------------|---------------------------------|--------|------------------------------------------------|
| PostgreSQL  | postgres:18-alpine              | 5432   | Database MDG (raw / ref / stg / usr)           |
| PgAdmin     | dpage/pgadmin4                  | 8080   | Admin UI database (solo IT user)               |
| SFTP        | atmoz/sftp:alpine               | 22     | Punto ingresso file ZIP/XLSX dall'ERP          |
| Bruin       | build locale (./bruin/)         | —      | Orchestratore pipeline qualità dati            |
| FastAPI     | build locale (./api/)           | 8000   | API REST: avvio pipeline Bruin da Streamlit    |
| Auth API    | build locale (./auth/api/)      | 8001   | Autenticazione JWT e gestione utenti (RBAC)    |
| Streamlit   | build locale (./streamlit/)     | 8501   | Dashboard qualità dati                         |

## Autenticazione e ruoli

L'accesso alla webapp Streamlit è protetto da autenticazione JWT tramite **FastAPI-Users**.
Tutti gli utenti passano per `mdg_auth` prima di accedere a qualsiasi pagina.

### Ruoli disponibili

| Ruolo           | Badge | Accesso                                                              |
|-----------------|-------|----------------------------------------------------------------------|
| `admin_role`    | 🔴    | Tutto — inclusa gestione utenti e modifica ruoli                     |
| `it_role`       | 🟡    | Dashboard, Check Results, Check Catalog, Pipeline Admin, PgAdmin     |
| `business_role` | 🟢    | Info, Dashboard, Check Results                                       |

### Schema database utenti

Le credenziali sono memorizzate nello schema dedicato **`usr`** del database MDG (`usr.users`),
separato dagli schemi `raw`, `ref`, `stg` usati dalla pipeline.

### Primo avvio — creazione utente admin

```bash
# Assicurarsi che AUTH_API_URL=http://localhost:8001 nel .env
uv run create_first_admin.py

# Poi reimpostare AUTH_API_URL=http://mdg_auth:8001 nel .env
```

## Architettura

```
Browser (Business user) ──►┐
                            ├──► mdg_auth :8001 ──► mdg_streamlit :8501 ──► mdg_postgres :5432
Browser (IT user)      ──►─┘         │                     │
                            │         └──► usr.users         └──► stg.check_results
                            │
                            └──► mdg_pgadmin :8080 ──► mdg_postgres :5432

ERP legacy ──SFTP──► mdg_sftp :22 ──► mdg_bruin ──► mdg_postgres
                                           ▲
                              mdg_fastapi :8000 (docker exec)
                                           │
                              mdg_streamlit (Pipeline Admin)
```

## Struttura directory

```
mdg-v0/
├── docker-compose.yml
├── .env                            <- NON committare su git
├── .env.example                    <- template variabili d'ambiente
├── .gitignore
├── README.md
├── setup.sh
├── create_first_admin.py           <- script creazione primo utente admin
│
├── api/                            <- FastAPI pipeline runner
│   ├── Dockerfile
│   ├── main.py                     <- endpoint avvio/stato/log pipeline Bruin
│   └── requirements.txt
│
├── auth/                           <- FastAPI autenticazione JWT
│   └── api/
│       ├── Dockerfile
│       ├── auth_main.py            <- FastAPI-Users + RBAC 3 ruoli
│       └── requirements.txt
│
├── bruin/                          <- orchestratore pipeline
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── pipeline.yml
│   ├── requirements_bruin.txt
│   └── assets/
│       ├── ingestion/              <- ZIP/CSV/XLSX → raw.* e ref.*
│       ├── setup/                  <- init DB, check_catalog
│       ├── stg/                    <- check qualità → stg.check_results
│       └── prd/                    <- trasformazioni finali (future)
│
├── datalake/
│   ├── from_olderp/                <- ZIP/CSV dall'ERP legacy
│   └── from_sap/                   <- XLSX tabelle controllo SAP
│
├── pgadmin/
│   └── servers.json
│
├── sftp/
│   ├── ssh_host_ed25519_key        <- generata da setup.sh (non committare)
│   └── ssh_host_rsa_key            <- generata da setup.sh (non committare)
│
└── streamlit/
    ├── Dockerfile
    ├── requirements.txt
    └── app/
        ├── mdg_auth.py             <- helper autenticazione (require_login, require_role)
        ├── Dashboard.py
        ├── .streamlit/
        │   └── config.toml         <- nasconde menu nativo Streamlit
        └── pages/
            ├── 1_Check_Results.py
            ├── 2_Check_Catalog.py
            ├── 3_Pipeline_Admin.py <- avvio pipeline via mdg_fastapi
            ├── 4_Admin_Users.py    <- gestione utenti (solo it_role/admin_role)
            └── 9_Info.py           <- documentazione e diagramma architettura
```

## Flusso dati

```
ERP legacy  --SFTP-->  datalake/from_olderp/  -->  Bruin ingestion  -->  raw.*
SAP tables  --SFTP-->  datalake/from_sap/     -->  Bruin ingestion  -->  ref.*
                                                          |
                                                     Bruin stg checks
                                                          |
                                                    stg.check_results
                                                          |
                                                  Streamlit dashboard
```

## Schemi database

| Schema | Contenuto                                                               |
|--------|-------------------------------------------------------------------------|
| `raw`  | Tabelle dati ERP — una per ogni file CSV                                |
| `ref`  | Tabelle di controllo SAP (codici paese, banche, regioni...)             |
| `stg`  | `check_results`, `check_catalog`, `pipeline_runs`                       |
| `usr`  | `users` — credenziali e ruoli utenti (gestito da mdg_auth)             |

## Asset Bruin

| Cartella    | Contenuto                                                  |
|-------------|------------------------------------------------------------|
| `ingestion` | Unzip ZIP, load CSV in `raw.*`, load XLSX in `ref.*`       |
| `setup`     | DDL aggiuntivi, popolamento `stg.check_catalog`            |
| `stg`       | SQL dei check (CK001…CK404) → `stg.check_results`          |
| `prd`       | Trasformazioni finali e normalizzazioni (step futuri)      |

## Avvio rapido

```bash
bash setup.sh          # genera chiavi SSH, crea directory
nano .env              # personalizza le password e JWT_SECRET
docker compose up -d   # avvia tutti i servizi

# Crea il primo utente admin (con AUTH_API_URL=http://localhost:8001 nel .env)
uv run create_first_admin.py

# Deposita i file sorgente nella inbox SFTP
cp *.zip   datalake/from_olderp/
cp *.xlsx  datalake/from_sap/

# Oppure via client SFTP
sftp -P 22 mdg_sftp@localhost
sftp> put file.zip from_olderp/
```

## URL servizi

| Servizio    | URL                    | Note                                    |
|-------------|------------------------|-----------------------------------------|
| Streamlit   | http://localhost:8501  | Login richiesto (mdg_auth)              |
| Auth API    | http://localhost:8001  | Docs: http://localhost:8001/docs        |
| Pipeline API| http://localhost:8000  | Docs: http://localhost:8000/docs        |
| PgAdmin     | http://localhost:8080  | Solo IT user / admin                    |
| SFTP        | sftp://localhost:22    | Client: `sftp -P 22 mdg_sftp@localhost` |
| PostgreSQL  | localhost:5432         | Credenziali nel .env                    |

## Variabili d'ambiente (.env)

```dotenv
# Postgres
POSTGRES_DB=mdg
POSTGRES_USER=mdg_user
POSTGRES_PASSWORD=<password>

# PgAdmin
PGADMIN_DEFAULT_EMAIL=<email>
PGADMIN_DEFAULT_PASSWORD=<password>

# SFTP
SFTP_PORT=22
SFTP_USERS=mdg_sftp:<password>:1001:1001:from_olderp,from_sap

# Auth JWT
JWT_SECRET=<genera con: openssl rand -hex 32>
AUTH_API_URL=http://mdg_auth:8001

# Primo admin (usato da create_first_admin.py)
ADMIN_EMAIL=admin@esempio.it
ADMIN_PASSWORD=<password>
ADMIN_NAME=Amministratore MDG
```

## Deploy produzione OCI

1. Aggiornare `.env` con IP e credenziali OCI
2. Aggiungere reverse proxy Traefik/Nginx con HTTPS
3. Sostituire `atmoz/sftp` con `SFTPGo` (WebAdmin + eventi)
4. Le cartelle `datalake/` diventano percorsi del VPS OCI
5. Rimuovere le porte esposte di `mdg_auth` e `mdg_fastapi` dal `docker-compose.yml`
