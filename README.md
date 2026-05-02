# MDG — Migration Data Governance

Pipeline di qualità dati per la migrazione ERP legacy → SAP S/4HANA.

## Scopo

L'applicazione MDG supporta il processo di migrazione dati da un ERP legacy verso **SAP S/4HANA**, garantendo la qualità e la coerenza dei dati prima del caricamento nel sistema di destinazione.

Ad ogni ciclo, la pipeline:
- Acquisisce i file esportati dall'ERP (ZIP/CSV) tramite SFTP e li carica nello **schema raw** di PostgreSQL
- Carica le **tabelle di controllo SAP** (codici paese, regioni, banche, ecc.) nello **schema ref**
- Esegue numerosi controlli di correttezza formale e logica su clienti, fornitori e dati anagrafici (P.IVA, codice fiscale, duplicati, coerenza geografica, ecc.)
- Consolida i risultati nello **schema stg**, rendendoli disponibili alla dashboard Streamlit
- Registra ogni run nella tabella **stg.pipeline_runs** con contatori di check superati ed errori

L'obiettivo è consentire al team funzionale SAP di identificare e correggere le anomalie **prima** del go-live, riducendo il rischio di dati inconsistenti nel sistema target.

---

## Stack

| Servizio        | Immagine                         | Scopo                                             |
|-----------------|----------------------------------|---------------------------------------------------|
| PostgreSQL      | postgres:18.3-alpine3.23         | Database MDG (raw / ref / stg / usr)              |
| PgAdmin         | dpage/pgadmin4                   | Admin UI database (solo it_role / admin_role)     |
| SFTPGo          | drakkan/sftpgo:latest            | Punto ingresso file ZIP/XLSX dall'ERP + WebAdmin  |
| Bruin           | build locale (./bruin/)          | Orchestratore pipeline qualità dati               |
| FastAPI         | build locale (./api/)            | API REST: avvio pipeline Bruin da Streamlit       |
| Auth API        | build locale (./auth/api/)       | Autenticazione JWT e gestione utenti (RBAC)       |
| Streamlit       | build locale (./streamlit/)      | Dashboard qualità dati                            |

Le porte di ogni servizio sono configurabili tramite il file `.env` (vedi `.env.example`).

---

## Autenticazione e ruoli

L'accesso alla webapp Streamlit è protetto da autenticazione JWT tramite **FastAPI-Users**.
Tutti gli utenti passano per `mdg_auth` prima di accedere a qualsiasi pagina.

| Ruolo           | Badge | Accesso                                                              |
|-----------------|-------|----------------------------------------------------------------------|
| `admin_role`    | 🔴    | Tutto — inclusa gestione utenti e modifica ruoli                     |
| `it_role`       | 🟡    | Dashboard, Check Results, Check Catalog, Pipeline Admin, PgAdmin     |
| `business_role` | 🟢    | Info, Dashboard, Check Results                                       |

Le credenziali sono memorizzate nello schema dedicato **`usr`** (`usr.users`), separato dagli schemi `raw`, `ref`, `stg` usati dalla pipeline.

---

## Architettura

```
Browser (utente) ──► mdg_auth ──► mdg_streamlit ──► mdg_postgres
                         │               │
                         └──► usr.users  └──► stg.check_results
                                               stg.pipeline_runs

Browser (IT/admin) ──► mdg_pgadmin ──► mdg_postgres

ERP legacy ──SFTP──► mdg_sftp ──► datalake volumes ──► mdg_bruin ──► mdg_postgres
                                                              ▲
                                             mdg_fastapi (docker exec)
                                                              │
                                             mdg_streamlit (Pipeline Admin)
```

---

## Struttura directory

```
mdg-v0/
├── docker-compose.yml              ← base comune (tutti gli ambienti)
├── docker-compose.override.yml     ← sviluppo locale (applicato automaticamente)
├── docker-compose.prod.yml         ← produzione OCI (applicato manualmente)
├── .env                            ← NON committare su git
├── .env.example                    ← template variabili d'ambiente
├── .gitignore
├── README.md
├── deploy.sh                       ← script deploy OCI
├── setup.sh                        ← genera chiavi SSH, crea directory
│
├── api/                            ← FastAPI pipeline runner
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
├── auth/                           ← FastAPI autenticazione JWT
│   └── api/
│       ├── Dockerfile
│       ├── auth_main.py            ← FastAPI-Users + RBAC 3 ruoli
│       └── requirements.txt
│
├── bruin/                          ← orchestratore pipeline
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── pipeline.yml
│   ├── requirements_bruin.txt
│   └── assets/
│       ├── ingestion/              ← ZIP/CSV/XLSX → raw.* e ref.*
│       ├── setup/                  ← init DB, check_catalog
│       ├── stg/                    ← check qualità → stg.check_results
│       └── prd/                    ← trasformazioni finali (step futuri)
│
├── pgadmin/
│   └── servers.json
│
├── sftpgo/
│   └── keys/                       ← chiavi SSH host (non committare)
│
└── streamlit/
    ├── Dockerfile
    ├── requirements.txt
    └── app/
        ├── mdg_auth.py             ← helper autenticazione (require_login, require_role)
        ├── Dashboard.py
        ├── .streamlit/
        │   └── config.toml
        └── pages/
            ├── 1_Check_Results.py
            ├── 2_Check_Catalog.py
            ├── 3_Pipeline_Admin.py ← avvio pipeline via mdg_fastapi
            ├── 4_Admin_Users.py    ← gestione utenti (solo it_role/admin_role)
            └── 9_Info.py           ← documentazione e diagramma architettura
```

---

## Schemi database

| Schema | Contenuto                                                               |
|--------|-------------------------------------------------------------------------|
| `raw`  | Tabelle dati ERP — una per ogni file CSV                                |
| `ref`  | Tabelle di controllo SAP (codici paese, banche, regioni...)             |
| `stg`  | `check_results`, `check_catalog`, `pipeline_runs`                       |
| `usr`  | `users` — credenziali e ruoli utenti (gestito da mdg_auth)              |

---

## Asset Bruin

| Cartella    | Contenuto                                                  |
|-------------|------------------------------------------------------------|
| `ingestion` | Unzip ZIP, load CSV in `raw.*`, load XLSX in `ref.*`       |
| `setup`     | DDL aggiuntivi, popolamento `stg.check_catalog`            |
| `stg`       | SQL dei check (CK001…CK404) → `stg.check_results`          |
| `prd`       | Trasformazioni finali e normalizzazioni (step futuri)      |

---

## Configurazione Docker Compose

Il progetto usa tre file per gestire i diversi ambienti da un'unica base di codice.

**Sviluppo locale** — Docker applica automaticamente base + override:
```bash
docker compose up -d
```

**Produzione OCI** — specificare esplicitamente il file prod:
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
# oppure tramite lo script:
./deploy.sh
./deploy.sh --build   # per ricompilare le immagini
```

---

## Avvio rapido (sviluppo locale)

```bash
# 1. Genera chiavi SSH e crea le directory necessarie
bash setup.sh

# 2. Copia e personalizza le variabili d'ambiente
cp .env.example .env
nano .env

# 3. Avvia lo stack
docker compose up -d

# 4. Verifica che tutti i container siano up
docker compose ps

# 5. Deposita i file sorgente nei volumi datalake via SFTP
sftp -P <SFTP_PORT> mdg_sftp@localhost
sftp> put file.zip from_olderp/

# Oppure via Docker cp diretto
docker cp file.zip mdg_sftp:/datalake/in_source_pprod/
```

---

## Variabili d'ambiente

Tutte le variabili di configurazione sono documentate nel file `.env.example`.
Copiarlo in `.env` e personalizzare prima di avviare lo stack.

```bash
cp .env.example .env
```

> ⚠️ Il file `.env` non deve mai essere committato su Git.
> Per generare `JWT_SECRET`: `openssl rand -hex 32`
