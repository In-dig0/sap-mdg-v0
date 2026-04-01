# MDG — Migration Data Governance

Pipeline di qualità dati per la migrazione ERP legacy → SAP S/4HANA.

## Stack v0 (sviluppo locale)

| Servizio    | Immagine                        | Porta  | Scopo                              |
|-------------|---------------------------------|--------|------------------------------------|
| PostgreSQL  | postgres:16-alpine              | 5432   | Database MDG (raw / ref / stg)     |
| PgAdmin     | dpage/pgadmin4                  | 8080   | Admin UI database                  |
| SFTP        | atmoz/sftp:alpine               | 22     | Punto ingresso file ZIP/XLSX       |
| Bruin       | build locale (./bruin/)         | —      | Orchestratore pipeline             |
| Streamlit   | build locale (./streamlit/)     | 8501   | Dashboard qualità dati             |

## Struttura directory

```
mdg-v0/
├── docker-compose.yml
├── .env                            <- NON committare su git
├── .gitignore
├── README.md
├── setup.sh
│
├── bruin/                          <- tutto il codice Bruin
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── pipeline.yml                <- connessioni + variabili pipeline
│   ├── requirements_bruin.txt
│   └── assets/
│       ├── ingestion/              <- ZIP/CSV/XLSX -> raw.* e ref.*
│       ├── setup/                  <- init DB, check_catalog
│       ├── stg/                    <- check qualità -> stg.check_results
│       └── prd/                    <- trasformazioni finali (future)
│
├── datalake/
│   ├── from_olderp/                <- ZIP/CSV dall'ERP legacy
│   └── from_sap/                   <- XLSX tabelle controllo SAP
│
├── db/
│   └── init/
│       └── 01_init_schemas.sql
│
├── pgadmin/
│   └── servers.json
│
├── sftp/
│   ├── ssh_host_ed25519_key        <- generata da setup.sh
│   └── ssh_host_rsa_key            <- generata da setup.sh
│
└── streamlit/
    ├── Dockerfile
    ├── requirements.txt
    └── app/
        └── dashboard.py
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

| Schema | Contenuto |
|--------|-----------|
| `raw`  | Tabelle dati ERP — una per ogni file CSV. |
| `ref`  | Tabelle di controllo SAP (codici paese, banche, regioni...). |
| `stg`  | `check_results`, `check_catalog`, `pipeline_runs`. |

## Asset Bruin — organizzazione

| Cartella    | Contenuto                                                  |
|-------------|------------------------------------------------------------|
| `ingestion` | Unzip ZIP, load CSV in `raw.*`, load XLSX in `ref.*`       |
| `setup`     | DDL aggiuntivi, popolamento `stg.check_catalog`            |
| `stg`       | SQL dei check (CHK01, CHK02...) -> `stg.check_results`     |
| `prd`       | Trasformazioni finali e normalizzazioni (step futuri)      |

## Avvio rapido

```bash
bash setup.sh          # genera chiavi SSH, crea directory
nano .env              # personalizza le password
docker compose up -d   # avvia tutti i servizi

# Deposita i file sorgente (oppure via SFTP)
cp *.zip   datalake/from_olderp/
cp *.xlsx  datalake/from_sap/

# Lancia una pipeline Bruin
docker exec -it mdg_bruin bruin run /pipelines/assets/ingestion
```

## URL servizi

| Servizio   | URL                   | Credenziali (default)              |
|------------|-----------------------|------------------------------------|
| PgAdmin    | http://localhost:8080 | admin@mdg.local / pgadmin_changeme |
| Streamlit  | http://localhost:8501 | —                                  |
| SFTP       | sftp://localhost:22   | mdg_erp / erp_changeme             |
| PostgreSQL | localhost:5432        | mdg_user / mdg_secret_changeme     |

## Deploy produzione OCI

1. Aggiornare `.env` con IP e credenziali OCI
2. Sostituire `atmoz/sftp` con `SFTPGo` (WebAdmin + eventi)
3. Aggiungere reverse proxy (Nginx/Caddy) per PgAdmin e Streamlit
4. Le cartelle `datalake/` diventano percorsi del VPS OCI
