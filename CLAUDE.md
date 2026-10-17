# CLAUDE.md — MDG (Migration Data Governance)

Pipeline di qualità dati per la migrazione da un ERP legacy a **SAP S/4HANA**.
L'ERP produce file di estrazione secondo i modelli SAP; MDG li carica in PostgreSQL,
esegue controlli di coerenza formale e di merito e ne mostra l'esito in una dashboard Streamlit.

> Se un'informazione di questo file non corrisponde al codice, **vale il codice**:
> segnalamelo e proponi l'aggiornamento di questo file.

## Flusso dei dati

1. L'ERP genera N file **ZIP**, uno per step di migrazione (es. `01a_BP_Vettori.zip`,
   `01b_BP_Clienti.zip`, `01c_BP_Fornitori.zip`), ognuno con M file **CSV** con i campi dei modelli SAP.
   Arrivano via **SFTP** (SFTPGo) nel datalake (`in_source_pprod`).
2. Le **tabelle di controllo SAP** (paesi, regioni, banche, ...) arrivano come file **XLSX**.
3. La pipeline **Bruin**:
   - carica i CSV nello schema `raw` (una tabella per file CSV, stesso nome);
   - carica gli XLSX nello schema `ref`;
   - esegue i check (asset `stg.ckNNN_*`) e scrive gli esiti in `stg.check_results`;
   - registra ogni esecuzione in `stg.pipeline_runs` (asset di apertura/chiusura run);
   - unisce i dati validati (`prd_merge_*`) ed esporta gli ZIP per SAP (`prd_export_*`).
4. **Streamlit** mostra cockpit, dettaglio esiti, catalogo controlli e gestione pipeline.

Cartelle del datalake (volumi Docker, montate in Bruin sotto `/project/datalake/`):

| Cartella           | Contenuto                                                         | Caricata da / in                  |
|--------------------|-------------------------------------------------------------------|-----------------------------------|
| `in_source_pprod`  | ZIP/CSV estratti dall'ERP attuale (quello sostituito da S/4HANA); include il semaforo `DATASET_READY.txt` | `ingest_zip_to_raw` → `raw` |
| `in_source_sap`    | Tabelle di controllo SAP in XLSX                                  | `ingest_xlsx_to_ref` → `ref`      |
| `in_source_others` | Altri file XLSX di supporto                                       | `ingest_others_to_stg` → `stg`    |
| `out_source_mdg`   | Output di MDG: ZIP per il caricamento in SAP e CSV degli scarti   | scritta da `prd_export_*`         |

In Bruin le cartelle `in_source_*` sono montate in sola lettura.

Asset Bruin (`bruin/assets/`): `setup/` (init db, catalogo check, apertura run),
`ingestion/` (caricamento file), `stg/` (check `ckNNN_*`), `prd/` (merge, export, chiusura run).

Schemi PostgreSQL (db `mdg`): `raw`, `ref`, `stg`, `prd`, `usr` (utenti applicativi).

## Servizi (Docker Compose)

| Servizio    | Container       | Cartella         | Ruolo                                                   |
|-------------|-----------------|------------------|---------------------------------------------------------|
| `postgres`  | `mdg_postgres`  | —                | Database MDG (PostgreSQL 18)                            |
| `pgadmin`   | `mdg_pgadmin`   | `pgadmin/`       | Admin UI database                                       |
| `sftp`      | `mdg_sftp`      | `sftpgo/`        | SFTPGo: ingresso file ZIP/XLSX dall'ERP                 |
| `bruin`     | `mdg_bruin`     | `bruin/`         | Orchestratore pipeline (`sleep infinity`, si usa con `docker exec`) |
| `fastapi`   | `mdg_fastapi`   | `api/`           | API REST: avvio pipeline Bruin da Streamlit             |
| `mdg_auth`  | `mdg_auth`      | `auth/api/`      | Autenticazione JWT (FastAPI-Users) e gestione utenti    |
| `streamlit` | `mdg_streamlit` | `streamlit/app/` | Dashboard (`dashboard.py` + `pages/`, vedi sotto)       |

Pagine Streamlit (`streamlit/app/pages/`): profilo utente, esiti check, catalogo controlli,
gestione pipeline, gestione utenti, consultazione dati, targhette matricola, targhette DiBa,
modifica tabelle, info, log accessi. `pages/backup/` contiene vecchie versioni non attive.

Ruoli applicativi (RBAC): `admin_role` (tutto, gestione utenti), `it_role`
(dashboard, esiti, catalogo, gestione pipeline, PgAdmin), `business_role` (info, dashboard, esiti).

Volumi con nome fisso (`mdg_postgres_data`, `mdg_datalake_*`, `mdg_pgadmin_data`,
`mdg_sftpgo_data`, `mdg_api_logs`): sopravvivono a `docker compose down` e vengono
riagganciati a ogni avvio. **Mai** usare `docker compose down -v` senza conferma esplicita.

## Ambienti

**Sviluppo locale** — Windows + WSL2 (Ubuntu 24.04), cartella `/mnt/c/DEV/Python/Projects/sap-mdg-v0`.
- `docker-compose.yml` + `docker-compose.override.yml` (letto automaticamente):
  espone le porte sull'host e imposta `restart: "no"`.
- Porte (dal `.env` locale): Streamlit 8503, PgAdmin 8081, SFTPGo WebAdmin 8082,
  SFTP 2222, FastAPI 8002, PostgreSQL 5432. Auth usa sempre la porta 8001
  (fissa nei file compose, non letta dal `.env`).

**Produzione** — VPS Oracle Cloud (OCI), **ARM64 / aarch64** (Ampere A1).
- `docker-compose.yml` + `docker-compose.prod.yml` (l'override locale NON viene letto);
  porte legate a `127.0.0.1` (dietro Nginx), tranne SFTP che è pubblica;
  PostgreSQL non espone porte. `restart: unless-stopped`.
- Bruin non ha un'immagine ARM64 ufficiale: il `bruin/Dockerfile` scarica il binario arm64.
  Ogni nuova immagine o dipendenza deve funzionare **sia su amd64 sia su arm64**.

## Comandi utili

```bash
# Avvio / arresto locale
docker compose up -d
docker compose ps
docker compose down

# Ricostruire un solo servizio dopo una modifica
docker compose up -d --build streamlit

# Log di un servizio
docker logs mdg_streamlit --tail 50

# Bruin
docker exec -it mdg_bruin bruin validate /project/bruin
docker exec -it mdg_bruin bruin lineage /project/bruin

# Query sul database
docker exec -it mdg_postgres psql -U <POSTGRES_USER> -d mdg -c "..."

# Ambiente Python lato host (script di servizio)
uv venv && uv pip install -r requirements-host.txt

# Deploy in produzione (sulla VPS)
git pull
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

## Flusso Git

Repo: https://github.com/In-dig0/sap-mdg-v0 — si lavora **direttamente su `main`**
(niente branch, per semplicità).
1. Sviluppo e test in locale su `main`, commit.
2. `git push` (solo su richiesta esplicita).
3. Sulla VPS: `git pull` + rebuild con i file di produzione.

Messaggi di commit in italiano, stile Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`).

## Regole

- **Non leggere, modificare o committare** `.env`, `sftpgo/keys/*` e qualunque credenziale.
  `.bruin.yml` è generato da `setup.sh` a partire dal `.env`: non modificarlo a mano.
- `bruin/config/check_states.json` (stato attivo/inattivo dei check, scritto dalla pagina
  Check Catalog) **non è tracciato** da Git: ogni ambiente ha il proprio.
- **Fine riga LF** su tutti i file (girano nei container Linux), imposto da
  `.gitattributes` (`* text=auto eol=lf`). Controllo: `git ls-files --eol | grep crlf` deve essere vuoto.
- **Nomi file**: attenzione alle maiuscole (Windows non le distingue, Linux sì);
  per rinominare usare `git mv`.
- Modifiche a `docker-compose.yml` impattano anche la produzione: verificare sempre
  la compatibilità con `docker-compose.prod.yml`.
- I check Bruin devono restare **idempotenti** (rilanciabili senza duplicare risultati).
- Interfaccia utente e messaggi dei check in **italiano**.
- Prima di dichiarare conclusa una modifica: ricostruire il servizio interessato
  e verificarne il funzionamento (log, pagina Streamlit o esecuzione pipeline).

## Evolutive in corso

<!-- Da completare: elenco delle modifiche da implementare -->
