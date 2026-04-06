"""
MDG — Migration Data Governance
Info — Scopo della pipeline e architettura dei container
"""

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Info — MDG",
    page_icon="ℹ️",
    layout="wide",
)

from mdg_auth import require_login, render_sidebar_menu
require_login()
render_sidebar_menu()

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@400;600;800&display=swap');
.mdg-title { font-family:'Syne',sans-serif; font-size:2.4rem; font-weight:800; letter-spacing:-0.03em; margin-bottom:0; }
.mdg-subtitle { font-family:'Syne',sans-serif; font-size:1rem; color:#6b7280; margin-top:0.2rem; margin-bottom:2rem; }
.section-title { font-family:'Syne',sans-serif; font-size:1.1rem; font-weight:600; text-transform:uppercase; letter-spacing:0.12em; color:#9ca3af; margin-bottom:0.8rem; margin-top:2rem; }
.scope-box { background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%); border:1px solid #334155; border-left:4px solid #3b82f6; border-radius:8px; padding:1.4rem 1.8rem; font-family:'Syne',sans-serif; font-size:0.95rem; line-height:1.8; color:#cbd5e1; margin-bottom:1rem; }
.scope-box b { color:#93c5fd; }
.container-card { background:#0f172a; border:1px solid #1e293b; border-radius:10px; padding:1rem 1.2rem; margin-top:0.5rem; }
.container-card .c-name { font-family:'JetBrains Mono',monospace; font-size:0.85rem; font-weight:600; color:#f8fafc; margin-bottom:0.2rem; }
.container-card .c-image { font-family:'JetBrains Mono',monospace; font-size:0.72rem; color:#64748b; margin-bottom:0.5rem; }
.container-card .c-desc { font-family:'Syne',sans-serif; font-size:0.82rem; color:#94a3b8; line-height:1.5; }
.badge { display:inline-block; font-family:'JetBrains Mono',monospace; font-size:0.65rem; font-weight:600; padding:2px 8px; border-radius:4px; margin-bottom:6px; }
.badge-db   { background:#164e63; color:#67e8f9; }
.badge-ui   { background:#14532d; color:#86efac; }
.badge-etl  { background:#3b0764; color:#d8b4fe; }
.badge-api  { background:#7c2d12; color:#fdba74; }
.badge-sftp { background:#1e3a5f; color:#93c5fd; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="mdg-title">ℹ️ MDG — Migration Data Governance</div>', unsafe_allow_html=True)
st.markdown('<div class="mdg-subtitle">Documentazione tecnica · v0 · Sviluppo locale (WSL2 + Docker)</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Scopo
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Scopo della pipeline</div>', unsafe_allow_html=True)

st.markdown("""
<div class="scope-box">
La pipeline <b>MDG</b> supporta il processo di migrazione dati da un ERP legacy verso <b>SAP S/4HANA</b>,
garantendo la qualità e la coerenza dei dati prima del caricamento nel sistema di destinazione.<br><br>
Ad ogni ciclo, la pipeline:
<ul style="margin-top:0.5rem; padding-left:1.2rem;">
  <li>Acquisisce i file esportati dall'ERP (ZIP/CSV) tramite SFTP e li carica nello <b>schema raw</b> su PostgreSQL</li>
  <li>Carica le <b>tabelle di controllo SAP</b> (codici paese, regioni, banche, ecc.) nello <b>schema ref</b></li>
  <li>Esegue <b>numerosi controlli di correttezza formale e logica</b> su clienti, fornitori e dati anagrafici (P.IVA, codice fiscale, duplicati, coerenza geografica, ecc.)</li>
  <li>Consolida i risultati nello <b>schema stg</b>, rendendoli disponibili alla dashboard Streamlit per l'analisi</li>
  <li>Registra ogni run nella tabella <b>stg.pipeline_runs</b> con contatori di check superati ed errori</li>
</ul>
L'obiettivo è consentire al team funzionale SAP di identificare e correggere le anomalie <b>prima</b> del go-live,
riducendo il rischio di dati inconsistenti nel sistema target.
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Diagramma architettura
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Architettura</div>', unsafe_allow_html=True)

svg = """
<svg viewBox="0 0 1060 530" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:1060px;display:block;margin:0 auto 1.5rem;">
  <defs>
    <marker id="arr" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#475569"/>
    </marker>
    <marker id="arr-dash" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#374151"/>
    </marker>
    <marker id="arr-auth" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#6d28d9"/>
    </marker>
    <marker id="arr-orange" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#f97316"/>
    </marker>
  </defs>

  <!-- Sfondo -->
  <rect width="1060" height="530" rx="14" fill="#0a0f1e"/>

  <!-- Rete Docker -->
  <rect x="130" y="30" width="700" height="450" rx="10" fill="none" stroke="#1e293b" stroke-width="1.5" stroke-dasharray="6 3"/>
  <text x="146" y="48" font-family="JetBrains Mono,monospace" font-size="10" fill="#334155">mdg_network</text>

  <!-- Label Docker Containers in basso a sinistra -->
  <text x="148" y="468" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#1e3a5f" letter-spacing="2">DOCKER CONTAINERS</text>

  <!-- ERP esterno sx -->
  <rect x="18" y="55" width="100" height="54" rx="8" fill="#0d1117" stroke="#374151" stroke-width="1.2" stroke-dasharray="4 3"/>
  <text x="68" y="78" font-family="JetBrains Mono,monospace" font-size="10" font-weight="600" fill="#6b7280" text-anchor="middle">ERP</text>
  <text x="68" y="94" font-family="JetBrains Mono,monospace" font-size="9" fill="#4b5563" text-anchor="middle">ZIP / XLSX</text>

  <!-- Browser Business user esterno dx —  vicino IT user -->
  <rect x="860" y="340" width="118" height="50" rx="8" fill="#0d1117" stroke="#374151" stroke-width="1.2" stroke-dasharray="4 3"/>
  <text x="919" y="361" font-family="JetBrains Mono,monospace" font-size="8.5" font-weight="600" fill="#6b7280" text-anchor="middle">Browser Web</text>
  <text x="919" y="377" font-family="JetBrains Mono,monospace" font-size="8" fill="#4b5563" text-anchor="middle">Business user</text>

  <!-- Browser IT user esterno dx basso -->
  <rect x="860" y="400" width="118" height="50" rx="8" fill="#0d1117" stroke="#374151" stroke-width="1.2" stroke-dasharray="4 3"/>
  <text x="919" y="421" font-family="JetBrains Mono,monospace" font-size="8.5" font-weight="600" fill="#6b7280" text-anchor="middle">Browser Web</text>
  <text x="919" y="437" font-family="JetBrains Mono,monospace" font-size="8" fill="#4b5563" text-anchor="middle">IT user</text>

  <!-- SFTP -->
  <rect x="150" y="55" width="130" height="72" rx="8" fill="#0c1a2e" stroke="#1e4976" stroke-width="1.5"/>
  <text x="215" y="78" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#93c5fd" text-anchor="middle">mdg_sftp</text>
  <text x="215" y="94" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">atmoz/sftp:alpine</text>
  <text x="215" y="110" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">:22</text>

  <!-- Bruin -->
  <rect x="150" y="210" width="130" height="80" rx="8" fill="#160b2e" stroke="#4c1d95" stroke-width="1.5"/>
  <text x="215" y="235" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#d8b4fe" text-anchor="middle">mdg_bruin</text>
  <text x="215" y="251" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">custom image</text>
  <text x="215" y="267" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">bruin run</text>

  <!-- Postgres -->
  <rect x="390" y="150" width="150" height="80" rx="8" fill="#051a10" stroke="#14532d" stroke-width="1.5"/>
  <text x="465" y="176" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#86efac" text-anchor="middle">mdg_postgres</text>
  <text x="465" y="192" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">postgres:18-alpine</text>
  <text x="465" y="208" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">raw · ref · stg  :5432</text>

  <!-- PgAdmin — centro basso -->
  <rect x="390" y="400" width="150" height="72" rx="8" fill="#0a1a10" stroke="#166534" stroke-width="1.5"/>
  <text x="465" y="422" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#4ade80" text-anchor="middle">mdg_pgadmin</text>
  <text x="465" y="438" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">dpage/pgadmin4</text>
  <text x="465" y="454" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">:8080</text>

  <!-- FastAPI -->
  <rect x="660" y="55" width="130" height="72" rx="8" fill="#1f0e03" stroke="#7c2d12" stroke-width="1.5"/>
  <text x="725" y="78" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#fdba74" text-anchor="middle">mdg_fastapi</text>
  <text x="725" y="94" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">python:3.12-slim</text>
  <text x="725" y="110" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">:8000</text>

  <!-- Streamlit -->
  <rect x="660" y="195" width="130" height="80" rx="8" fill="#0e1a0e" stroke="#15803d" stroke-width="1.5"/>
  <text x="725" y="220" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#86efac" text-anchor="middle">mdg_streamlit</text>
  <text x="725" y="236" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">python:3.12-slim</text>
  <text x="725" y="252" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">:8501</text>

  <!-- ERP -> SFTP -->
  <line x1="118" y1="82" x2="147" y2="82" stroke="#374151" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-dash)"/>

  <!-- SFTP -> Bruin -->
  <line x1="215" y1="127" x2="215" y2="207" stroke="#475569" stroke-width="1.2" marker-end="url(#arr)"/>
  <text x="222" y="172" font-family="JetBrains Mono,monospace" font-size="8" fill="#334155">volume</text>

  <!-- Bruin -> Postgres — parte dal basso del box Bruin per evitare la curva arancione -->
  <line x1="280" y1="270" x2="387" y2="225" stroke="#475569" stroke-width="1.2" marker-end="url(#arr)"/>
  <text x="298" y="248" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155">INSERT</text>

  <!-- PgAdmin -> Postgres -->
  <line x1="465" y1="400" x2="465" y2="233" stroke="#475569" stroke-width="1.2" marker-end="url(#arr)"/>
  <text x="471" y="325" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155">DQL-DML-DDL</text>
  <text x="471" y="336" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155">commands</text>

  <!-- Streamlit -> Postgres -->
  <line x1="660" y1="222" x2="543" y2="205" stroke="#475569" stroke-width="1.2" marker-end="url(#arr)"/>
  <text x="612" y="220" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155">SELECT</text>

  <!-- Streamlit -> FastAPI -->
  <line x1="725" y1="195" x2="725" y2="130" stroke="#475569" stroke-width="1.2" marker-end="url(#arr)"/>
  <text x="731" y="167" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155">REST</text>

  <!-- FastAPI -> Postgres log run -->
  <line x1="660" y1="95" x2="543" y2="158" stroke="#475569" stroke-width="1.2" marker-end="url(#arr)"/>
  <text x="563" y="115" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155">log run</text>

  <!-- FastAPI -> Bruin docker exec — curva alta, label SOTTO la curva vicino a FastAPI -->
  <path d="M 660 78 Q 465 22 280 230" fill="none" stroke="#f97316" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#arr-orange)"/>
  <text x="645" y="90" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#f97316" text-anchor="end">docker exec</text>

  <!-- Business user -> mdg_auth -->
  <line x1="860" y1="365" x2="793" y2="365" stroke="#374151" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-dash)"/>

  <!-- Auth API -->
  <rect x="660" y="330" width="130" height="72" rx="8" fill="#1a0e2e" stroke="#6d28d9" stroke-width="1.5"/>
  <text x="725" y="354" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#c4b5fd" text-anchor="middle">mdg_auth</text>
  <text x="725" y="370" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">python:3.12-slim</text>
  <text x="725" y="386" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">fastapi-users · JWT  :8001</text>

  <!-- mdg_auth -> Streamlit (JWT) -->
  <line x1="725" y1="327" x2="725" y2="278" stroke="#6d28d9" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-auth)"/>
  <text x="731" y="308" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#7c3aed">JWT</text>

  <!-- mdg_auth -> Postgres (schema usr) -->
  <line x1="660" y1="366" x2="543" y2="230" stroke="#6d28d9" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-auth)"/>
  <text x="577" y="315" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#7c3aed">usr schema</text>

  <!-- IT user -> mdg_auth -->
  <line x1="860" y1="425" x2="793" y2="380" stroke="#374151" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-dash)"/>

  <!-- IT user -> PgAdmin -->
  <line x1="860" y1="440" x2="543" y2="440" stroke="#374151" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-dash)"/>
  <text x="680" y="435" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155">:8080</text>



</svg>
"""

components.html(f"""
<!DOCTYPE html>
<html>
<head><style>body{{margin:0;padding:0;background:transparent;}}</style></head>
<body>{svg}</body>
</html>
""", height=550)

# ---------------------------------------------------------------------------
# Schede container
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Descrizione dei container</div>', unsafe_allow_html=True)

containers = [
    {
        "badge": "badge-sftp", "badge_label": "SFTP",
        "name": "mdg_sftp",
        "image": "atmoz/sftp:alpine  ·  porta 22",
        "desc": "Punto di ingresso dei file provenienti dall'ERP. Espone un server SFTP su cui vengono depositati i file ZIP (dati anagrafici) e XLSX (tabelle di controllo SAP). Il volume <code>datalake_olderp</code> è condiviso con Bruin per l'accesso diretto ai file.",
    },
    {
        "badge": "badge-etl", "badge_label": "PIPELINE",
        "name": "mdg_bruin",
        "image": "image custom (bruin CLI)  ·  nessuna porta esposta",
        "desc": "Cuore della pipeline. Esegue in sequenza: ingestione dei file nello schema <code>raw</code>, caricamento delle tabelle di riferimento SAP nello schema <code>ref</code>, numerosi controlli di correttezza formale e logica nello schema <code>stg</code>, e aggiornamento della tabella <code>stg.pipeline_runs</code> con i risultati del run.",
    },
    {
        "badge": "badge-db", "badge_label": "DATABASE",
        "name": "mdg_postgres",
        "image": "postgres:18-alpine  ·  porta 5432",
        "desc": "Database centrale del progetto. Ospita tre schemi: <code>raw</code> (dati ERP grezzi), <code>ref</code> (tabelle di controllo SAP), <code>stg</code> (risultati check, catalogo e storico run). È l'unico servizio con healthcheck attivo — tutti gli altri dipendono da esso.",
    },
    {
        "badge": "badge-db", "badge_label": "ADMIN DB",
        "name": "mdg_pgadmin",
        "image": "dpage/pgadmin4:latest  ·  porta 8080",
        "desc": "Interfaccia web per l'amministrazione di PostgreSQL. Permette al team IT di eseguire query SQL, ispezionare gli schemi e verificare i dati direttamente nel browser. Preconfigurato con la connessione al database MDG.",
    },
    {
        "badge": "badge-api", "badge_label": "API",
        "name": "mdg_fastapi",
        "image": "python:3.12-slim + docker CLI  ·  porta 8000",
        "desc": "API REST che funge da intermediario tra Streamlit e Bruin. Espone endpoint per avviare la pipeline (<code>POST /pipeline/run</code>), monitorarne lo stato, leggere i log e listare i file nella inbox SFTP. Monta il Docker socket per eseguire <code>docker exec</code> sul container Bruin. Registra ogni run nello schema <code>stg</code>.",
    },
    {
        "badge": "badge-ui", "badge_label": "DASHBOARD",
        "name": "mdg_streamlit",
        "image": "python:3.12-slim + streamlit  ·  porta 8501",
        "desc": "Dashboard interattiva per il team funzionale SAP (Business user). Mostra i risultati dei controlli di qualità, il catalogo degli asset, lo storico dei run e permette di avviare la pipeline direttamente dal browser tramite le API FastAPI. Legge i dati direttamente da PostgreSQL.",
    },
    {
        "badge": "badge-api", "badge_label": "AUTH",
        "name": "mdg_auth",
        "image": "python:3.12-slim + fastapi-users  ·  porta 8001",
        "desc": "Servizio di autenticazione e gestione utenti. Implementa login JWT tramite FastAPI-Users, con ruoli <code>admin</code> (IT user) e <code>user</code> (Business user). Gestisce le credenziali nello schema dedicato <code>usr</code> del database PostgreSQL. Streamlit verifica il token JWT ad ogni pagina.",
    },
]

import pandas as pd
cols = st.columns(3)
for i, c in enumerate(containers):
    with cols[i % 3]:
        st.markdown(f"""
        <div class="container-card">
            <div><span class="badge {c['badge']}">{c['badge_label']}</span></div>
            <div class="c-name">{c['name']}</div>
            <div class="c-image">{c['image']}</div>
            <div class="c-desc">{c['desc']}</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("")

st.markdown("<br>", unsafe_allow_html=True)
st.caption("MDG v0 · Sviluppo locale WSL2 · Deploy produzione: OCI VPS")
