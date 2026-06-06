"""
MDG — Migration Data Governance
POSIZIONE: mdg-v0/streamlit/app/pages/9_info.py

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

.mdg-title { font-family:inherit; font-size:2.4rem; font-weight:800; letter-spacing:-0.03em; margin-bottom:0; }
.mdg-subtitle { font-family:inherit; font-size:1rem; color:#6b7280; margin-top:0.2rem; margin-bottom:2rem; }
.section-title { font-family:inherit; font-size:1.1rem; font-weight:600; text-transform:uppercase; letter-spacing:0.12em; color:#9ca3af; margin-bottom:0.8rem; margin-top:2rem; }
.scope-box { background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%); border:1px solid #334155; border-left:4px solid #3b82f6; border-radius:8px; padding:1.4rem 1.8rem; font-family:inherit; font-size:0.95rem; line-height:1.8; color:#cbd5e1; margin-bottom:1rem; }
.scope-box b { color:#93c5fd; }
.container-card { background:#0f172a; border:1px solid #1e293b; border-radius:10px; padding:1rem 1.2rem; margin-top:0.5rem; }
.container-card .c-name { font-family:'JetBrains Mono',monospace; font-size:0.85rem; font-weight:600; color:#f8fafc; margin-bottom:0.2rem; }
.container-card .c-image { font-family:'JetBrains Mono',monospace; font-size:0.72rem; color:#64748b; margin-bottom:0.5rem; }
.container-card .c-desc { font-family:inherit; font-size:0.82rem; color:#94a3b8; line-height:1.5; }
.badge { display:inline-block; font-family:'JetBrains Mono',monospace; font-size:0.65rem; font-weight:600; padding:2px 8px; border-radius:4px; margin-bottom:6px; }
.badge-db   { background:#164e63; color:#67e8f9; }
.badge-ui   { background:#14532d; color:#86efac; }
.badge-etl  { background:#3b0764; color:#d8b4fe; }
.badge-api  { background:#7c2d12; color:#fdba74; }
.badge-sftp { background:#1e3a5f; color:#93c5fd; }
</style>
""", unsafe_allow_html=True)

st.markdown(
    '<h1 style="color:#38BDF8;">ℹ️ MDG — Migration Data Governance</h1>',
    unsafe_allow_html=True,
)
st.caption(":yellow[Documentazione tecnica · v0 · Sviluppo locale (WSL2 + Docker)]")
st.divider()

# ---------------------------------------------------------------------------
# Scopo
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Scopo dell'' applicazione</div>', unsafe_allow_html=True)

st.markdown("""
<div class="scope-box">
L'applicazione <b>MDG</b> supporta il processo di migrazione dati da un ERP legacy verso <b>SAP S/4HANA</b>,
garantendo la qualità e la coerenza dei dati prima del caricamento nel sistema di destinazione.<br><br>
Ad ogni ciclo, la pipeline:
<ul style="margin-top:0.5rem; padding-left:1.2rem;">
  <li>Acquisisce i file esportati dall'ERP (ZIP/CSV) tramite SFTP e li carica nello <b>schema raw</b> di PostgreSQL</li>
  <li>Carica le <b>tabelle di controllo SAP</b> (codici paese, regioni, banche, ecc.) nello <b>schema ref</b></li>
  <li>Esegue <b>numerosi controlli di correttezza formale e logica</b> su clienti, fornitori e dati anagrafici (P.IVA, codice fiscale, duplicati, coerenza geografica, ecc.)</li>
  <li>Consolida i risultati dei controlli nello <b>schema stg</b>, rendendoli disponibili alla dashboard Streamlit per l'analisi</li>
  <li>Registra ogni run nella tabella <b>stg.pipeline_runs</b> con contatori di check superati ed errori</li>
</ul>
L'obiettivo è consentire al team funzionale SAP di identificare e correggere le anomalie <b>prima</b> del go-live,
riducendo il rischio di dati inconsistenti nel sistema target.
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Diagramma architettura
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Architettura container</div>', unsafe_allow_html=True)

svg = """
<svg viewBox="0 0 1380 800" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:1380px;display:block;margin:0 auto;">
  <defs>
    <marker id="arr"        markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0,9 3.5,0 7" fill="#94a3b8"/></marker>
    <marker id="arr-orange" markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0,9 3.5,0 7" fill="#fb923c"/></marker>
    <marker id="arr-green"  markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0,9 3.5,0 7" fill="#4ade80"/></marker>
    <marker id="arr-purple" markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0,9 3.5,0 7" fill="#c084fc"/></marker>
  </defs>

  <rect width="1380" height="800" rx="14" fill="#060c18"/>

  <!-- mdg_network box -->
  <rect x="210" y="24" width="820" height="736" rx="12" fill="none" stroke="#334155" stroke-width="1.5" stroke-dasharray="7 4"/>
  <rect x="228" y="15" width="142" height="20" rx="4" fill="#060c18"/>
  <text x="299" y="30" font-family="JetBrains Mono,monospace" font-size="12" font-weight="600" fill="#475569" text-anchor="middle" letter-spacing="1">mdg_network</text>
  <text x="620" y="752" font-family="JetBrains Mono,monospace" font-size="10" fill="#1e293b" text-anchor="middle" letter-spacing="2">DOCKER CONTAINERS</text>

  <!-- ─── ERP esterno ─── -->
  <rect x="28" y="194" width="140" height="68" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="98" y="222" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">ERP Legacy</text>
  <text x="98" y="244" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">ZIP</text>

  <!-- ─── SAP S/4HANA (XLSX) ─── -->
  <rect x="28" y="294" width="140" height="68" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="98" y="322" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">SAP S/4HANA</text>
  <text x="98" y="344" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">XLSX</text>

  <!-- ─── mdg_sftp ─── -->
  <rect x="228" y="46" width="192" height="136" rx="9" fill="#0c1e38" stroke="#3b82f6" stroke-width="2.2"/>
  <text x="324" y="78" font-family="JetBrains Mono,monospace" font-size="14" font-weight="700" fill="#93c5fd" text-anchor="middle">mdg_sftp</text>
  <line x1="238" y1="88" x2="410" y2="88" stroke="#1d4ed8" stroke-width="1"/>
  <text x="324" y="108" font-family="JetBrains Mono,monospace" font-size="11" fill="#7dd3fc" text-anchor="middle">SFTPGo · SFTP :2022</text>
  <text x="324" y="126" font-family="JetBrains Mono,monospace" font-size="11" fill="#7dd3fc" text-anchor="middle">WebAdmin :8080</text>
  <text x="324" y="146" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">4 volumi datalake condivisi</text>
  <text x="324" y="164" font-family="JetBrains Mono,monospace" font-size="10" fill="#1e3a5f" text-anchor="middle">DATASET_READY.txt semaphore</text>

  <!-- ─── mdg_bruin ─── -->
  <rect x="228" y="314" width="192" height="128" rx="9" fill="#1a0b38" stroke="#7c3aed" stroke-width="2.2"/>
  <text x="324" y="346" font-family="JetBrains Mono,monospace" font-size="14" font-weight="700" fill="#c084fc" text-anchor="middle">mdg_bruin</text>
  <line x1="238" y1="356" x2="410" y2="356" stroke="#4c1d95" stroke-width="1"/>
  <text x="324" y="376" font-family="JetBrains Mono,monospace" font-size="11" fill="#a78bfa" text-anchor="middle">Pipeline ETL · Bruin CLI</text>
  <text x="324" y="396" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#a78bfa" text-anchor="middle">ingestion → checks → prd</text>
  <text x="324" y="416" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">bruin run /project/bruin</text>
  <text x="324" y="434" font-family="JetBrains Mono,monospace" font-size="10" fill="#4c1d95" text-anchor="middle">triggered via docker exec</text>

  <!-- ─── mdg_postgres ─── -->
  <rect x="488" y="150" width="228" height="204" rx="9" fill="#051a10" stroke="#22c55e" stroke-width="2.2"/>
  <text x="602" y="182" font-family="JetBrains Mono,monospace" font-size="14" font-weight="700" fill="#4ade80" text-anchor="middle">mdg_postgres</text>
  <line x1="498" y1="192" x2="706" y2="192" stroke="#16a34a" stroke-width="1"/>
  <text x="602" y="212" font-family="JetBrains Mono,monospace" font-size="11" fill="#86efac" text-anchor="middle">PostgreSQL 18-alpine  :5432</text>
  <rect x="500" y="222" width="66" height="20" rx="4" fill="#0d3321"/>
  <text x="533" y="236" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="700" fill="#4ade80" text-anchor="middle">raw</text>
  <rect x="574" y="222" width="56" height="20" rx="4" fill="#0d3321"/>
  <text x="602" y="236" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="700" fill="#4ade80" text-anchor="middle">ref</text>
  <rect x="638" y="222" width="56" height="20" rx="4" fill="#1a1060"/>
  <text x="666" y="236" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="700" fill="#c084fc" text-anchor="middle">stg</text>
  <rect x="500" y="248" width="56" height="20" rx="4" fill="#2a1a00"/>
  <text x="528" y="262" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="700" fill="#fbbf24" text-anchor="middle">prd</text>
  <rect x="564" y="248" width="56" height="20" rx="4" fill="#1a0a28"/>
  <text x="592" y="262" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="700" fill="#d8b4fe" text-anchor="middle">usr</text>
  <text x="602" y="294" font-family="JetBrains Mono,monospace" font-size="10" fill="#64748b" text-anchor="middle">healthcheck attivo</text>
  <text x="602" y="340" font-family="JetBrains Mono,monospace" font-size="10" fill="#1e293b" text-anchor="middle">max_connections 300</text>

  <!-- ─── mdg_pgadmin ─── -->
  <rect x="488" y="548" width="228" height="110" rx="9" fill="#0a1a10" stroke="#16a34a" stroke-width="2.2"/>
  <text x="602" y="580" font-family="JetBrains Mono,monospace" font-size="14" font-weight="700" fill="#4ade80" text-anchor="middle">mdg_pgadmin</text>
  <line x1="498" y1="590" x2="706" y2="590" stroke="#14532d" stroke-width="1"/>
  <text x="602" y="612" font-family="JetBrains Mono,monospace" font-size="11" fill="#86efac" text-anchor="middle">PgAdmin 4  :PGADMIN_PORT</text>
  <text x="602" y="634" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">SQL · ispezione schema · it_role</text>

  <!-- ─── mdg_fastapi ─── -->
  <rect x="782" y="46" width="212" height="148" rx="9" fill="#1f0e03" stroke="#f97316" stroke-width="2.2"/>
  <text x="888" y="78" font-family="JetBrains Mono,monospace" font-size="14" font-weight="700" fill="#fb923c" text-anchor="middle">mdg_fastapi</text>
  <line x1="792" y1="88" x2="984" y2="88" stroke="#7c2d12" stroke-width="1"/>
  <text x="888" y="108" font-family="JetBrains Mono,monospace" font-size="11" fill="#fdba74" text-anchor="middle">Pipeline Controller  :8000</text>
  <text x="888" y="128" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">docker.sock · datalake vols</text>
  <text x="888" y="148" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">DATASET_READY.txt trigger</text>
  <text x="888" y="168" font-family="JetBrains Mono,monospace" font-size="10" fill="#7c2d12" text-anchor="middle">POST /pipeline/run · GET /logs</text>

  <!-- ─── mdg_streamlit ─── -->
  <rect x="782" y="256" width="212" height="140" rx="9" fill="#0e1a0e" stroke="#22c55e" stroke-width="2.2"/>
  <text x="888" y="288" font-family="JetBrains Mono,monospace" font-size="14" font-weight="700" fill="#4ade80" text-anchor="middle">mdg_streamlit</text>
  <line x1="792" y1="298" x2="984" y2="298" stroke="#15803d" stroke-width="1"/>
  <text x="888" y="318" font-family="JetBrains Mono,monospace" font-size="11" fill="#86efac" text-anchor="middle">Dashboard MDG  :8501</text>
  <text x="888" y="338" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">check results · pipeline admin</text>
  <text x="888" y="358" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">RBAC server-side · 10 pagine</text>
  <text x="888" y="378" font-family="JetBrains Mono,monospace" font-size="10" fill="#15803d" text-anchor="middle">direct DB + REST API + JWT auth</text>

  <!-- ─── mdg_auth ─── -->
  <rect x="782" y="466" width="212" height="152" rx="9" fill="#1a0a1a" stroke="#9333ea" stroke-width="2.2"/>
  <text x="888" y="498" font-family="JetBrains Mono,monospace" font-size="14" font-weight="700" fill="#d8b4fe" text-anchor="middle">mdg_auth</text>
  <line x1="792" y1="508" x2="984" y2="508" stroke="#6b21a8" stroke-width="1"/>
  <text x="888" y="528" font-family="JetBrains Mono,monospace" font-size="11" fill="#c4b5fd" text-anchor="middle">FastAPI-Users + JWT  :8001</text>
  <rect x="795" y="540" width="96" height="18" rx="4" fill="#2d0a4a"/>
  <text x="843" y="553" font-family="JetBrains Mono,monospace" font-size="10" font-weight="700" fill="#e879f9" text-anchor="middle">admin_role</text>
  <rect x="899" y="540" width="88" height="18" rx="4" fill="#1a0a30"/>
  <text x="943" y="553" font-family="JetBrains Mono,monospace" font-size="10" font-weight="700" fill="#a78bfa" text-anchor="middle">it_role</text>
  <rect x="795" y="564" width="130" height="18" rx="4" fill="#0a1a2a"/>
  <text x="860" y="577" font-family="JetBrains Mono,monospace" font-size="10" font-weight="700" fill="#7dd3fc" text-anchor="middle">business_role</text>
  <text x="888" y="600" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#475569" text-anchor="middle">JWT lifetime 8h · schema usr</text>

  <!-- ─── Browser Admin ─── -->
  <rect x="1068" y="136" width="162" height="74" rx="8" fill="#0f172a" stroke="#e879f9" stroke-width="1.8" stroke-dasharray="5 3"/>
  <text x="1149" y="164" font-family="JetBrains Mono,monospace" font-size="12" font-weight="700" fill="#e879f9" text-anchor="middle">Browser Admin</text>
  <text x="1149" y="184" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#64748b" text-anchor="middle">admin_role · full access</text>
  <text x="1149" y="200" font-family="JetBrains Mono,monospace" font-size="9" fill="#475569" text-anchor="middle">utenti · pipeline · dati</text>

  <!-- ─── Browser IT ─── -->
  <rect x="1068" y="314" width="162" height="74" rx="8" fill="#0f172a" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="1149" y="342" font-family="JetBrains Mono,monospace" font-size="12" font-weight="700" fill="#94a3b8" text-anchor="middle">Browser IT</text>
  <text x="1149" y="362" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#64748b" text-anchor="middle">it_role · Streamlit</text>
  <text x="1149" y="378" font-family="JetBrains Mono,monospace" font-size="9" fill="#475569" text-anchor="middle">+ PgAdmin diretto</text>

  <!-- ─── Browser Business ─── -->
  <rect x="1068" y="492" width="162" height="74" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="1149" y="520" font-family="JetBrains Mono,monospace" font-size="12" font-weight="700" fill="#94a3b8" text-anchor="middle">Browser Business</text>
  <text x="1149" y="540" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#64748b" text-anchor="middle">business_role</text>
  <text x="1149" y="556" font-family="JetBrains Mono,monospace" font-size="9" fill="#475569" text-anchor="middle">Streamlit (read-only)</text>

  <!-- ═══════════════ FRECCE ═══════════════ -->

  <!-- ERP → mdg_sftp -->
  <line x1="168" y1="212" x2="226" y2="112" stroke="#64748b" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#arr)"/>

  <!-- SAP → mdg_sftp -->
  <line x1="168" y1="328" x2="226" y2="158" stroke="#64748b" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#arr)"/>

  <!-- mdg_sftp → mdg_bruin (datalake volumes) -->
  <line x1="324" y1="182" x2="324" y2="312" stroke="#c084fc" stroke-width="1.8" marker-end="url(#arr-purple)"/>
  <text x="350" y="242" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#7c3aed" text-anchor="start">datalake</text>
  <text x="350" y="256" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#7c3aed" text-anchor="start">volumes</text>

  <!-- mdg_bruin → mdg_postgres (INSERT raw/ref/stg/prd) -->
  <line x1="420" y1="366" x2="486" y2="272" stroke="#4ade80" stroke-width="1.8" marker-end="url(#arr-green)"/>
  <text x="461" y="306" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="600" fill="#4ade80" text-anchor="middle">INSERT</text>
  <text x="461" y="320" font-family="JetBrains Mono,monospace" font-size="10" fill="#16a34a" text-anchor="middle">raw/ref/stg/prd</text>

  <!-- mdg_fastapi → mdg_bruin (docker exec bruin run, curva) -->
  <path d="M 782 118 Q 600 20 420 342" fill="none" stroke="#fb923c" stroke-width="1.8" stroke-dasharray="6 3" marker-end="url(#arr-orange)"/>
  <text x="606" y="32" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#fb923c" text-anchor="middle">docker exec bruin run</text>

  <!-- mdg_fastapi → mdg_postgres (stg.pipeline_runs) -->
  <line x1="782" y1="130" x2="718" y2="228" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
  <text x="766" y="188" font-family="JetBrains Mono,monospace" font-size="10" fill="#64748b" text-anchor="middle">stg.pipeline_runs</text>

  <!-- mdg_streamlit → mdg_postgres (SELECT) -->
  <line x1="782" y1="316" x2="718" y2="298" stroke="#4ade80" stroke-width="1.8" marker-end="url(#arr-green)"/>
  <text x="754" y="292" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="600" fill="#4ade80" text-anchor="middle">SELECT</text>

  <!-- mdg_streamlit → mdg_fastapi (REST API) -->
  <line x1="888" y1="256" x2="888" y2="194" stroke="#fb923c" stroke-width="1.8" marker-end="url(#arr-orange)"/>
  <text x="916" y="230" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="600" fill="#fb923c" text-anchor="start">REST API</text>

  <!-- mdg_streamlit → mdg_auth (login / JWT) -->
  <line x1="888" y1="396" x2="888" y2="464" stroke="#c084fc" stroke-width="1.8" stroke-dasharray="4 2" marker-end="url(#arr-purple)"/>
  <text x="916" y="436" font-family="JetBrains Mono,monospace" font-size="10.5" font-weight="600" fill="#c084fc" text-anchor="start">login/JWT</text>

  <!-- mdg_auth → mdg_postgres (usr schema) -->
  <line x1="782" y1="522" x2="718" y2="346" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
  <text x="748" y="444" font-family="JetBrains Mono,monospace" font-size="10" fill="#64748b" text-anchor="middle">usr schema</text>

  <!-- mdg_pgadmin → mdg_postgres -->
  <line x1="602" y1="548" x2="602" y2="354" stroke="#4ade80" stroke-width="1.8" marker-end="url(#arr-green)"/>

  <!-- Browser Admin → mdg_streamlit (:8501) -->
  <line x1="1066" y1="172" x2="994" y2="306" stroke="#e879f9" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#arr)"/>

  <!-- Browser IT → mdg_streamlit -->
  <line x1="1066" y1="348" x2="994" y2="336" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#arr)"/>

  <!-- Browser Business → mdg_streamlit -->
  <line x1="1066" y1="520" x2="994" y2="374" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#arr)"/>

  <!-- HTTP :8501 label -->
  <text x="1052" y="282" font-family="JetBrains Mono,monospace" font-size="10" fill="#475569" text-anchor="end">HTTP :8501</text>

  <!-- Browser IT → mdg_pgadmin (:PGADMIN_PORT) -->
  <line x1="1066" y1="372" x2="718" y2="608" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#arr)"/>
  <text x="920" y="530" font-family="JetBrains Mono,monospace" font-size="10" fill="#475569" text-anchor="middle">:PGADMIN_PORT</text>

</svg>
"""

components.html(f"""
<!DOCTYPE html>
<html>
<head><style>body{{margin:0;padding:0;background:transparent;}}</style></head>
<body>{svg}</body>
</html>
""", height=800)


# ---------------------------------------------------------------------------
# Schema pipeline (flusso dati tra schemi DB)
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Schema pipeline — flusso dati</div>', unsafe_allow_html=True)

pipeline_svg = """
<svg viewBox="0 0 1440 540" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:1440px;display:block;margin:0 auto 1.5rem;">
  <defs>
    <marker id="parr"        markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 9 3.5, 0 7" fill="#94a3b8"/></marker>
    <marker id="parr-blue"   markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 9 3.5, 0 7" fill="#60a5fa"/></marker>
    <marker id="parr-green"  markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 9 3.5, 0 7" fill="#4ade80"/></marker>
    <marker id="parr-purple" markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 9 3.5, 0 7" fill="#c084fc"/></marker>
    <marker id="parr-amber"  markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 9 3.5, 0 7" fill="#fbbf24"/></marker>
    <marker id="parr-teal"   markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 9 3.5, 0 7" fill="#2dd4bf"/></marker>
    <marker id="parr-red"    markerWidth="9" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 9 3.5, 0 7" fill="#f87171"/></marker>
  </defs>

  <rect width="1440" height="540" rx="14" fill="#060c18"/>

  <!-- -- SORGENTI (x=20) -- -->
  <rect x="20" y="80"  width="120" height="58" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="80" y="105" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">ERP Legacy</text>
  <text x="80" y="127" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">ZIP / CSV</text>

  <rect x="20" y="162" width="120" height="58" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="80" y="187" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">SAP S/4HANA</text>
  <text x="80" y="209" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">XLSX ref tables</text>

  <rect x="20" y="244" width="120" height="58" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="80" y="269" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">Altre sorgenti</text>
  <text x="80" y="291" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">XLSX</text>

  <!-- -- SFTP INPUT (x=240, gap=100 dalle sorgenti) -- -->
  <rect x="240" y="118" width="150" height="126" rx="8" fill="#0c1e38" stroke="#3b82f6" stroke-width="2.2"/>
  <text x="315" y="150" font-family="JetBrains Mono,monospace" font-size="15" font-weight="700" fill="#93c5fd" text-anchor="middle">SFTPGo</text>
  <line x1="248" y1="162" x2="382" y2="162" stroke="#1d4ed8" stroke-width="1"/>
  <text x="315" y="182" font-family="JetBrains Mono,monospace" font-size="11.5" fill="#7dd3fc" text-anchor="middle">in_source_pprod</text>
  <text x="315" y="200" font-family="JetBrains Mono,monospace" font-size="11.5" fill="#7dd3fc" text-anchor="middle">in_source_sap</text>
  <text x="315" y="218" font-family="JetBrains Mono,monospace" font-size="11.5" fill="#7dd3fc" text-anchor="middle">in_source_others</text>

  <!-- frecce sorgenti → SFTPGo (gap=100px, ben visibili) -->
  <line x1="140" y1="109" x2="238" y2="148" stroke="#64748b" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#parr)"/>
  <line x1="140" y1="191" x2="238" y2="191" stroke="#64748b" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#parr)"/>
  <line x1="140" y1="273" x2="238" y2="232" stroke="#64748b" stroke-width="1.5" stroke-dasharray="5 3" marker-end="url(#parr)"/>

  <!-- -- RIQUADRO DB PostgreSQL (x=470, copre RAW+STG+PRD) -- -->
  <rect x="468" y="14" width="732" height="428" rx="12" fill="none" stroke="#f59e0b" stroke-width="1.8" stroke-dasharray="8 4"/>
  <rect x="490" y="6" width="168" height="20" rx="4" fill="#060c18"/>
  <text x="574" y="21" font-family="JetBrains Mono,monospace" font-size="12" font-weight="700" fill="#f59e0b" text-anchor="middle" letter-spacing="1">DB PostgreSQL</text>

  <!-- -- SCHEMA RAW (x=490) -- -->
  <rect x="490" y="26" width="180" height="196" rx="9" fill="#0d1f3c" stroke="#3b82f6" stroke-width="2.2"/>
  <text x="580" y="52"  font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#60a5fa" text-anchor="middle" letter-spacing="2">SCHEMA RAW</text>
  <line x1="500" y1="62" x2="660" y2="62" stroke="#1d4ed8" stroke-width="1"/>
  <text x="580" y="82"  font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_CUST_GEN#ZBP-*</text>
  <text x="580" y="102" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_SUPPL_GEN#ZBP-*</text>
  <text x="580" y="122" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_CUST/SUPPL_TAXNUMBERS</text>
  <text x="580" y="142" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_MARA · S_MARC</text>
  <text x="580" y="162" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_MBEW · S_MVKE</text>
  <text x="580" y="202" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#64748b" text-anchor="middle">dati grezzi ERP</text>

  <!-- -- SCHEMA REF (x=490, y=244) -- -->
  <rect x="490" y="244" width="180" height="178" rx="9" fill="#0a2010" stroke="#22c55e" stroke-width="2.2"/>
  <text x="580" y="272" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#4ade80" text-anchor="middle" letter-spacing="2">SCHEMA REF</text>
  <line x1="500" y1="282" x2="660" y2="282" stroke="#16a34a" stroke-width="1"/>
  <text x="580" y="306" font-family="JetBrains Mono,monospace" font-size="12" fill="#86efac" text-anchor="middle">SAP_EXPORT_T005S</text>
  <text x="580" y="328" font-family="JetBrains Mono,monospace" font-size="12" fill="#86efac" text-anchor="middle">SAP_EXPORT_T134/T024/T025</text>
  <text x="580" y="396" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#64748b" text-anchor="middle">tabelle controllo SAP</text>

  <!-- frecce SFTPGo → RAW / REF -->
  <line x1="390" y1="158" x2="488" y2="120" stroke="#60a5fa" stroke-width="2" marker-end="url(#parr-blue)"/>
  <text x="437" y="118" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#60a5fa" text-anchor="middle">ingestion</text>
  <line x1="390" y1="202" x2="488" y2="296" stroke="#4ade80" stroke-width="2" marker-end="url(#parr-green)"/>
  <text x="432" y="278" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#4ade80" text-anchor="middle">ingestion</text>

  <!-- -- SCHEMA STG (x=730) -- -->
  <rect x="730" y="26" width="210" height="386" rx="9" fill="#1a0b38" stroke="#9333ea" stroke-width="2.2"/>
  <text x="835" y="54"  font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#c084fc" text-anchor="middle" letter-spacing="2">SCHEMA STG</text>
  <line x1="740" y1="64" x2="930" y2="64" stroke="#6b21a8" stroke-width="1"/>
  <rect x="742" y="72"  width="186" height="28" rx="5" fill="#2d1060"/>
  <text x="835" y="91"  font-family="JetBrains Mono,monospace" font-size="11.5" font-weight="700" fill="#a78bfa" text-anchor="middle">SAP_REF  ·  CK001–CK099</text>
  <rect x="742" y="104" width="186" height="28" rx="5" fill="#2d1060"/>
  <text x="835" y="123" font-family="JetBrains Mono,monospace" font-size="11.5" font-weight="700" fill="#a78bfa" text-anchor="middle">EXISTENCE  ·  CK201–CK299</text>
  <rect x="742" y="136" width="186" height="28" rx="5" fill="#2d1060"/>
  <text x="835" y="155" font-family="JetBrains Mono,monospace" font-size="11.5" font-weight="700" fill="#a78bfa" text-anchor="middle">CROSS_TABLE  ·  CK401–CK499</text>
  <rect x="742" y="168" width="186" height="28" rx="5" fill="#3b0f78"/>
  <text x="835" y="187" font-family="JetBrains Mono,monospace" font-size="11.5" font-weight="700" fill="#e879f9" text-anchor="middle">EXT_REF  ·  CK801–CK899</text>
  <line x1="740" y1="204" x2="930" y2="204" stroke="#6b21a8" stroke-width="1"/>
  <text x="835" y="226" font-family="JetBrains Mono,monospace" font-size="12" fill="#c4b5fd" text-anchor="middle">check_results</text>
  <text x="835" y="249" font-family="JetBrains Mono,monospace" font-size="12" fill="#c4b5fd" text-anchor="middle">check_catalog</text>
  <text x="835" y="272" font-family="JetBrains Mono,monospace" font-size="12" fill="#c4b5fd" text-anchor="middle">check_vat_vies</text>
  <text x="835" y="295" font-family="JetBrains Mono,monospace" font-size="12" fill="#c4b5fd" text-anchor="middle">pipeline_runs</text>
  <text x="835" y="318" font-family="JetBrains Mono,monospace" font-size="12" fill="#c4b5fd" text-anchor="middle">S_CUST/SUPPL_GEN_STG</text>
  <text x="835" y="390" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#94a3b8" text-anchor="middle">quality checks + risultati + staging</text>

  <!-- frecce RAW/REF → STG -->
  <line x1="670" y1="120" x2="728" y2="150" stroke="#c084fc" stroke-width="2" marker-end="url(#parr-purple)"/>
  <line x1="670" y1="320" x2="728" y2="264" stroke="#c084fc" stroke-width="2" marker-end="url(#parr-purple)"/>
  <text x="696" y="198" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#c084fc" text-anchor="middle">checks</text>

  <!-- -- SCHEMA PRD (x=1000) -- -->
  <rect x="1000" y="90" width="178" height="234" rx="9" fill="#191400" stroke="#facc15" stroke-width="2.2"/>
  <text x="1089" y="120" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#fde047" text-anchor="middle" letter-spacing="2">SCHEMA PRD</text>
  <line x1="1010" y1="132" x2="1168" y2="132" stroke="#ca8a04" stroke-width="1"/>
  <text x="1089" y="158" font-family="JetBrains Mono,monospace" font-size="12" fill="#fef08a" text-anchor="middle">dati clienti validati</text>
  <text x="1089" y="181" font-family="JetBrains Mono,monospace" font-size="12" fill="#fef08a" text-anchor="middle">dati fornitori validati</text>
  <text x="1089" y="204" font-family="JetBrains Mono,monospace" font-size="12" fill="#fef08a" text-anchor="middle">dati materiali validati</text>
  <text x="1089" y="300" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#94a3b8" text-anchor="middle">output pronto per SAP</text>

  <!-- STG → PRD -->
  <line x1="940" y1="207" x2="998" y2="207" stroke="#fbbf24" stroke-width="2.2" marker-end="url(#parr-amber)"/>
  <text x="969" y="197" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#fbbf24" text-anchor="middle">promote</text>

  <!-- -- SFTP OUTPUT (x=1260) -- -->
  <rect x="1260" y="130" width="150" height="78" rx="8" fill="#0c1e38" stroke="#2dd4bf" stroke-width="2.2"/>
  <text x="1335" y="158" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#93c5fd" text-anchor="middle">SFTPGo</text>
  <line x1="1268" y1="168" x2="1402" y2="168" stroke="#0e7490" stroke-width="1"/>
  <text x="1335" y="192" font-family="JetBrains Mono,monospace" font-size="11.5" fill="#2dd4bf" font-weight="600" text-anchor="middle">out_source_mdg</text>

  <!-- PRD → SFTPGo output -->
  <line x1="1178" y1="190" x2="1258" y2="176" stroke="#2dd4bf" stroke-width="2.2" marker-end="url(#parr-teal)"/>
  <text x="1218" y="208" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#2dd4bf" text-anchor="middle">ZIP normalizzati</text>

  <!-- -- SAP S/4HANA (x=1260, y=340) -- -->
  <rect x="1260" y="340" width="150" height="78" rx="8" fill="#0f172a" stroke="#f87171" stroke-width="2" stroke-dasharray="5 3"/>
  <text x="1335" y="372" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#fca5a5" text-anchor="middle">SAP S/4HANA</text>
  <text x="1335" y="396" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">target</text>

  <!-- SFTPGo output → SAP -->
  <line x1="1335" y1="208" x2="1335" y2="338" stroke="#f87171" stroke-width="2.2" marker-end="url(#parr-red)"/>
  <text x="1368" y="278" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#f87171" text-anchor="start">load</text>

  <!-- Bruin banner -->
  <rect x="240" y="466" width="958" height="38" rx="8" fill="#150830" stroke="#7c3aed" stroke-width="1.5" stroke-dasharray="6 3"/>
  <text x="719" y="491" font-family="JetBrains Mono,monospace" font-size="11.5" font-weight="600" fill="#a78bfa" text-anchor="middle" letter-spacing="0.5">Bruin CLI — orchestrazione: ingestion → ref load → quality checks → promotion</text>
  <line x1="580" y1="466" x2="580" y2="424" stroke="#7c3aed" stroke-width="1" stroke-dasharray="4 2" marker-end="url(#parr-purple)"/>
  <line x1="835" y1="466" x2="835" y2="414" stroke="#7c3aed" stroke-width="1" stroke-dasharray="4 2" marker-end="url(#parr-purple)"/>
  <line x1="1089" y1="466" x2="1089" y2="326" stroke="#7c3aed" stroke-width="1" stroke-dasharray="4 2" marker-end="url(#parr-purple)"/>

</svg>
"""

st.markdown(pipeline_svg, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Schede container
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Descrizione dei container</div>', unsafe_allow_html=True)

containers = [
    {
        "badge": "badge-sftp", "badge_label": "SFTP",
        "name": "mdg_sftp",
        "image": "drakkan/sftpgo:latest",
        "desc": "Punto di ingresso dei file provenienti dall'ERP. Espone un server SFTP (<b>porta 2022</b>) su cui vengono depositati i file ZIP e XLSX. Utilizza <b>SFTPGo</b>: WebAdmin su <b>porta 8080</b> per gestione utenti, audit log e chiavi SSH senza riavvio del container. I quattro volumi datalake sono condivisi con Bruin e FastAPI. Il file semaforo <b>DATASET_READY.txt</b> segnala al controller la disponibilità dei dati per l'avvio della pipeline.",
    },
    {
        "badge": "badge-etl", "badge_label": "PIPELINE",
        "name": "mdg_bruin",
        "image": "image custom (bruin CLI)",
        "desc": "Cuore della pipeline. Esegue in sequenza: ingestione dei file nello schema <code>raw</code>, caricamento delle tabelle di riferimento SAP nello schema <code>ref</code>, numerosi controlli di correttezza formale e logica nello schema <code>stg</code>, e aggiornamento della tabella <code>stg.pipeline_runs</code> con i risultati del run.",
    },
    {
        "badge": "badge-db", "badge_label": "DATABASE",
        "name": "mdg_postgres",
        "image": "postgres:18-alpine",
        "desc": "Database centrale del progetto. Ospita cinque schemi: <code>raw</code> (dati ERP grezzi), <code>ref</code> (tabelle di controllo SAP), <code>stg</code> (risultati check, catalogo, storico run), <code>prd</code> (dati validati pronti per SAP) e <code>usr</code> (utenti e credenziali RBAC). È l'unico servizio con healthcheck attivo — tutti gli altri dipendono da esso.",
    },
    {
        "badge": "badge-db", "badge_label": "ADMIN DB",
        "name": "mdg_pgadmin",
        "image": "dpage/pgadmin4:latest",
        "desc": "Interfaccia web per l'amministrazione di PostgreSQL. Permette al team IT di eseguire query SQL, ispezionare gli schemi e verificare i dati direttamente nel browser. Preconfigurato con la connessione al database MDG.",
    },
    {
        "badge": "badge-api", "badge_label": "API",
        "name": "mdg_fastapi",
        "image": "python:3.12-slim + docker CLI",
        "desc": "API REST che funge da intermediario tra Streamlit e Bruin. Espone endpoint per avviare la pipeline (<code>POST /pipeline/run</code>), monitorarne lo stato, leggere i log e listare i file nella inbox SFTP. Monta il Docker socket per eseguire <code>docker exec</code> sul container Bruin. Registra ogni run nello schema <code>stg</code>.",
    },
    {
        "badge": "badge-ui", "badge_label": "DASHBOARD",
        "name": "mdg_streamlit",
        "image": "python:3.12-slim + streamlit",
        "desc": "Dashboard interattiva per il team funzionale SAP (Business user). Mostra i risultati dei controlli di qualità, il catalogo degli asset, lo storico dei run e permette di avviare la pipeline direttamente dal browser tramite le API FastAPI. Legge i dati direttamente da PostgreSQL.",
    },
    {
        "badge": "badge-api", "badge_label": "AUTH",
        "name": "mdg_auth",
        "image": "python:3.12-slim + fastapi-users",
        "desc": "Servizio di autenticazione e gestione utenti. Implementa login JWT tramite FastAPI-Users con tre ruoli RBAC: <code>admin_role</code> (accesso completo), <code>it_role</code> (Streamlit + PgAdmin), <code>business_role</code> (solo Streamlit read-only). Gestisce le credenziali nello schema <code>usr</code>. Streamlit verifica il token JWT ad ogni navigazione di pagina e adatta l'interfaccia al ruolo dell'utente.",
    },
]

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


# ---------------------------------------------------------------------------
# Framework versions
# ---------------------------------------------------------------------------
import os
import sys
import requests
import importlib.metadata

st.markdown('<div class="section-title">Framework versions</div>', unsafe_allow_html=True)

# CSS aggiuntivo per la sezione versioni
st.markdown("""
<style>
.fw-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:0.8rem; margin-bottom:1.5rem; }
.fw-card { background:#0f172a; border:1px solid #1e293b; border-radius:10px; padding:1rem 1.2rem; }
.fw-card .fw-label { font-family:inherit; font-size:0.72rem; font-weight:600; text-transform:uppercase; letter-spacing:0.1em; color:#475569; margin-bottom:0.5rem; }
.fw-card .fw-name  { font-family:'JetBrains Mono',monospace; font-size:0.85rem; font-weight:600; color:#f8fafc; margin-bottom:0.2rem; }
.fw-card .fw-ver   { font-family:'JetBrains Mono',monospace; font-size:0.75rem; color:#3b82f6; }
.fw-card .fw-sub   { font-family:'JetBrains Mono',monospace; font-size:0.68rem; color:#475569; margin-top:0.3rem; line-height:1.6; }
</style>
""", unsafe_allow_html=True)

def _get_version(package: str) -> str:
    """Legge la versione di un package Python installato."""
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return "n/a"

def _get_docker_version() -> str:
    """Chiama FastAPI (che ha accesso al Docker socket) per leggere la versione Docker."""
    try:
        fastapi_url = os.getenv("FASTAPI_URL", "http://mdg_fastapi:8000")
        r = requests.get(f"{fastapi_url}/system/docker-version", timeout=3)
        if r.status_code == 200:
            return r.json().get("version", "n/a")
        return "n/a"
    except Exception:
        return "n/a"

# Versioni Python e librerie (dinamiche — lette a runtime)
py_version    = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
st_version    = _get_version("streamlit")
fa_version    = _get_version("fastapi")
pd_version    = _get_version("pandas")
psy_version   = _get_version("psycopg2-binary") or _get_version("psycopg2")
req_version   = _get_version("requests")
sql_version   = _get_version("sqlalchemy")

# Versione Docker (dinamica — via FastAPI)
docker_version = _get_docker_version()

# Versioni infrastruttura (statiche — da docker-compose / immagini usate)
fw_cards = [
    {
        "label": "SFTP",
        "name":  "SFTPGo",
        "ver":   "2.7.1",
        "sub":   "server SFTP · WebAdmin · REST API",
    },
    {
        "label": "UX / Dashboard",
        "name":  "Streamlit",
        "ver":   st_version,
        "sub":   "dashboard interattiva",
    },
    {
        "label": "Database",
        "name":  "PostgreSQL",
        "ver":   "18-alpine",
        "sub":   "database centrale",
    },
    {
        "label": "Database Admin",
        "name":  "PgAdmin 4",
        "ver":   "latest",
        "sub":   "interfaccia web DB",
    },
    {
        "label": "Pipeline",
        "name":  "Bruin CLI",
        "ver":   "custom image",
        "sub":   "orchestratore ETL",
    },
    {
        "label": "Autenticazione",
        "name":  "FastAPI + fastapi-users",
        "ver":   fa_version,
        "sub":   "JWT · autenticazione",
    },
    {
        "label": "Environment",
        "name":  "Docker Engine",
        "ver":   docker_version,
        "sub":   "container runtime",
    },
    {
        "label": "Processing Engine",
        "name":  "Python",
        "ver":   py_version,
        "sub":   (
            f"pandas {pd_version}  ·  psycopg2 {psy_version}<br>"
            f"requests {req_version}  ·  sqlalchemy {sql_version}"
        ),
    },
]

# Render griglia
html_cards = ''
for c in fw_cards:
    html_cards += f"""
    <div class="fw-card">
        <div class="fw-label">{c["label"]}</div>
        <div class="fw-name">{c["name"]}</div>
        <div class="fw-ver">v {c["ver"]}</div>
        <div class="fw-sub">{c["sub"]}</div>
    </div>"""

st.markdown(f'<div class="fw-grid">{html_cards}</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.caption("MDG v0 · Sviluppo locale WSL2 · Deploy produzione: OCI VPS")

