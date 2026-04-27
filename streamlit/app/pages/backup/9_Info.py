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
  <text x="215" y="94" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">drakkan/sftpgo:latest</text>
  <text x="215" y="110" font-family="JetBrains Mono,monospace" font-size="7.5" fill="#334155" text-anchor="middle">SFTP :22 · WebAdmin :8082</text>
  

  <!-- Bruin -->
  <rect x="150" y="210" width="130" height="80" rx="8" fill="#160b2e" stroke="#4c1d95" stroke-width="1.5"/>
  <text x="215" y="235" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#d8b4fe" text-anchor="middle">mdg_bruin</text>
  <text x="215" y="251" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">custom image</text>
  <text x="215" y="267" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">bruin run</text>

  <!-- Postgres -->
  <rect x="390" y="150" width="150" height="80" rx="8" fill="#051a10" stroke="#14532d" stroke-width="1.5"/>
  <text x="465" y="176" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#86efac" text-anchor="middle">mdg_postgres</text>
  <text x="465" y="192" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">postgres:18-alpine</text>
  <text x="465" y="208" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">raw · ref · stg · prd</text>

  <!-- PgAdmin — centro basso -->
  <rect x="390" y="400" width="150" height="72" rx="8" fill="#0a1a10" stroke="#166534" stroke-width="1.5"/>
  <text x="465" y="422" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#4ade80" text-anchor="middle">mdg_pgadmin</text>
  <text x="465" y="438" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">dpage/pgadmin4</text>
  

  <!-- FastAPI -->
  <rect x="660" y="55" width="130" height="72" rx="8" fill="#1f0e03" stroke="#7c2d12" stroke-width="1.5"/>
  <text x="725" y="78" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#fdba74" text-anchor="middle">mdg_fastapi</text>
  <text x="725" y="94" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">python:3.12-slim</text>
  

  <!-- Streamlit -->
  <rect x="660" y="195" width="130" height="80" rx="8" fill="#0e1a0e" stroke="#15803d" stroke-width="1.5"/>
  <text x="725" y="220" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#86efac" text-anchor="middle">mdg_streamlit</text>
  <text x="725" y="236" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#475569" text-anchor="middle">python:3.12-slim</text>
  

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
  <text x="725" y="386" font-family="JetBrains Mono,monospace" font-size="8.5" fill="#64748b" text-anchor="middle">fastapi-users · JWT</text>

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

  <!-- ── SORGENTI (x=20) ── -->
  <rect x="20" y="80"  width="120" height="58" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="80" y="105" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">ERP Legacy</text>
  <text x="80" y="127" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">ZIP / CSV</text>

  <rect x="20" y="162" width="120" height="58" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="80" y="187" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">SAP S/4HANA</text>
  <text x="80" y="209" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">XLSX ref tables</text>

  <rect x="20" y="244" width="120" height="58" rx="8" fill="#0f172a" stroke="#475569" stroke-width="1.5" stroke-dasharray="5 3"/>
  <text x="80" y="269" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#94a3b8" text-anchor="middle">Altre sorgenti</text>
  <text x="80" y="291" font-family="JetBrains Mono,monospace" font-size="11" fill="#64748b" text-anchor="middle">XLSX</text>

  <!-- ── SFTP INPUT (x=240, gap=100 dalle sorgenti) ── -->
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

  <!-- ── RIQUADRO DB PostgreSQL (x=470, copre RAW+STG+PRD) ── -->
  <rect x="468" y="14" width="732" height="428" rx="12" fill="none" stroke="#f59e0b" stroke-width="1.8" stroke-dasharray="8 4"/>
  <rect x="490" y="6" width="168" height="20" rx="4" fill="#060c18"/>
  <text x="574" y="21" font-family="JetBrains Mono,monospace" font-size="12" font-weight="700" fill="#f59e0b" text-anchor="middle" letter-spacing="1">DB PostgreSQL</text>

  <!-- ── SCHEMA RAW (x=490) ── -->
  <rect x="490" y="26" width="180" height="196" rx="9" fill="#0d1f3c" stroke="#3b82f6" stroke-width="2.2"/>
  <text x="580" y="52"  font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#60a5fa" text-anchor="middle" letter-spacing="2">SCHEMA RAW</text>
  <line x1="500" y1="62" x2="660" y2="62" stroke="#1d4ed8" stroke-width="1"/>
  <text x="580" y="82"  font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_CUST_GEN#ZBP-*</text>
  <text x="580" y="102" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_SUPPL_GEN#ZBP-*</text>
  <text x="580" y="122" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_CUST/SUPPL_TAXNUMBERS</text>
  <text x="580" y="142" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_MARA · S_MARC</text>
  <text x="580" y="162" font-family="JetBrains Mono,monospace" font-size="12" fill="#93c5fd" text-anchor="middle">S_MBEW · S_MVKE</text>
  <text x="580" y="202" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#64748b" text-anchor="middle">dati grezzi ERP</text>

  <!-- ── SCHEMA REF (x=490, y=244) ── -->
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

  <!-- ── SCHEMA STG (x=730) ── -->
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

  <!-- ── SCHEMA PRD (x=1000) ── -->
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

  <!-- ── SFTP OUTPUT (x=1260) ── -->
  <rect x="1260" y="130" width="150" height="78" rx="8" fill="#0c1e38" stroke="#2dd4bf" stroke-width="2.2"/>
  <text x="1335" y="158" font-family="JetBrains Mono,monospace" font-size="13" font-weight="700" fill="#93c5fd" text-anchor="middle">SFTPGo</text>
  <line x1="1268" y1="168" x2="1402" y2="168" stroke="#0e7490" stroke-width="1"/>
  <text x="1335" y="192" font-family="JetBrains Mono,monospace" font-size="11.5" fill="#2dd4bf" font-weight="600" text-anchor="middle">out_source_mdg</text>

  <!-- PRD → SFTPGo output -->
  <line x1="1178" y1="190" x2="1258" y2="176" stroke="#2dd4bf" stroke-width="2.2" marker-end="url(#parr-teal)"/>
  <text x="1218" y="208" font-family="JetBrains Mono,monospace" font-size="11" font-weight="600" fill="#2dd4bf" text-anchor="middle">ZIP normalizzati</text>

  <!-- ── SAP S/4HANA (x=1260, y=340) ── -->
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
        "desc": "Punto di ingresso dei file provenienti dall'ERP. Espone un server SFTP (porta 22) su cui vengono depositati i file ZIP e XLSX. Sostituisce atmoz/sftp con <b>SFTPGo</b>: WebAdmin su porta 8082 per gestione utenti, audit log e chiavi SSH senza riavvio del container. I volumi datalake sono condivisi con Bruin.",
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
        "desc": "Database centrale del progetto. Ospita tre schemi: <code>raw</code> (dati ERP grezzi), <code>ref</code> (tabelle di controllo SAP), <code>stg</code> (risultati check, catalogo e storico run). È l'unico servizio con healthcheck attivo — tutti gli altri dipendono da esso.",
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

