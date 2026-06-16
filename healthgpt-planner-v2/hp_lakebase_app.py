"""
HealthGPT Care Gap Trust Planner — Redesigned
hp_lakebase_app.py

Databricks Apps — Streamlit frontend backed by Lakebase (PostgreSQL)
6 pages: Overview · Care Map · Facilities · Evidence Review · Scenario Planner · Architecture/Trust
"""

import json
import os
import warnings

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

warnings.filterwarnings("ignore")

# ============================================================================
# Configuration
# ============================================================================
LAKEBASE_HOST = "ep-wild-snow-d8k94scg.database.us-east-2.cloud.databricks.com"
LAKEBASE_PORT = 5432
LAKEBASE_DB = "healthgpt"
LAKEBASE_USER = "manoj.rayalla@acuitybrands.com"
ENDPOINT_NAME = "projects/hackthon/branches/production/endpoints/primary"
TOKEN_TTL_SECS = 3300
WORKSPACE_URL = "https://dbc-f49e9aec-67ba.cloud.databricks.com"

DEFAULT_STATE = "Tamil Nadu"
DEFAULT_DISTRICT = "Nicobars"
DEFAULT_CAPABILITY = "Maternity Care"

EVIDENCE_BADGES = {
    "strong": {"bg": "#D1FAE5", "fg": "#065F46", "label": "Strong"},
    "partial": {"bg": "#FEF3C7", "fg": "#92400E", "label": "Partial"},
    "weak": {"bg": "#FEE2E2", "fg": "#991B1B", "label": "Weak"},
    "no_claim": {"bg": "#F3F4F6", "fg": "#374151", "label": "No Claim"},
}

LEVEL_COLORS = {
    "strong": "#10B981",
    "partial": "#F59E0B",
    "weak": "#EF4444",
    "no_claim": "#9CA3AF",
}

NAV_ITEMS = [
    ("overview", "📊", "Overview"),
    ("care_map", "🗺️", "Care Map"),
    ("facilities", "🏥", "Facilities"),
    ("evidence_review", "📋", "Evidence Review"),
    ("scenario_planner", "🔮", "Scenario Planner"),
    ("architecture_trust", "🔒", "Architecture / Trust"),
]

# ============================================================================
# Connection management  (copied verbatim from healthgpt_app.py)
# ============================================================================


def _generate_token() -> str:
    """Return a token usable as Lakebase PostgreSQL password.

    Priority order (fastest / most reliable first):
    1. DATABRICKS_TOKEN env var — always set inside Databricks Apps.
    2. Databricks REST API /api/2.0/lakebase/credentials/generate (using bearer from SDK)
    3. Bearer token from SDK authenticate() directly (works for OAuth/CLI + PAT)
    """
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if token:
        return token

    try:
        import requests  # noqa: PLC0415
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415

        ws_host = os.environ.get("DATABRICKS_HOST", WORKSPACE_URL).rstrip("/")
        _w = WorkspaceClient()

        # Resolve bearer via authenticate() — works for OAuth/CLI as well as PAT
        try:
            auth_headers = dict(_w.config.authenticate())
            bearer = (
                auth_headers.get("Authorization", "").replace("Bearer ", "").strip()
            )
        except Exception:
            bearer = _w.config.token or ""

        if bearer:
            # Try Lakebase-specific token exchange first
            try:
                resp = requests.post(
                    f"{ws_host}/api/2.0/lakebase/credentials/generate",
                    headers={
                        "Authorization": f"Bearer {bearer}",
                        "Content-Type": "application/json",
                    },
                    json={"endpoint": ENDPOINT_NAME},
                    timeout=15,
                )
                if resp.ok:
                    data = resp.json()
                    tok = data.get("token") or data.get("access_token")
                    if tok:
                        return tok
            except Exception:
                pass
            # Fall back to using the bearer token directly as Lakebase password
            return bearer
    except Exception:
        pass

    raise RuntimeError(
        "Unable to obtain a Lakebase token. "
        "Ensure DATABRICKS_TOKEN is set or the Databricks SDK is configured."
    )


@st.cache_resource(ttl=TOKEN_TTL_SECS)
def _get_connection():
    """Cached psycopg3 connection, refreshed every TOKEN_TTL_SECS seconds."""
    import psycopg  # noqa: PLC0415

    token = _generate_token()
    conn = psycopg.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB,
        user=LAKEBASE_USER,
        password=token,
        sslmode="require",
        connect_timeout=30,
        autocommit=True,
    )
    return conn


def query_df(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


def query_one(sql: str, params=None):
    df = query_df(sql, params)
    if df.empty:
        return None
    return df.iloc[0]


def sg(row, key: str, default="") -> str:
    """Safe-get a value from a pandas Series, converting NaN to default."""
    val = row.get(key, default)
    try:
        if pd.isna(val):
            return default
    except Exception:
        pass
    return str(val) if val is not None else default


def si(row, key: str, default: int = 0) -> int:
    """Safe-get an integer value."""
    try:
        return int(row.get(key, default))
    except Exception:
        return default


def sf(row, key: str, default: float = 0.0) -> float:
    """Safe-get a float value."""
    try:
        return float(row.get(key, default))
    except Exception:
        return default


# ============================================================================
# CSS / design system injection
# ============================================================================


def inject_css():
    st.markdown(
        """
<style>
/* ── Global ──────────────────────────────────────────────────────────────── */
[data-testid="stAppViewContainer"] > .main {
    background: #F8FAFC;
}
[data-testid="stAppViewContainer"] .block-container {
    padding-top: 1rem;
    padding-bottom: 1rem;
}

/* ── Sidebar ─────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #0D1B2A !important;
    border-right: 1px solid #1E3A4A;
}
[data-testid="stSidebar"] * {
    color: #E2E8F0 !important;
}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] * {
    color: #CBD5E1 !important;
    font-size: 13px !important;
}
[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    color: #CBD5E1 !important;
    border: none !important;
    border-radius: 6px !important;
    text-align: left !important;
    padding: 8px 12px !important;
    font-size: 13px !important;
    width: 100%;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(13,148,136,0.12) !important;
    color: #F0FDFA !important;
}

/* ── Cards ───────────────────────────────────────────────────────────────── */
.hp-card {
    background: white;
    border-radius: 10px;
    padding: 18px 20px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    margin-bottom: 14px;
}

/* ── Context bar ─────────────────────────────────────────────────────────── */
.hp-ctx {
    background: white;
    border-radius: 8px;
    padding: 9px 18px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    display: flex;
    align-items: center;
    gap: 6px;
    margin-bottom: 18px;
    font-size: 13px;
    color: #374151;
}
.hp-ctx-sep { color: #D1D5DB; margin: 0 2px; }

/* ── Badges ──────────────────────────────────────────────────────────────── */
.hp-badge {
    display: inline-block;
    padding: 2px 9px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    line-height: 1.6;
    white-space: nowrap;
}
.hp-bg-strong    { background:#D1FAE5; color:#065F46; }
.hp-bg-partial   { background:#FEF3C7; color:#92400E; }
.hp-bg-weak      { background:#FEE2E2; color:#991B1B; }
.hp-bg-no-claim  { background:#F3F4F6; color:#374151; }
.hp-bg-high-gap  { background:#FEE2E2; color:#DC2626; }
.hp-bg-high      { background:#FEE2E2; color:#DC2626; }
.hp-bg-medium    { background:#FEF3C7; color:#92400E; }
.hp-bg-info      { background:#EFF6FF; color:#1D4ED8; }
.hp-bg-purple    { background:#F5F3FF; color:#5B21B6; }

/* ── Section titles ──────────────────────────────────────────────────────── */
.hp-sec {
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #6B7280;
    margin-bottom: 10px;
}

/* ── Risk pill ───────────────────────────────────────────────────────────── */
.hp-risk {
    background: #FEF9C3;
    color: #713F12;
    border-radius: 6px;
    padding: 5px 10px;
    font-size: 12px;
    margin-bottom: 6px;
    display: block;
}

/* ── Action item ─────────────────────────────────────────────────────────── */
.hp-act {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 7px 0;
    border-bottom: 1px solid #F3F4F6;
    font-size: 12px;
    color: #374151;
}
.hp-act-num {
    background: #0D9488;
    color: white;
    border-radius: 50%;
    min-width: 20px;
    height: 20px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 10px;
    font-weight: 700;
    flex-shrink: 0;
}

/* ── Chat bubbles ────────────────────────────────────────────────────────── */
.hp-chat-user {
    background: #0D9488;
    color: white;
    border-radius: 12px 12px 2px 12px;
    padding: 9px 13px;
    margin: 5px 0 5px 20%;
    font-size: 12px;
    line-height: 1.5;
}
.hp-chat-ai {
    background: white;
    color: #1F2937;
    border-radius: 12px 12px 12px 2px;
    padding: 9px 13px;
    margin: 5px 20% 5px 0;
    font-size: 12px;
    line-height: 1.5;
    border: 1px solid #E5E7EB;
}

/* ── Queue item ──────────────────────────────────────────────────────────── */
.hp-q-high   { border-left: 3px solid #EF4444; }
.hp-q-medium { border-left: 3px solid #F59E0B; }
.hp-q-item {
    padding: 9px 13px;
    background: white;
    border-radius: 0 8px 8px 0;
    margin-bottom: 9px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}

/* ── Trust First card ────────────────────────────────────────────────────── */
.hp-trust-card {
    background: rgba(13,148,136,0.14);
    border: 1px solid rgba(13,148,136,0.28);
    border-radius: 8px;
    padding: 12px 14px;
}

/* ── Main action buttons ─────────────────────────────────────────────────── */
.stButton > button {
    background: #0D9488 !important;
    color: white !important;
    border: none !important;
    border-radius: 6px !important;
    font-size: 13px !important;
}
.stButton > button:hover {
    background: #0F766E !important;
}

/* ── Hide chrome ─────────────────────────────────────────────────────────── */
#MainMenu, footer, .stDeployButton { visibility: hidden; }
header[data-testid="stHeader"] { background: transparent; }
</style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================================
# Helpers
# ============================================================================


def badge(level: str) -> str:
    """Return an HTML badge span for the given evidence level string."""
    lvl = level.strip().lower().replace(" ", "_")
    cfg = EVIDENCE_BADGES.get(lvl, EVIDENCE_BADGES["no_claim"])
    return (
        f'<span class="hp-badge" '
        f'style="background:{cfg["bg"]};color:{cfg["fg"]}">'
        f"{cfg['label']}</span>"
    )


def ctx_bar(state: str, district: str, capability: str):
    st.markdown(
        f"""
<div class="hp-ctx">
  <span>📍 <strong>State:</strong> {state}</span>
  <span class="hp-ctx-sep">|</span>
  <span><strong>District:</strong> {district}</span>
  <span class="hp-ctx-sep">|</span>
  <span><strong>Capability:</strong> {capability}</span>
  <span style="flex:1"></span>
  <span style="background:#0D9488;color:white;padding:4px 12px;
               border-radius:6px;font-size:11px;font-weight:600">
    ✏️ Change Context
  </span>
</div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================================
# Sidebar
# ============================================================================


def render_sidebar():
    with st.sidebar:
        st.markdown(
            """
<div style="padding:16px 4px 20px 4px">
  <div style="font-size:19px;font-weight:700;color:#F0FDFA">🏥 HealthGPT</div>
  <div style="font-size:11px;color:#94A3B8;margin-top:2px">Care Gap Trust Planner</div>
</div>
            """,
            unsafe_allow_html=True,
        )

        current = st.session_state.get("page", "overview")

        for key, icon, label in NAV_ITEMS:
            is_active = current == key
            btn_style = (
                (
                    "background:rgba(13,148,136,0.18)!important;"
                    "color:#F0FDFA!important;"
                    "border-left:3px solid #0D9488!important;"
                    "padding-left:9px!important;"
                )
                if is_active
                else ""
            )

            # Use a container div to apply active styling around the button
            st.markdown(
                f'<div style="{btn_style}border-radius:6px;margin-bottom:2px">',
                unsafe_allow_html=True,
            )
            if st.button(
                f"{icon}  {label}", key=f"nav_{key}", use_container_width=True
            ):
                st.session_state["page"] = key
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown(
            "<hr style='border-color:#1E3A4A;margin:18px 0 14px 0'>",
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div style="font-size:10px;color:#94A3B8;font-weight:700;'
            'text-transform:uppercase;margin-bottom:6px">Context</div>',
            unsafe_allow_html=True,
        )

        st.selectbox("State", ["Tamil Nadu"], index=0, key="sel_state")
        st.selectbox("District", ["Nicobars"], index=0, key="sel_district")
        st.selectbox("Capability", ["Maternity Care"], index=0, key="sel_capability")

        st.markdown(
            """
<div class="hp-trust-card" style="margin-top:28px">
  <div style="font-size:12px;font-weight:700;color:#99F6E4;margin-bottom:4px">
    🛡️ Trust First
  </div>
  <div style="font-size:11px;color:#A7F3D0;line-height:1.5">
    Every recommendation is evidence-graded.
    Unverified claims are clearly flagged.
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================================
# Page 1: Overview
# ============================================================================


def page_overview(state, district, capability):
    ctx_bar(state, district, capability)
    st.markdown(f"### {district} · {capability} — Gap Analysis")

    row = query_one(
        "SELECT * FROM hp_district_overview "
        "WHERE state=%s AND district=%s AND capability=%s LIMIT 1",
        (state, district, capability),
    )
    if row is None:
        st.warning("No overview data found for the selected context.")
        return

    gap = si(row, "gap_score", 0)
    conf = sf(row, "confidence_score", 0.0)
    sc = si(row, "strong_count")
    pc = si(row, "partial_count")
    wc = si(row, "weak_count")
    nc = si(row, "no_claim_count")

    # ── Row 1: Gap gauge · Confidence gauge · Donut ─────────────────────────
    c1, c2, c3 = st.columns([1, 1, 1.4])

    with c1:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown('<div class="hp-sec">Gap Score</div>', unsafe_allow_html=True)
        fig_g = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=gap,
                gauge={
                    "axis": {
                        "range": [0, 100],
                        "tickcolor": "#9CA3AF",
                        "tickfont": {"size": 9},
                    },
                    "bar": {"color": "#DC2626"},
                    "bgcolor": "#F3F4F6",
                    "steps": [
                        {"range": [0, 40], "color": "#DCFCE7"},
                        {"range": [40, 70], "color": "#FEF9C3"},
                        {"range": [70, 100], "color": "#FEE2E2"},
                    ],
                },
                number={"suffix": "/100", "font": {"size": 26, "color": "#1F2937"}},
                domain={"x": [0, 1], "y": [0, 1]},
            )
        )
        fig_g.update_layout(
            height=175, margin=dict(l=12, r=12, t=16, b=10), paper_bgcolor="white"
        )
        st.plotly_chart(
            fig_g, use_container_width=True, config={"displayModeBar": False}
        )
        level_label = sg(row, "gap_level", "HIGH GAP")
        st.markdown(
            f'<div style="text-align:center;margin-top:-6px">'
            f'<span class="hp-badge hp-bg-high-gap">{level_label}</span></div>',
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown('<div class="hp-sec">Confidence</div>', unsafe_allow_html=True)
        fig_c = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=round(conf * 100),
                gauge={
                    "axis": {
                        "range": [0, 100],
                        "tickcolor": "#9CA3AF",
                        "tickfont": {"size": 9},
                    },
                    "bar": {"color": "#0D9488"},
                    "bgcolor": "#F3F4F6",
                },
                number={"suffix": "%", "font": {"size": 26, "color": "#1F2937"}},
                domain={"x": [0, 1], "y": [0, 1]},
            )
        )
        fig_c.update_layout(
            height=175, margin=dict(l=12, r=12, t=16, b=10), paper_bgcolor="white"
        )
        st.plotly_chart(
            fig_c, use_container_width=True, config={"displayModeBar": False}
        )
        conf_label = sg(row, "confidence_level", "Medium-High")
        st.markdown(
            f'<div style="text-align:center;margin-top:-6px;font-size:12px;color:#6B7280">'
            f"{conf_label} Confidence</div>",
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    with c3:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Facility Evidence Distribution</div>',
            unsafe_allow_html=True,
        )
        total = sc + pc + wc + nc
        fig_d = go.Figure(
            data=[
                go.Pie(
                    labels=["Strong", "Partial", "Weak", "No Claim"],
                    values=[sc, pc, wc, nc],
                    hole=0.56,
                    marker_colors=["#10B981", "#F59E0B", "#EF4444", "#D1D5DB"],
                    textinfo="label+value",
                    textfont_size=11,
                    hovertemplate="%{label}: %{value}<extra></extra>",
                )
            ]
        )
        fig_d.update_layout(
            height=195,
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="white",
            showlegend=False,
            annotations=[
                {
                    "text": f"{total}<br><span style='font-size:10px'>Total</span>",
                    "x": 0.5,
                    "y": 0.5,
                    "font_size": 15,
                    "showarrow": False,
                }
            ],
        )
        st.plotly_chart(
            fig_d, use_container_width=True, config={"displayModeBar": False}
        )
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Row 2: AI brief + Radar ──────────────────────────────────────────────
    c_brief, c_radar = st.columns([1.4, 1])

    with c_brief:
        ai_brief = sg(row, "ai_brief")
        st.markdown(
            f'<div class="hp-card">'
            f'<div class="hp-sec">🤖 AI Analysis Brief</div>'
            f'<div style="font-size:13px;color:#374151;line-height:1.75">{ai_brief}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )

    with c_radar:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown('<div class="hp-sec">Trust Radar</div>', unsafe_allow_html=True)
        cats = [
            "Facility Density",
            "ANC Coverage",
            "Specialist Access",
            "Equipment Trust",
            "Claims Authenticity",
        ]
        vals = [
            sf(row, "radar_facility_density"),
            sf(row, "radar_anc_coverage"),
            sf(row, "radar_specialist_access"),
            sf(row, "radar_equipment_trust"),
            sf(row, "radar_claims_authenticity"),
        ]
        fig_r = go.Figure(
            go.Scatterpolar(
                r=vals + [vals[0]],
                theta=cats + [cats[0]],
                fill="toself",
                fillcolor="rgba(13,148,136,0.18)",
                line={"color": "#0D9488", "width": 2},
                marker={"color": "#0D9488", "size": 6},
            )
        )
        fig_r.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100], tickfont={"size": 9})
            ),
            showlegend=False,
            height=220,
            margin=dict(l=30, r=30, t=16, b=16),
            paper_bgcolor="white",
        )
        st.plotly_chart(
            fig_r, use_container_width=True, config={"displayModeBar": False}
        )
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Row 3: Risk · Actions · Decision ────────────────────────────────────
    c_risk, c_act, c_dec = st.columns(3)

    with c_risk:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">⚠️ At-Risk Indicators</div>', unsafe_allow_html=True
        )
        for i in range(1, 5):
            ri = sg(row, f"risk_indicator_{i}")
            if ri:
                st.markdown(
                    f'<span class="hp-risk">• {ri}</span>', unsafe_allow_html=True
                )
        st.markdown("</div>", unsafe_allow_html=True)

    with c_act:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">✅ Recommended Actions</div>', unsafe_allow_html=True
        )
        for i in range(1, 5):
            act = sg(row, f"action_{i}")
            if act:
                st.markdown(
                    f'<div class="hp-act">'
                    f'<span class="hp-act-num">{i}</span>'
                    f"<span>{act}</span></div>",
                    unsafe_allow_html=True,
                )
        st.markdown("</div>", unsafe_allow_html=True)

    with c_dec:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">📝 Planner Decision</div>', unsafe_allow_html=True
        )
        cur_dec = sg(row, "planner_decision", "Pending Review")
        st.markdown(
            f'<div style="font-size:12px;color:#6B7280;margin-bottom:10px">Current status:</div>'
            f'<span class="hp-badge hp-bg-medium" style="font-size:12px;padding:5px 12px">'
            f"{cur_dec}</span>",
            unsafe_allow_html=True,
        )
        st.markdown('<div style="margin-top:14px">', unsafe_allow_html=True)
        st.selectbox(
            "Update decision",
            [
                "Pending Review",
                "Escalate to State",
                "Deploy Resources",
                "Schedule Field Visit",
                "Archive",
            ],
            index=0,
            key="ov_decision",
        )
        if st.button("Save Decision", key="ov_save"):
            st.success("Decision saved.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# Page 2: Care Map
# ============================================================================


def page_care_map(state, district, capability):
    ctx_bar(state, district, capability)
    st.markdown("### Care Facility Map")

    df = query_df(
        "SELECT * FROM hp_facility_map "
        "WHERE state=%s AND district=%s AND capability=%s",
        (state, district, capability),
    )
    if df.empty:
        st.warning("No facility map data found.")
        return

    label_map = {
        "strong": "Strong",
        "partial": "Partial",
        "weak": "Weak",
        "no_claim": "No Claim",
    }
    color_map = {
        "Strong": "#10B981",
        "Partial": "#F59E0B",
        "Weak": "#EF4444",
        "No Claim": "#9CA3AF",
    }
    df["ev_label"] = df["evidence_level"].map(label_map).fillna("Unknown")

    col_map, col_info = st.columns([2.4, 1])

    with col_map:
        fig_m = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            color="ev_label",
            color_discrete_map=color_map,
            hover_name="facility_name",
            hover_data={
                "latitude": False,
                "longitude": False,
                "zone": True,
                "ev_label": True,
                "review_reason": True,
            },
            size_max=14,
            zoom=7,
            height=480,
            mapbox_style="carto-positron",
        )
        fig_m.update_traces(marker=dict(size=13, opacity=0.9))
        fig_m.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            legend=dict(
                title="Evidence",
                orientation="h",
                y=-0.04,
                font={"size": 11},
            ),
        )
        st.plotly_chart(
            fig_m, use_container_width=True, config={"displayModeBar": False}
        )

    with col_info:
        total = len(df)
        priority = int(df["is_priority_review"].astype(bool).sum())
        n_strong = int((df["evidence_level"] == "strong").sum())
        n_weak_nc = int(
            (
                (df["evidence_level"] == "weak") | (df["evidence_level"] == "no_claim")
            ).sum()
        )

        st.markdown(
            f"""
<div class="hp-card">
  <div class="hp-sec">Geography Summary</div>
  <div style="font-size:13px;color:#374151;line-height:2.1">
    <div>📍 <strong>District:</strong> {district}</div>
    <div>🏥 <strong>Total Facilities:</strong> {total}</div>
    <div>🔴 <strong>Priority Review:</strong> {priority}</div>
    <div>✅ <strong>Strong Evidence:</strong> {n_strong}</div>
    <div>⚠️ <strong>Weak / No Claim:</strong> {n_weak_nc}</div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Facilities to Review</div>', unsafe_allow_html=True
        )
        priority_df = df[df["is_priority_review"].astype(bool)][
            ["facility_name", "evidence_level", "review_reason"]
        ].head(6)
        for _, r in priority_df.iterrows():
            reason = r["review_reason"] or "Review needed"
            b = badge(r["evidence_level"])
            st.markdown(
                f'<div style="padding:7px 0;border-bottom:1px solid #F3F4F6">'
                f'<div style="font-size:13px;font-weight:600;color:#1F2937">'
                f"{r['facility_name']} {b}</div>"
                f'<div style="font-size:11px;color:#6B7280;margin-top:2px">{reason}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# Page 3: Facilities
# ============================================================================


def page_facilities(state, district, capability):
    ctx_bar(state, district, capability)
    st.markdown("### Facility Evidence Review")

    df_names = query_df(
        "SELECT DISTINCT facility_name FROM hp_facility_evidence "
        "WHERE state=%s AND district=%s AND capability=%s ORDER BY facility_name",
        (state, district, capability),
    )
    if df_names.empty:
        st.warning("No facility evidence data found.")
        return

    facilities = df_names["facility_name"].tolist()
    selected = st.selectbox("Select Facility", facilities, key="fac_sel")

    row = query_one(
        "SELECT * FROM hp_facility_evidence "
        "WHERE facility_name=%s AND state=%s AND district=%s AND capability=%s LIMIT 1",
        (selected, state, district, capability),
    )
    if row is None:
        return

    trust_sig = sg(row, "trust_signal", "Unknown")
    ts_map = {
        "strong": "hp-bg-strong",
        "partial": "hp-bg-partial",
        "weak": "hp-bg-weak",
    }
    ts_class = ts_map.get(trust_sig.lower(), "hp-bg-weak")
    specialties = sg(row, "specialties", "—")
    equipment = sg(row, "equipment", "—")
    source_url = sg(row, "source_url", "")

    # Facility header card
    src_link = (
        f'<a href="{source_url}" target="_blank" style="font-size:11px;color:#0D9488">'
        f"🔗 Source</a>"
        if source_url
        else ""
    )
    st.markdown(
        f"""
<div class="hp-card" style="margin-bottom:10px">
  <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <span style="font-size:19px;font-weight:700;color:#1F2937">{selected}</span>
    <span class="hp-badge {ts_class}">{trust_sig} Trust</span>
    {src_link}
  </div>
  <div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap">
    <span class="hp-badge hp-bg-info">{specialties}</span>
    <span class="hp-badge hp-bg-purple">{equipment}</span>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    col_ev, col_txt = st.columns([1.2, 1])

    with col_ev:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Evidence Breakdown</div>', unsafe_allow_html=True
        )
        for i in range(1, 7):
            lbl = sg(row, f"ev{i}_label")
            txt = sg(row, f"ev{i}_text")
            level = sg(row, f"ev{i}_level", "no_claim").lower()
            if not lbl:
                continue
            dot = LEVEL_COLORS.get(level, "#9CA3AF")
            b = badge(level)
            st.markdown(
                f"""
<div style="display:flex;align-items:flex-start;gap:8px;
            padding:7px 0;border-bottom:1px solid #F3F4F6">
  <span style="width:9px;height:9px;border-radius:50%;background:{dot};
               display:inline-block;margin-top:4px;flex-shrink:0"></span>
  <div style="flex:1">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <span style="font-size:13px;font-weight:600;color:#374151">{lbl}</span>
      {b}
    </div>
    <div style="font-size:12px;color:#6B7280;margin-top:2px">{txt}</div>
  </div>
</div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)

    with col_txt:
        underlying = sg(row, "underlying_text", "—")
        suggested = sg(row, "suggested_note", "—")

        st.markdown(
            f'<div class="hp-card">'
            f'<div class="hp-sec">Underlying Evidence Text</div>'
            f'<div style="font-size:13px;color:#374151;line-height:1.75">{underlying}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="hp-card">'
            f'<div class="hp-sec">Suggested Note</div>'
            f'<div style="font-size:12px;color:#6B7280;font-style:italic">{suggested}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )

        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Planner Decision</div>', unsafe_allow_html=True
        )
        bc1, bc2 = st.columns(2)
        with bc1:
            if st.button("✅ Accept", key="fac_accept", use_container_width=True):
                st.success("Accepted")
            if st.button("❌ Reject", key="fac_reject", use_container_width=True):
                st.error("Rejected")
        with bc2:
            if st.button("🔍 Needs Review", key="fac_review", use_container_width=True):
                st.warning("Flagged for review")
            if st.button("📝 Add Note", key="fac_note", use_container_width=True):
                st.session_state["show_note"] = True
        if st.session_state.get("show_note"):
            note = st.text_area("Add a note", key="fac_note_txt", height=70)
            if note and st.button("Submit Note", key="fac_note_submit"):
                st.info("Note saved.")
                st.session_state["show_note"] = False
        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# Page 4: Evidence Review
# ============================================================================


def page_evidence_review(state, district, capability):
    ctx_bar(state, district, capability)
    st.markdown("### Evidence Review Brief")

    row = query_one(
        "SELECT * FROM hp_evidence_briefs "
        "WHERE state=%s AND district=%s AND capability=%s LIMIT 1",
        (state, district, capability),
    )
    if row is None:
        st.warning("No evidence brief found for the selected context.")
        return

    c_left, c_right = st.columns([1.45, 1])

    with c_left:
        title = sg(row, "brief_title", "Evidence Brief")
        conf = sg(row, "confidence_level", "Medium-High")
        tag1 = sg(row, "tag_1")
        tag2 = sg(row, "tag_2")
        tag3 = sg(row, "tag_3")

        st.markdown(
            f"""
<div class="hp-card">
  <div style="font-size:17px;font-weight:700;color:#1F2937;margin-bottom:8px">{title}</div>
  <div style="display:flex;gap:7px;flex-wrap:wrap">
    <span class="hp-badge hp-bg-info">{tag1}</span>
    <span class="hp-badge hp-bg-medium">{tag2}</span>
    <span class="hp-badge hp-bg-high-gap">{tag3}</span>
  </div>
  <div style="font-size:12px;color:#6B7280;margin-top:8px">
    Confidence: <strong>{conf}</strong>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

        # Why this gap is real
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Why This Gap Is Likely Real</div>',
            unsafe_allow_html=True,
        )
        for i in range(1, 5):
            r_txt = sg(row, f"reason_{i}")
            if r_txt:
                st.markdown(
                    f'<div class="hp-act">'
                    f'<span class="hp-act-num">{i}</span>'
                    f'<span style="font-size:13px;color:#374151">{r_txt}</span>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
        st.markdown("</div>", unsafe_allow_html=True)

        # Recommended actions
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Recommended Actions</div>', unsafe_allow_html=True
        )
        for i in range(1, 5):
            act = sg(row, f"action_{i}")
            if act:
                st.markdown(
                    f'<div class="hp-act">'
                    f'<span class="hp-act-num" style="background:#0D9488">{i}</span>'
                    f'<span style="font-size:13px;color:#374151">{act}</span>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
        st.markdown("</div>", unsafe_allow_html=True)

    with c_right:
        # Expected impact table
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown('<div class="hp-sec">Expected Impact</div>', unsafe_allow_html=True)

        metrics = [
            "Gap Score",
            "Confidence",
            "Strong Evidence Facilities",
            "Facilities to Review",
        ]
        befores = [
            sg(row, "impact_gap_before", "—"),
            sg(row, "impact_conf_before", "—"),
            sg(row, "impact_strong_before", "—"),
            sg(row, "impact_review_before", "—"),
        ]
        afters = [
            sg(row, "impact_gap_after", "—"),
            sg(row, "impact_conf_after", "—"),
            sg(row, "impact_strong_after", "—"),
            sg(row, "impact_review_after", "—"),
        ]

        tbl = (
            '<table style="width:100%;font-size:12px;border-collapse:collapse">'
            '<thead><tr style="background:#F9FAFB">'
            '<th style="padding:7px 8px;text-align:left;color:#6B7280;font-weight:600">Metric</th>'
            '<th style="padding:7px 8px;text-align:center;color:#6B7280;font-weight:600">Before</th>'
            '<th style="padding:7px 8px;text-align:center;color:#6B7280;font-weight:600">After</th>'
            '<th style="padding:7px 8px;text-align:center;color:#6B7280;font-weight:600">Δ</th>'
            "</tr></thead><tbody>"
        )
        for m, b_val, a_val in zip(metrics, befores, afters):
            try:
                delta = int(a_val) - int(b_val)
                arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "→")
                dc = "#10B981" if delta > 0 else ("#EF4444" if delta < 0 else "#6B7280")
                ds = f'<span style="color:{dc}">{arrow} {abs(delta)}</span>'
            except Exception:
                ds = "—"
            tbl += (
                f'<tr style="border-bottom:1px solid #F3F4F6">'
                f'<td style="padding:7px 8px;font-weight:500;color:#374151">{m}</td>'
                f'<td style="padding:7px 8px;text-align:center;color:#6B7280">{b_val}</td>'
                f'<td style="padding:7px 8px;text-align:center;color:#0D9488;font-weight:600">{a_val}</td>'
                f'<td style="padding:7px 8px;text-align:center">{ds}</td>'
                f"</tr>"
            )
        tbl += "</tbody></table>"
        st.markdown(tbl, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# Page 5: Scenario Planner
# ============================================================================


def page_scenario_planner(state, district, capability):
    ctx_bar(state, district, capability)
    st.markdown("### Scenario Planner")

    row = query_one(
        "SELECT * FROM hp_scenario_data "
        "WHERE state=%s AND district=%s AND capability=%s LIMIT 1",
        (state, district, capability),
    )
    if row is None:
        st.warning("No scenario data found.")
        return

    gap_before = si(row, "gap_before", 82)

    c_sliders, c_charts = st.columns([1.1, 1])

    with c_sliders:
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Simulation Parameters</div>', unsafe_allow_html=True
        )

        s1 = st.slider(
            sg(row, "scenario_1_name", "Add OB/GYN Centers"),
            si(row, "scenario_1_min"),
            si(row, "scenario_1_max", 5),
            si(row, "scenario_1_default", 1),
            key="sc1",
        )
        s2 = st.slider(
            sg(row, "scenario_2_name", "Verify Facility Claims"),
            si(row, "scenario_2_min"),
            si(row, "scenario_2_max", 10),
            si(row, "scenario_2_default", 3),
            key="sc2",
        )
        s3 = st.slider(
            sg(row, "scenario_3_name", "ANC Outreach Coverage (%)"),
            si(row, "scenario_3_min"),
            si(row, "scenario_3_max", 100),
            si(row, "scenario_3_default", 20),
            key="sc3",
        )
        st.markdown("</div>", unsafe_allow_html=True)

        simulated_gap = max(0, round(gap_before - (s1 * 10) - (s2 * 3) - (s3 * 0.25)))
        pct_improve = (
            round((1 - simulated_gap / gap_before) * 100) if gap_before > 0 else 0
        )

        st.markdown(
            f"""
<div class="hp-card" style="text-align:center;padding:22px 20px">
  <div class="hp-sec">Simulated Gap Score</div>
  <div style="font-size:54px;font-weight:800;color:#0D9488;line-height:1">
    {simulated_gap}
  </div>
  <div style="font-size:15px;color:#6B7280;margin-top:6px">
    <span style="text-decoration:line-through;color:#EF4444">{gap_before}</span>
    &nbsp;→&nbsp;
    <span style="color:#0D9488;font-weight:700">{simulated_gap}</span>
  </div>
  <div style="font-size:12px;color:#6B7280;margin-top:6px">
    {pct_improve}% improvement
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

        interpretation = sg(row, "interpretation")
        if interpretation:
            st.markdown(
                f'<div class="hp-card">'
                f'<div class="hp-sec">📊 Interpretation</div>'
                f'<div style="font-size:13px;color:#374151;line-height:1.7">'
                f"{interpretation}</div></div>",
                unsafe_allow_html=True,
            )

    with c_charts:
        # Impact matrix bubble chart
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown('<div class="hp-sec">Impact Matrix</div>', unsafe_allow_html=True)
        names = [
            sg(row, "scenario_1_name", "OB/GYN"),
            sg(row, "scenario_2_name", "Claims"),
            sg(row, "scenario_3_name", "ANC"),
        ]
        short = ["OB/GYN", "Claims", "ANC"]
        impacts = [s1 * 10, s2 * 3, round(s3 * 0.25)]
        effort = [3, 2, 1]
        fig_b = go.Figure(
            go.Scatter(
                x=effort,
                y=impacts,
                mode="markers+text",
                text=short,
                textposition="top center",
                marker=dict(
                    size=[max(18, v * 4) for v in impacts],
                    color=["#0D9488", "#F59E0B", "#3B82F6"],
                    opacity=0.82,
                ),
                hovertemplate="%{text}<br>Gap reduction: %{y}<extra></extra>",
            )
        )
        fig_b.update_layout(
            xaxis=dict(
                title="Effort Level",
                tickvals=[1, 2, 3],
                ticktext=["Low", "Med", "High"],
            ),
            yaxis=dict(title="Gap Reduction"),
            height=210,
            margin=dict(l=40, r=10, t=10, b=40),
            paper_bgcolor="white",
        )
        st.plotly_chart(
            fig_b, use_container_width=True, config={"displayModeBar": False}
        )
        st.markdown("</div>", unsafe_allow_html=True)

        # Projected outcomes line chart
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">Projected Outcomes</div>', unsafe_allow_html=True
        )
        try:
            months = json.loads(sg(row, "timeline_months", "[1,3,6,9,12]"))
            baseline = json.loads(sg(row, "timeline_baseline", "[82,82,80,79,78]"))
            n = len(months)
            projected = [
                round(gap_before - (gap_before - simulated_gap) * i / max(n - 1, 1))
                for i in range(n)
            ]
            fig_l = go.Figure()
            fig_l.add_trace(
                go.Scatter(
                    x=months,
                    y=baseline,
                    mode="lines+markers",
                    name="Baseline",
                    line=dict(color="#9CA3AF", dash="dash", width=2),
                    marker=dict(size=5),
                )
            )
            fig_l.add_trace(
                go.Scatter(
                    x=months,
                    y=projected,
                    mode="lines+markers",
                    name="Projected",
                    line=dict(color="#0D9488", width=2),
                    fill="tonexty",
                    fillcolor="rgba(13,148,136,0.07)",
                    marker=dict(size=5),
                )
            )
            fig_l.update_layout(
                xaxis=dict(title="Month"),
                yaxis=dict(title="Gap Score"),
                height=210,
                margin=dict(l=40, r=10, t=10, b=40),
                paper_bgcolor="white",
                legend=dict(orientation="h", y=-0.28, font={"size": 11}),
            )
            st.plotly_chart(
                fig_l, use_container_width=True, config={"displayModeBar": False}
            )
        except Exception:
            st.info("Timeline chart unavailable.")
        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# Page 6: Architecture / Trust
# ============================================================================


def page_architecture_trust(state, district, capability):
    ctx_bar(state, district, capability)
    st.markdown("### Architecture & Trust Flow")

    row = query_one(
        "SELECT * FROM hp_review_queue "
        "WHERE state=%s AND district=%s AND capability=%s LIMIT 1",
        (state, district, capability),
    )

    c_flow, c_queue = st.columns([1.5, 1])

    with c_flow:
        # Trust data-flow Sankey diagram
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown('<div class="hp-sec">Trust Data Flow</div>', unsafe_allow_html=True)

        nodes = [
            "NPI Registry",  # 0
            "HMIS Reports",  # 1
            "Claims Data",  # 2
            "Data Fusion Engine",  # 3
            "AI Gap Analyser",  # 4
            "Evidence Grader",  # 5
            "Planner Dashboard",  # 6
        ]
        sources = [0, 1, 2, 3, 3, 4]
        targets = [3, 3, 3, 4, 5, 6]
        values = [30, 30, 30, 90, 60, 60]
        colors_node = [
            "#3B82F6",
            "#8B5CF6",
            "#F59E0B",
            "#0D9488",
            "#10B981",
            "#6366F1",
            "#EC4899",
        ]

        fig_s = go.Figure(
            go.Sankey(
                node=dict(
                    pad=22,
                    thickness=18,
                    line=dict(color="white", width=0.4),
                    label=nodes,
                    color=colors_node,
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color="rgba(13,148,136,0.18)",
                ),
            )
        )
        fig_s.update_layout(
            height=280, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="white"
        )
        st.plotly_chart(
            fig_s, use_container_width=True, config={"displayModeBar": False}
        )
        st.markdown("</div>", unsafe_allow_html=True)

        # Ask HealthGPT chat panel (embedded)
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">💬 Ask HealthGPT</div>', unsafe_allow_html=True
        )

        # Seed default Q/A if available
        if row is not None:
            dq = sg(row, "ask_default_question")
            da = sg(row, "ask_default_answer")
            if dq and da:
                st.markdown(
                    f'<div class="hp-chat-user">{dq}</div>'
                    f'<div class="hp-chat-ai">🤖 {da}</div>',
                    unsafe_allow_html=True,
                )

        # Chat history in session state
        if "hgpt_history" not in st.session_state:
            st.session_state["hgpt_history"] = []

        for msg in st.session_state["hgpt_history"]:
            if msg["role"] == "user":
                st.markdown(
                    f'<div class="hp-chat-user">{msg["text"]}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="hp-chat-ai">🤖 {msg["text"]}</div>',
                    unsafe_allow_html=True,
                )

        user_q = st.text_input(
            "Ask a question about this district...", key="ask_hgpt_input"
        )
        if st.button("Ask", key="ask_hgpt_btn"):
            if user_q.strip():
                # Static demo response
                demo_ans = (
                    f"Based on available data for {district} ({capability}), the gap score "
                    f"is {DEFAULT_STATE} district. Primary drivers include facility inactivity "
                    f"(8/15 facilities zero claims), no OB/GYN on any outer island, and "
                    f"4+ hour boat transit to nearest C-section centre. "
                    f"Recommend field verification within 30 days."
                )
                st.session_state["hgpt_history"].append(
                    {"role": "user", "text": user_q}
                )
                st.session_state["hgpt_history"].append(
                    {"role": "ai", "text": demo_ans}
                )
                st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)

    with c_queue:
        # Review queue
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown('<div class="hp-sec">🔔 Review Queue</div>', unsafe_allow_html=True)
        if row is not None:
            for i in range(1, 5):
                itype = sg(row, f"issue_{i}_type")
                iprio = sg(row, f"issue_{i}_priority")
                idesc = sg(row, f"issue_{i}_description")
                if not itype:
                    continue
                bc = "#EF4444" if iprio == "High" else "#F59E0B"
                pc = "hp-bg-high" if iprio == "High" else "hp-bg-medium"
                st.markdown(
                    f"""
<div style="border-left:3px solid {bc};padding:9px 12px;
            background:white;border-radius:0 8px 8px 0;
            margin-bottom:9px;box-shadow:0 1px 2px rgba(0,0,0,0.05)">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <span style="font-size:13px;font-weight:600;color:#1F2937">{itype}</span>
    <span class="hp-badge {pc}">{iprio}</span>
  </div>
  <div style="font-size:12px;color:#6B7280;margin-top:3px">{idesc}</div>
</div>
                    """,
                    unsafe_allow_html=True,
                )
        else:
            st.info("No queue items found.")
        st.markdown("</div>", unsafe_allow_html=True)

        # Planner workspace
        st.markdown('<div class="hp-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hp-sec">📋 Planner Workspace</div>', unsafe_allow_html=True
        )
        if row is not None:
            for i in range(1, 5):
                note = sg(row, f"planner_note_{i}")
                if note:
                    st.markdown(
                        f'<div style="padding:5px 0;border-bottom:1px solid #F3F4F6;'
                        f'font-size:12px;color:#374151">✏️ {note}</div>',
                        unsafe_allow_html=True,
                    )
        new_note = st.text_area(
            "Add a note...",
            key="arch_note_input",
            height=75,
            label_visibility="collapsed",
            placeholder="Add a note...",
        )
        if st.button("Save Note", key="arch_save_note"):
            if new_note.strip():
                st.success("Note saved to workspace.")
        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# Main entry point
# ============================================================================


def main():
    st.set_page_config(
        page_title="HealthGPT Care Gap Trust Planner",
        page_icon="🏥",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    inject_css()

    if "page" not in st.session_state:
        st.session_state["page"] = "overview"
    if "show_note" not in st.session_state:
        st.session_state["show_note"] = False

    render_sidebar()

    state = st.session_state.get("sel_state", DEFAULT_STATE)
    district = st.session_state.get("sel_district", DEFAULT_DISTRICT)
    capability = st.session_state.get("sel_capability", DEFAULT_CAPABILITY)
    page = st.session_state.get("page", "overview")

    PAGE_MAP = {
        "overview": page_overview,
        "care_map": page_care_map,
        "facilities": page_facilities,
        "evidence_review": page_evidence_review,
        "scenario_planner": page_scenario_planner,
        "architecture_trust": page_architecture_trust,
    }

    fn = PAGE_MAP.get(page, page_overview)
    fn(state, district, capability)


if __name__ == "__main__":
    main()
