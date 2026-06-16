import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
from datetime import datetime
from databricks.sdk import WorkspaceClient

# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER - Lakebase Backend
# ============================================================================
# Backend: Lakebase PostgreSQL
# Host: ep-wild-snow-d8k94scg.database.us-east-2.cloud.databricks.com
# Database: healthgpt
# Tables: care_gap_summary, facility_detail, risk_indicators
# ============================================================================

st.set_page_config(
    page_title="HealthGPT Care Gap Trust Planner",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS - Matching Design Images Exactly
st.markdown('''
<style>
    /* Dark blue sidebar - exact match */
    [data-testid="stSidebar"] {
        background-color: #1e3a52 !important;
    }
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    
    /* Teal accent buttons - matching design */
    .stButton > button {
        background-color: #0d9488;
        color: white;
        border-radius: 8px;
        font-weight: 600;
        padding: 0.6rem 1.2rem;
        border: none;
    }
    
    /* Large score display */
    .score-display {
        background: white;
        padding: 2rem;
        border-radius: 12px;
        border: 1px solid #e5e7eb;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        text-align: center;
    }
    .score-number {
        font-size: 3.5rem;
        font-weight: bold;
        color: #dc2626;
        line-height: 1;
    }
    .score-label {
        font-size: 1.2rem;
        color: #dc2626;
        font-weight: 600;
        background: #fee2e2;
        padding: 0.3rem 0.8rem;
        border-radius: 6px;
        display: inline-block;
        margin-top: 0.5rem;
    }
    
    /* Metric cards - professional design */
    .metric-card {
        background: white;
        padding: 1.8rem;
        border-radius: 12px;
        border: 1px solid #e5e7eb;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    .metric-title {
        font-size: 0.9rem;
        color: #64748b;
        font-weight: 500;
        margin-bottom: 0.5rem;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1e293b;
    }
    
    /* Indicator items */
    .indicator-item {
        display: flex;
        align-items: center;
        padding: 0.8rem;
        margin: 0.5rem 0;
        background: white;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
    }
    .indicator-dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 0.8rem;
    }
    .indicator-text {
        flex: 1;
        font-size: 0.95rem;
        color: #334155;
    }
    .indicator-value {
        font-weight: 700;
        font-size: 1rem;
    }
    .indicator-negative {
        color: #dc2626;
    }
    .indicator-positive {
        color: #0d9488;
    }
    
    /* Action items with checkmarks */
    .action-item {
        display: flex;
        align-items: flex-start;
        padding: 0.8rem;
        margin: 0.5rem 0;
        background: #f0fdf4;
        border-radius: 8px;
        border: 1px solid #bbf7d0;
    }
    .action-check {
        color: #0d9488;
        font-size: 1.2rem;
        margin-right: 0.8rem;
        font-weight: bold;
    }
    .action-text {
        font-size: 0.95rem;
        color: #166534;
        line-height: 1.5;
    }
    
    /* Evidence badges */
    .evidence-badge {
        padding: 0.3rem 0.8rem;
        border-radius: 6px;
        font-size: 0.85rem;
        font-weight: 600;
        display: inline-block;
        margin: 0.2rem;
    }
    .badge-strong {
        background: #d1fae5;
        color: #065f46;
    }
    .badge-partial {
        background: #fed7aa;
        color: #9a3412;
    }
    .badge-weak {
        background: #fecaca;
        color: #991b1b;
    }
    
    /* Decision buttons */
    .decision-button {
        padding: 0.8rem 1.5rem;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.95rem;
        border: none;
        cursor: pointer;
        margin: 0.3rem;
    }
    .btn-accept {
        background: #0d9488;
        color: white;
    }
    .btn-review {
        background: #fb923c;
        color: white;
    }
    .btn-reject {
        background: #dc2626;
        color: white;
    }
    .btn-note {
        background: #3b82f6;
        color: white;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1.3rem;
        font-weight: 700;
        color: #1e293b;
        margin: 1.5rem 0 1rem 0;
        border-bottom: 2px solid #e5e7eb;
        padding-bottom: 0.5rem;
    }
    
    /* Info boxes */
    .info-box {
        background: #f8fafc;
        border-left: 4px solid #0d9488;
        padding: 1.2rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    
    /* Confidence badge */
    .confidence-badge {
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 600;
        display: inline-block;
    }
    .confidence-medium-high {
        background: #fef3c7;
        color: #92400e;
    }
    .confidence-high {
        background: #d1fae5;
        color: #065f46;
    }
</style>
''', unsafe_allow_html=True)

# ============================================================================
# LAKEBASE CONNECTION
# ============================================================================

# Use environment variables injected by Databricks Apps
PGHOST = os.environ.get("PGHOST", "ep-wild-snow-d8k94scg.database.us-east-2.cloud.databricks.com")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "healthgpt")
# Use current user email for PGUSER
if "PGUSER" in os.environ:
    PGUSER = os.environ["PGUSER"]
else:
    # Get current user email from Workspace Client
    try:
        _temp_w = WorkspaceClient()
        _current_user = _temp_w.current_user.me()
        PGUSER = _current_user.user_name
    except:
        # Fallback to service principal if available
        PGUSER = os.environ.get("DATABRICKS_CLIENT_ID") or os.environ.get("DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID", "54db6394-5ecf-408e-a05f-ddb1ba14b4b2")
ENDPOINT_NAME = "projects/hackthon/branches/production/endpoints/primary"

@st.cache_resource(ttl=900)
def get_lakebase_token():
    """Generate Lakebase database credential token via REST API"""
    w = WorkspaceClient()
    try:
        result = w.api_client.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": ENDPOINT_NAME}
        )
        token = result.get("token") or result.get("access_token")
        return token
    except Exception as e:
        st.error(f"Unable to generate database credentials: {e}")
        return None

def get_connection():
    """Connect to Lakebase PostgreSQL - creates a new connection each time"""
    token = get_lakebase_token()
    if not token:
        return None
    try:
        conn = psycopg2.connect(
            host=PGHOST,
            port=int(PGPORT),
            dbname=PGDATABASE,
            user=PGUSER,
            password=token,
            sslmode="require",
            connect_timeout=30
        )
        return conn
    except Exception as e:
        st.error(f"Database connection unavailable. Please contact support.")
        return None

def query_data(query):
    """Execute query with proper connection management"""
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            st.error("Database connection not available")
            return pd.DataFrame()
        
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return pd.DataFrame()
    finally:
        # Always close the connection after query execution
        if conn is not None:
            try:
                conn.close()
            except:
                pass

# ============================================================================
# SIDEBAR
# ============================================================================

st.sidebar.markdown("# 🏥 HealthGPT")
st.sidebar.markdown("**Care Gap Trust Planner**")
st.sidebar.markdown("---")

# Initialize session state for page selection
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Overview"

# Navigation menu items with checkbox style
menu_items = [
    "Overview",
    "Care Map",
    "Facilities",
    "Evidence Review",
    "Scenario Planner",
    "Ask HealthGPT",
    "Architecture / Trust"
]

# Create navigation buttons with checkbox style
for item_name in menu_items:
    is_selected = st.session_state.current_page == item_name
    checkbox_icon = "☑" if is_selected else "☐"
    
    if is_selected:
        # Show as selected with teal background
        st.sidebar.markdown(
            f'<div style="background: #0891b2; padding: 0.8rem 1rem; border-radius: 8px; margin: 0.3rem 0; color: white; font-weight: 500; cursor: pointer;">'
            f'{checkbox_icon} {item_name}'
            f'</div>',
            unsafe_allow_html=True
        )
    else:
        # Show as button
        if st.sidebar.button(f"{checkbox_icon} {item_name}", key=f"nav_{item_name}", use_container_width=True):
            st.session_state.current_page = item_name
            st.rerun()

page = st.session_state.current_page

# Trust First branding at bottom
st.sidebar.markdown(
    '''
    <div style="background: linear-gradient(135deg, #0d9488 0%, #0891b2 100%); padding: 1.5rem; border-radius: 10px; color: white; text-align: center; margin-top: 2rem;">
        <h3 style="margin:0; color:white;">Trust first</h3>
        <p style="margin:0.5rem 0 0 0; font-size:0.9rem; color:white;">
            Evidence-weighted<br/>
            gaps Planner<br/>
            decisions saved
        </p>
    </div>
    ''',
    unsafe_allow_html=True
)
# ============================================================================

# ============================================================================
# FILTER BAR (TOP OF PAGE) - Working filters with proper data binding
# ============================================================================

# Load filter options from database with proper NULL handling
states_df = query_data("SELECT DISTINCT state FROM public.care_gap_summary WHERE state IS NOT NULL ORDER BY state")
capabilities_df = query_data("SELECT DISTINCT capability FROM public.care_gap_summary WHERE capability IS NOT NULL ORDER BY capability")

# Filter bar with proper spacing
st.markdown("### 🔍 Filters")
col1, col2, col3 = st.columns(3)

with col1:
    state_options = ["All States"] + (states_df['state'].tolist() if not states_df.empty else [])
    selected_state = st.selectbox(
        "Select State",
        state_options,
        index=0,
        key="state_filter"
    )

with col2:
    # District filter is cosmetic only (no city data in database)
    st.markdown("<label style='font-size:0.875rem; font-weight:400; color:#31333F;'>District (Display only)</label>", unsafe_allow_html=True)
    st.markdown("<div style='background:#f1f5f9; padding:0.5rem 0.75rem; border-radius:0.375rem; color:#64748b; margin-top:0.25rem;'>Nicobars</div>", unsafe_allow_html=True)

with col3:
    capability_options = ["All Capabilities"] + (capabilities_df['capability'].tolist() if not capabilities_df.empty else [])
    selected_capability = st.selectbox(
        "Select Capability",
        capability_options,
        index=0,
        key="capability_filter"
    )

# Build WHERE clause based on filters - CASE INSENSITIVE
where_clauses = []
if selected_state and selected_state != "All States":
    # Use UPPER for case-insensitive comparison
    where_clauses.append(f"UPPER(state) = UPPER('{selected_state}')")
if selected_capability and selected_capability != "All Capabilities":
    # Use LOWER for case-insensitive comparison
    where_clauses.append(f"LOWER(capability) = LOWER('{selected_capability}')")

where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""

st.markdown("---")

# PAGE 1: OVERVIEW DASHBOARD - REDESIGNED TO MATCH DESIGN IMAGE
# ============================================================================

if page == "Overview":
    # Generate Brief button aligned right
    col_spacer, col_button = st.columns([5, 1])
    with col_button:
        if st.button("Generate Brief", use_container_width=True, type="primary"):
            st.success("Brief generated!")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Query data for top metrics based on filters
    gap_metrics_query = f'''
        SELECT 
            AVG(gap_score) as avg_gap_score,
            confidence_level,
            SUM(weak_count) as total_weak,
            SUM(partial_count) as total_partial,
            SUM(strong_count) as total_strong,
            COUNT(*) as total_records
        FROM public.care_gap_summary
        WHERE 1=1 {where_clause}
        GROUP BY confidence_level
        ORDER BY COUNT(*) DESC
        LIMIT 1
    '''
    metrics_df = query_data(gap_metrics_query)
    
    # Calculate metrics with fallback
    if not metrics_df.empty:
        avg_score = int(metrics_df['avg_gap_score'].iloc[0])
        confidence = metrics_df['confidence_level'].iloc[0] if 'confidence_level' in metrics_df.columns else "MEDIUM"
        weak_count = int(metrics_df['total_weak'].iloc[0]) if metrics_df['total_weak'].iloc[0] else 0
        partial_count = int(metrics_df['total_partial'].iloc[0]) if metrics_df['total_partial'].iloc[0] else 0
        strong_count = int(metrics_df['total_strong'].iloc[0]) if metrics_df['total_strong'].iloc[0] else 0
        
        # Determine gap severity
        if avg_score >= 70:
            gap_label = "HIGH GAP"
            gap_color = "#dc2626"
        elif avg_score >= 50:
            gap_label = "MEDIUM GAP"
            gap_color = "#f59e0b"
        else:
            gap_label = "LOW GAP"
            gap_color = "#0d9488"
        
        # Map confidence to display
        confidence_display = "Medium-High" if confidence == "MEDIUM" else confidence.title()
    else:
        avg_score = 82
        gap_label = "HIGH GAP"
        gap_color = "#dc2626"
        confidence_display = "Medium-High"
        weak_count = 4
        partial_count = 4
        strong_count = 1
    
    # Top row: Care Gap Score, Confidence, Facility Evidence, AI Brief Summary
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    
    # CARE GAP SCORE (Large prominent display - DYNAMIC)
    with col1:
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-title">Care Gap Score</div>
            <div class="score-number" style="color:{gap_color};">{avg_score}<span style="font-size:2rem; color:#94a3b8;">/100</span></div>
            <div class="score-label" style="color:{gap_color}; background: {gap_color}22;">{gap_label}</div>
        </div>
        ''', unsafe_allow_html=True)
    
    # CONFIDENCE (Interactive Gauge - DYNAMIC)
    with col2:
        st.markdown('''
        <div class="metric-card">
            <div class="metric-title">Confidence</div>
        ''', unsafe_allow_html=True)
        
        # Map confidence level to percentage for gauge
        confidence_map = {
            "LOW": 25,
            "MEDIUM": 60,
            "MEDIUM-HIGH": 75,
            "HIGH": 90
        }
        
        # Get confidence value (default to MEDIUM if not found)
        conf_upper = confidence.upper() if confidence else "MEDIUM"
        conf_value = confidence_map.get(conf_upper, 60)
        
        # Create gauge chart using plotly
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=conf_value,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "", 'font': {'size': 14}},
            number={'suffix': "%", 'font': {'size': 24, 'color': '#1e293b'}},
            gauge={
                'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "#e5e7eb"},
                'bar': {'color': "#0891b2", 'thickness': 0.75},
                'bgcolor': "white",
                'borderwidth': 2,
                'bordercolor': "#e5e7eb",
                'steps': [
                    {'range': [0, 33], 'color': '#fee2e2'},
                    {'range': [33, 66], 'color': '#fef3c7'},
                    {'range': [66, 100], 'color': '#d1fae5'}
                ],
                'threshold': {
                    'line': {'color': "#0891b2", 'width': 4},
                    'thickness': 0.75,
                    'value': conf_value
                }
            }
        ))
        
        fig_gauge.update_layout(
            height=180,
            margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor='white',
            font={'color': "#64748b", 'family': "Arial"}
        )
        
        st.plotly_chart(fig_gauge, use_container_width=True)
        
        st.markdown(f'''
            <div style="text-align:center; font-weight:600; color:#1e293b; margin-top:-1rem;">{confidence_display}</div>
        </div>
        ''', unsafe_allow_html=True)
    
    # FACILITY EVIDENCE (Donut Chart - DYNAMIC)
    with col3:
        st.markdown('''
        <div class="metric-card">
            <div class="metric-title">Facility Evidence</div>
        ''', unsafe_allow_html=True)
        
        # Create a donut chart for facility evidence
        evidence_labels = ['Missing', 'Weak/Suspicious', 'Medium']
        evidence_values = [4, 4, 1]  # Default values matching design
        evidence_colors = ['#ef4444', '#f97316', '#fbbf24']
        
        # Try to use real data if available
        if weak_count > 0 or partial_count > 0 or strong_count > 0:
            evidence_labels = ['Weak', 'Partial', 'Strong']
            evidence_values = [weak_count, partial_count, strong_count]
            evidence_colors = ['#ef4444', '#f97316', '#10b981']
        
        fig_evidence = go.Figure(data=[go.Pie(
            labels=evidence_labels,
            values=evidence_values,
            hole=0.6,
            marker=dict(
                colors=evidence_colors,
                line=dict(color='white', width=2)
            ),
            textinfo='label+value',
            textposition='auto',
            textfont=dict(size=11, color='white'),
            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>',
            direction='clockwise',
            sort=False
        )])
        
        # Add center text showing total
        total_facilities = sum(evidence_values)
        
        fig_evidence.update_layout(
            height=200,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor='white',
            showlegend=False,
            annotations=[dict(
                text=f'<b>{total_facilities}</b><br><span style="font-size:11px; color:#64748b;">Total</span>',
                x=0.5, y=0.5,
                font_size=20,
                showarrow=False,
                font=dict(color='#1e293b')
            )]
        )
        
        st.plotly_chart(fig_evidence, use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # AI BRIEF SUMMARY (Text summary - DYNAMIC based on filters)
    with col4:
        # Generate dynamic summary based on selected filters
        state_text = selected_state if selected_state != "All States" else "the selected region"
        capability_text = selected_capability if selected_capability != "All Capabilities" else "healthcare services"
        
        if avg_score >= 70:
            summary = f"{state_text} shows a <strong>significant {capability_text} gap</strong> (score: {avg_score}). "
            if weak_count > strong_count:
                summary += f"Only {strong_count} facilities have strong evidence vs {weak_count} weak. "
            summary += "<strong>Urgent action needed</strong> to address service gaps and improve facility capabilities."
        elif avg_score >= 50:
            summary = f"{state_text} has a <strong>moderate {capability_text} gap</strong> (score: {avg_score}). "
            summary += f"Evidence quality is mixed: {strong_count} strong, {partial_count} partial, {weak_count} weak facilities. "
            summary += "Targeted interventions recommended."
        else:
            summary = f"{state_text} shows <strong>relatively good {capability_text} coverage</strong> (score: {avg_score}). "
            summary += f"Most facilities have adequate evidence. Continue monitoring and maintain service quality."
        
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-title">AI Brief Summary</div>
            <div style="margin-top:1rem; font-size:0.95rem; color:#475569; line-height:1.6;">
                {summary}
            </div>
        </div>
        ''', unsafe_allow_html=True)
    
    st.markdown("<div style='margin:1.5rem 0;'></div>", unsafe_allow_html=True)
    
    # Middle section: Top At-Risk Indicators & Recommended Actions
    col_left, col_right = st.columns([1, 1])
    
    # TOP AT-RISK INDICATORS (Left)
    with col_left:
        st.markdown('''
        <div class="metric-card">
            <div style="font-size:1.1rem; font-weight:600; color:#1e293b; margin-bottom:1rem;">Top At-Risk Indicators</div>
        ''', unsafe_allow_html=True)
        
        # Query risk indicators - NO WHERE CLAUSE (global metrics)
        risk_query = '''
            SELECT indicator_name, risk_pct, priority 
            FROM public.risk_indicators 
            ORDER BY ABS(risk_pct) DESC LIMIT 4
        '''
        risk_df = query_data(risk_query)
        
        if not risk_df.empty:
            for _, row in risk_df.iterrows():
                is_negative = row['risk_pct'] < 0
                color_class = "indicator-negative" if is_negative else "indicator-positive"
                sign = "" if is_negative else "+"
                st.markdown(f'''
                <div class="indicator-item">
                    <span class="indicator-dot" style="background:{'#dc2626' if is_negative else '#f59e0b'};"></span>
                    <span class="indicator-text">{row['indicator_name']}</span>
                    <span class="indicator-value {color_class}">{sign}{row['risk_pct']:.0f} pp</span>
                </div>
                ''', unsafe_allow_html=True)
        else:
            # Fallback static data
            st.markdown('''
            <div class="indicator-item">
                <span class="indicator-dot" style="background:#dc2626;"></span>
                <span class="indicator-text">ANC coverage < 4 visits</span>
                <span class="indicator-value indicator-negative">34 pp</span>
            </div>
            <div class="indicator-item">
                <span class="indicator-dot" style="background:#f59e0b;"></span>
                <span class="indicator-text">Institutional delivery 10%</span>
                <span class="indicator-value indicator-negative">-29 pp</span>
            </div>
            <div class="indicator-item">
                <span class="indicator-dot" style="background:#f59e0b;"></span>
                <span class="indicator-text">Anemia women 13-49%</span>
                <span class="indicator-value indicator-positive">+32 pp</span>
            </div>
            <div class="indicator-item">
                <span class="indicator-dot" style="background:#dc2626;"></span>
                <span class="indicator-text">Strong maternity confidence</span>
                <span class="indicator-value indicator-negative">Low</span>
            </div>
            ''', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # RECOMMENDED ACTIONS (Right)
    with col_right:
        st.markdown('''
        <div class="metric-card">
            <div style="font-size:1.1rem; font-weight:600; color:#1e293b; margin-bottom:1rem;">Recommended Actions</div>
            <div class="action-item">
                <span class="action-check">✓</span>
                <span class="action-text">Verify 3 weak maternity claims</span>
            </div>
            <div class="action-item">
                <span class="action-check">✓</span>
                <span class="action-text">Deploy mobile ANC outreach</span>
            </div>
            <div class="action-item">
                <span class="action-check">✓</span>
                <span class="action-text">Map emergency obstetric referral</span>
            </div>
            <div class="action-item">
                <span class="action-check">✓</span>
                <span class="action-text">Improve anemia screening & treatment</span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
    
    st.markdown("<div style='margin:1.5rem 0;'></div>", unsafe_allow_html=True)
    
    # PLANNER DECISION SNAPSHOT (Bottom section)
    st.markdown('''
    <div class="metric-card">
        <div style="font-size:1.1rem; font-weight:600; color:#1e293b; margin-bottom:1rem;">Planner Decision Snapshot</div>
        <div style="font-size:0.95rem; color:#475569; line-height:1.6;">
            <strong>Decision:</strong> likely real maternity care desert. <strong>Prioritize evidence review + outreach</strong> before capital investment.<br/>
            <em style="color:#64748b;">Source notes: websites, clinician attestations, and shortlist can be previewed in Evidence tab.</em>
        </div>
    </div>
    ''', unsafe_allow_html=True)

# ============================================================================
# PAGE 2: CARE MAP - Matching Design Image Exactly
# ============================================================================

elif page == "Care Map":
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("Refresh Map", use_container_width=True, type="primary"):
            st.success("Map refreshed!")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Main layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<div class="section-header">Care Gap Map</div>', unsafe_allow_html=True)
        
        # Map Layers Legend
        st.markdown('''
        <div style="background: white; padding: 1rem; border-radius: 10px; border: 1px solid #e5e7eb; margin-bottom: 1rem;">
            <div style="font-weight: 600; margin-bottom: 0.8rem;">Map Layers</div>
            <div style="display: flex; align-items: center; margin: 0.4rem 0;">
                <div style="width: 12px; height: 12px; background: #0d9488; border-radius: 50%; margin-right: 0.6rem;"></div>
                <span style="font-size: 0.9rem;">Strong evidence</span>
            </div>
            <div style="display: flex; align-items: center; margin: 0.4rem 0;">
                <div style="width: 12px; height: 12px; background: #f59e0b; border-radius: 50%; margin-right: 0.6rem;"></div>
                <span style="font-size: 0.9rem;">Partial evidence</span>
            </div>
            <div style="display: flex; align-items: center; margin: 0.4rem 0;">
                <div style="width: 12px; height: 12px; background: #dc2626; border-radius: 50%; margin-right: 0.6rem;"></div>
                <span style="font-size: 0.9rem;">Weak claim</span>
            </div>
            <div style="display: flex; align-items: center; margin: 0.4rem 0;">
                <div style="width: 12px; height: 12px; background: #94a3b8; border-radius: 50%; margin-right: 0.6rem;"></div>
                <span style="font-size: 0.9rem;">No claim</span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        
        # Simplified map visualization (placeholder for geographic visualization)
        st.markdown('''
        <div style="background: #f1f5f9; padding: 2rem; border-radius: 12px; height: 500px; border: 1px solid #cbd5e1;">
            <div style="text-align: center; padding-top: 180px; color: #64748b;">
                <div style="font-size: 3rem; margin-bottom: 1rem;">🗺️</div>
                <div style="font-size: 1.1rem; font-weight: 600;">Geographic Care Gap Map</div>
                <div style="font-size: 0.9rem; margin-top: 0.5rem;">Interactive map showing facility evidence by location</div>
            </div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="section-header">Geography Summary</div>', unsafe_allow_html=True)
        
        st.markdown('''
        <div class="metric-card" style="margin-bottom: 1rem;">
            <div class="metric-title">PIN codes flagged</div>
            <div class="metric-value">7</div>
        </div>
        <div class="metric-card" style="margin-bottom: 1rem;">
            <div class="metric-title">Strong maternity sites</div>
            <div class="metric-value">1</div>
        </div>
        <div class="metric-card" style="margin-bottom: 1rem;">
            <div class="metric-title">Weak/suspicious claims</div>
            <div class="metric-value">4</div>
        </div>
        <div class="metric-card" style="margin-bottom: 1rem;">
            <div class="metric-title">Estimated travel gap</div>
            <div class="metric-value" style="color: #dc2626;">&gt; 60 min</div>
        </div>
        <div class="metric-card" style="margin-bottom: 1rem;">
            <div class="metric-title">Data completeness</div>
            <div class="metric-value">Medium</div>
        </div>
        ''', unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-header">Facilities to Review First</div>', unsafe_allow_html=True)
        
        st.markdown('''
        <div style="background: white; padding: 1rem; border-radius: 10px; border: 1px solid #e5e7eb; margin-bottom: 0.8rem;">
            <div style="font-weight: 600; color: #1e293b;">Nico Island Hospital</div>
            <div style="font-size: 0.85rem; color: #f59e0b; margin-top: 0.3rem;">Partial evidence - Missing equipment detail</div>
        </div>
        <div style="background: white; padding: 1rem; border-radius: 10px; border: 1px solid #e5e7eb; margin-bottom: 0.8rem;">
            <div style="font-weight: 600; color: #1e293b;">Bay Clinic</div>
            <div style="font-size: 0.85rem; color: #dc2626; margin-top: 0.3rem;">Weak claim - No source support</div>
        </div>
        <div style="background: white; padding: 1rem; border-radius: 10px; border: 1px solid #e5e7eb; margin-bottom: 0.8rem;">
            <div style="font-weight: 600; color: #1e293b;">Coastal Care Center</div>
            <div style="font-size: 0.85rem; color: #64748b; margin-top: 0.3rem;">Suspicious - Maternity t no doctors</div>
        </div>
        ''', unsafe_allow_html=True)

# ============================================================================
# PAGE 3: FACILITIES - Facility Detail View Matching Design
# ============================================================================

elif page == "Facilities":
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("💾 Save Review", use_container_width=True):
            st.success("Review saved!")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    st.markdown('<div class="section-header">Facility Evidence - Detail View</div>', unsafe_allow_html=True)
    st.markdown('<h2 style="color: #1e293b; margin-top: 0.5rem;">Nicobar Island Hospital</h2>', unsafe_allow_html=True)
    
    st.markdown('''
    <div style="margin: 1rem 0;">
        <span class="evidence-badge" style="background: #dbeafe; color: #1e40af; font-size: 0.95rem;">Claimed capability: Maternity Care</span>
        <span class="evidence-badge" style="background: #fed7aa; color: #9a3412; font-size: 0.95rem;">Trust signal: Partial Evidence</span>
    </div>
    ''', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown('<div class="section-header">Evidence Breakdown</div>', unsafe_allow_html=True)
        
        st.markdown('''
        <div style="background: white; padding: 1.5rem; border-radius: 10px; border: 1px solid #e5e7eb;">
            <div style="display: flex; align-items: center; padding: 0.8rem 0; border-bottom: 1px solid #f1f5f9;">
                <div style="width: 12px; height: 12px; background: #0d9488; border-radius: 50%; margin-right: 0.8rem;"></div>
                <div style="flex: 1; font-size: 0.95rem;">Description mentions maternity delivery</div>
                <div class="evidence-badge badge-strong">Strong</div>
            </div>
            <div style="display: flex; align-items: center; padding: 0.8rem 0; border-bottom: 1px solid #f1f5f9;">
                <div style="width: 12px; height: 12px; background: #0d9488; border-radius: 50%; margin-right: 0.8rem;"></div>
                <div style="flex: 1; font-size: 0.95rem;">Specialties include Obstetrics</div>
                <div class="evidence-badge badge-strong">Strong</div>
            </div>
            <div style="display: flex; align-items: center; padding: 0.8rem 0; border-bottom: 1px solid #f1f5f9;">
                <div style="width: 12px; height: 12px; background: #f59e0b; border-radius: 50%; margin-right: 0.8rem;"></div>
                <div style="flex: 1; font-size: 0.95rem;">Procedure list contains C-section</div>
                <div class="evidence-badge badge-partial">Partial</div>
            </div>
            <div style="display: flex; align-items: center; padding: 0.8rem 0; border-bottom: 1px solid #f1f5f9;">
                <div style="width: 12px; height: 12px; background: #dc2626; border-radius: 50%; margin-right: 0.8rem;"></div>
                <div style="flex: 1; font-size: 0.95rem;">Equipment field lacks incubator/OT detail</div>
                <div class="evidence-badge badge-weak">Weak</div>
            </div>
            <div style="display: flex; align-items: center; padding: 0.8rem 0; border-bottom: 1px solid #f1f5f9;">
                <div style="width: 12px; height: 12px; background: #dc2626; border-radius: 50%; margin-right: 0.8rem;"></div>
                <div style="flex: 1; font-size: 0.95rem;">Doctor count missing</div>
                <div class="evidence-badge badge-weak">Weak</div>
            </div>
            <div style="display: flex; align-items: center; padding: 0.8rem 0;">
                <div style="width: 12px; height: 12px; background: #0d9488; border-radius: 50%; margin-right: 0.8rem;"></div>
                <div style="flex: 1; font-size: 0.95rem;">Source URL available</div>
                <div class="evidence-badge badge-strong">Strong</div>
            </div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="section-header">Underlying Text Evidence</div>', unsafe_allow_html=True)
        
        st.markdown('''
        <div style="background: white; padding: 1.5rem; border-radius: 10px; border: 1px solid #e5e7eb;">
            <div style="margin-bottom: 1.5rem;">
                <div style="font-weight: 600; color: #0d9488; margin-bottom: 0.5rem;">description</div>
                <div style="font-size: 0.9rem; color: #334155; font-style: italic;">
                    "Provides maternal care, delivery support, and emergency consultation."
                </div>
            </div>
            <div style="margin-bottom: 1.5rem;">
                <div style="font-weight: 600; color: #0d9488; margin-bottom: 0.5rem;">specialties</div>
                <div style="font-size: 0.9rem; color: #334155;">
                    Obstetrics, General Medicine, Pediatrics
                </div>
            </div>
            <div style="margin-bottom: 1.5rem;">
                <div style="font-weight: 600; color: #0d9488; margin-bottom: 0.5rem;">equipment</div>
                <div style="font-size: 0.9rem; color: #334155;">
                    Basic beds, oxygen; no OT / neonatal equipment listed
                </div>
            </div>
            <div>
                <div style="font-weight: 600; color: #0d9488; margin-bottom: 0.5rem;">source_urls</div>
                <div style="font-size: 0.9rem; color: #334155;">
                    1 source available for manual verification
                </div>
            </div>
        </div>
        ''', unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Planner Review Decision</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("✓ Accept claim", use_container_width=True, key="accept"):
            st.success("Claim accepted")
    with col2:
        if st.button("⚠️ Needs review", use_container_width=True, key="review"):
            st.warning("Flagged for review")
    with col3:
        if st.button("✗ Reject claim", use_container_width=True, key="reject"):
            st.error("Claim rejected")
    with col4:
        if st.button("📝 Add note", use_container_width=True, key="note"):
            st.info("Note added")
    
    st.markdown('''
    <div class="info-box" style="margin-top: 1.5rem;">
        <strong>Suggested note:</strong> Partial evidence supports basic maternity services, but emergency obstetric capability requires manual verification.
    </div>
    ''', unsafe_allow_html=True)

# ============================================================================
# PAGE 4: EVIDENCE REVIEW - AI Gap Brief Matching Design
# ============================================================================

elif page == "Evidence Review":
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("💾 Download Brief", use_container_width=True):
            st.success("Brief downloaded!")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    st.markdown('<div class="section-header">AI Gap Brief - Planner Ready Output</div>', unsafe_allow_html=True)
    st.markdown('<h2 style="color: #1e293b; margin-top: 0.5rem;">Care Gap: Maternity Desert in Nicobars</h2>', unsafe_allow_html=True)
    
    st.markdown('''
    <div style="margin: 1rem 0;">
        <span class="confidence-badge confidence-medium-high">Confidence: Medium-High</span>
        <span class="evidence-badge badge-strong" style="margin-left: 0.5rem;">Evidence-backed</span>
        <span class="evidence-badge" style="background: #fef3c7; color: #92400e; margin-left: 0.5rem;">Uncertainty disclosed</span>
    </div>
    ''', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown('<div class="section-header">Why this is likely real</div>', unsafe_allow_html=True)
        
        st.markdown('''
        <div style="background: white; padding: 1.5rem; border-radius: 10px; border: 1px solid #e5e7eb;">
            <div style="display: flex; align-items: flex-start; margin-bottom: 1rem;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">1</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    Low ANC coverage and high anemia burden point to unmet need.
                </div>
            </div>
            <div style="display: flex; align-items: flex-start; margin-bottom: 1rem;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">2</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    Only one facility has strong or partial maternity evidence.
                </div>
            </div>
            <div style="display: flex; align-items: flex-start; margin-bottom: 1rem;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">3</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    No strong evidence of emergency obstetric capability in district.
                </div>
            </div>
            <div style="display: flex; align-items: flex-start;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">4</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    Multiple facility claims are weak, incomplete, or suspicious.
                </div>
            </div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="section-header">Recommended Actions</div>', unsafe_allow_html=True)
        
        st.markdown('''
        <div style="background: white; padding: 1.5rem; border-radius: 10px; border: 1px solid #e5e7eb;">
            <div style="display: flex; align-items: flex-start; margin-bottom: 1rem;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">1</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    Verify top 3 weak maternity claims using source URLs.
                </div>
            </div>
            <div style="display: flex; align-items: flex-start; margin-bottom: 1rem;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">2</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    Deploy mobile ANC outreach to high-gap PIN codes.
                </div>
            </div>
            <div style="display: flex; align-items: flex-start; margin-bottom: 1rem;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">3</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    Create referral shortlist for emergency obstetric care.
                </div>
            </div>
            <div style="display: flex; align-items: flex-start;">
                <div style="background: #0d9488; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; flex-shrink: 0; margin-right: 1rem;">4</div>
                <div style="font-size: 0.95rem; color: #334155; line-height: 1.6;">
                    Track whether data improvements reduce gap score.
                </div>
            </div>
        </div>
        ''', unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Expected Impact & Confidence Shift</div>', unsafe_allow_html=True)
    
    impact_data = {
        'Metric': ['Gap Score', 'Confidence', 'Strong evidence facilities', 'Facilities needing review'],
        'Current': ['82', 'Medium', '1', '7'],
        'After': ['58', 'High', '3', '3'],
        'Change': ['-24', 'Improves', '+2', '-4']
    }
    impact_df = pd.DataFrame(impact_data)
    
    st.markdown('''
    <style>
        .impact-table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }
        .impact-table th {
            background: #f8fafc;
            padding: 1rem;
            text-align: left;
            font-weight: 600;
            color: #1e293b;
            border-bottom: 2px solid #e5e7eb;
        }
        .impact-table td {
            padding: 1rem;
            border-bottom: 1px solid #f1f5f9;
            color: #334155;
        }
        .change-positive {
            color: #0d9488;
            font-weight: 600;
        }
        .change-negative {
            color: #dc2626;
            font-weight: 600;
        }
    </style>
    ''', unsafe_allow_html=True)
    
    st.dataframe(
        impact_df,
        use_container_width=True,
        hide_index=True
    )
    
    st.markdown('''
    <div class="info-box" style="margin-top: 1.5rem;">
        <strong>Guardrail:</strong> This is a planning brief, not a clinical recommendation. Every ranking should show underlying facility text, trust score, and uncertainty.
    </div>
    ''', unsafe_allow_html=True)

# ============================================================================
# PAGE 5: MODEL INSIGHTS
# ============================================================================

elif page == "Scenario Planner":
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("▶️ Run Scenario", use_container_width=True):
            st.success("Scenario updated!")
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Scenario Planner - What If We Close the Gap?</div>', unsafe_allow_html=True)
    
    st.info("Interactive scenario planning tool - adjust interventions to simulate outcomes.")
    
    # Model performance metrics
    st.markdown("### 🎯 Dual Scoring System Performance")
    
    col1, col2, col3 = st.columns(3)
    
    agreement_query = f'''
        SELECT dual_score_agreement, COUNT(*) as count
        FROM public.facility_detail
        WHERE 1=1
        GROUP BY dual_score_agreement
    '''
    agreement_df = query_data(agreement_query)
    
    if not agreement_df.empty:
        with col1:
            agree_count = agreement_df[agreement_df['dual_score_agreement'] == 'agree']['count'].sum()
            total = agreement_df['count'].sum()
            agree_pct = (agree_count / total * 100) if total > 0 else 0
            st.metric("Rule-ML Agreement", f"{agree_pct:.1f}%")
        
        with col2:
            high_conf = query_data("SELECT COUNT(*) as count FROM public.facility_detail WHERE ml_capability_confidence = 'high'")
            if not high_conf.empty:
                st.metric("High Confidence Predictions", f"{int(high_conf['count'].iloc[0]):,}")
        
        with col3:
            review_needed = query_data("SELECT COUNT(*) as count FROM public.facility_detail WHERE review_priority = 'urgent'")
            if not review_needed.empty:
                st.metric("Facilities Needing Review", f"{int(review_needed['count'].iloc[0]):,}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Confidence Level Distribution")
        confidence_query = '''
            SELECT ml_capability_confidence, COUNT(*) as count
            FROM public.facility_detail
            GROUP BY ml_capability_confidence
        '''
        confidence_df = query_data(confidence_query)
        
        if not confidence_df.empty:
            fig = px.pie(
                confidence_df,
                values='count',
                names='ml_capability_confidence',
                title='ML Model Confidence Distribution',
                color='ml_capability_confidence',
                color_discrete_map={'high': '#4caf50', 'medium': '#ff9800', 'low': '#f44336'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### Score Divergence Analysis")
        divergence_query = '''
            SELECT score_divergence, COUNT(*) as count
            FROM public.facility_detail
            GROUP BY score_divergence
            ORDER BY count DESC
        '''
        divergence_df = query_data(divergence_query)
        
        if not divergence_df.empty:
            fig = px.bar(
                divergence_df,
                x='score_divergence',
                y='count',
                title='Rule vs ML Score Divergence',
                color='count',
                color_continuous_scale='Oranges'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.markdown("### Gap Severity Prediction Accuracy")
    
    score_query = f'''
        SELECT gap_severity, confidence_level, COUNT(*) as count, AVG(gap_score) as avg_score
        FROM public.care_gap_summary
        WHERE 1=1 {where_clause}
        GROUP BY gap_severity, confidence_level
        ORDER BY avg_score DESC
    '''
    score_df = query_data(score_query)
    
    if not score_df.empty:
        fig = px.sunburst(
            score_df,
            path=['gap_severity', 'confidence_level'],
            values='count',
            title='Gap Severity by Confidence Level',
            color='avg_score',
            color_continuous_scale='RdYlGn_r'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(
            score_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "avg_score": st.column_config.NumberColumn("Avg Score", format="%.2f")
            }
        )

# ============================================================================
# PAGE 6: ACTION PLANNER
# ============================================================================

elif page == "Architecture / Trust":
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.success("Refreshed!")
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Trust Architecture & Data Readiness</div>', unsafe_allow_html=True)
    
    st.markdown("### Architecture Flow")
    
    urgent_query = f'''
        SELECT state, capability, gap_score, gap_severity, intervention_urgency,
               total_facilities, weak_count, underserved_pop, supply_adequacy
        FROM public.care_gap_summary
        WHERE intervention_urgency = 'URGENT' {where_clause}
        ORDER BY gap_score DESC
        LIMIT 30
    '''
    urgent_df = query_data(urgent_query)
    
    if not urgent_df.empty:
        st.warning(f"⚠️ {len(urgent_df)} gaps require URGENT intervention")
        
        # Priority matrix
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Intervention Priority Matrix")
            fig = px.scatter(
                urgent_df,
                x='gap_score',
                y='underserved_pop',
                size='total_facilities',
                color='capability',
                hover_name='state',
                title='Priority: Gap Score vs Underserved Population',
                labels={'gap_score': 'Gap Score', 'underserved_pop': 'Underserved Population'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Intervention Urgency by Capability")
            urgency_by_cap = urgent_df.groupby('capability').size().reset_index(name='count')
            fig = px.bar(
                urgency_by_cap,
                x='capability',
                y='count',
                title='Urgent Interventions by Healthcare Capability',
                color='count',
                color_continuous_scale='Reds'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### Action Items")
        st.dataframe(
            urgent_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "gap_score": st.column_config.ProgressColumn(
                    "Gap Score",
                    format="%.2f",
                    min_value=0,
                    max_value=100,
                ),
                "underserved_pop": st.column_config.NumberColumn(
                    "Underserved Pop",
                    format="%d"
                )
            }
        )
    else:
        st.success("✅ No urgent interventions required based on current filters.")
    
    st.markdown("---")
    st.markdown("### 📊 Resource Allocation Recommendations")
    
    resource_query = f'''
        SELECT state, SUM(weak_count) as total_weak_facilities,
               SUM(underserved_pop) as total_underserved,
               AVG(gap_score) as avg_gap_score
        FROM public.care_gap_summary
        WHERE gap_severity IN ('CRITICAL', 'HIGH') {where_clause}
        GROUP BY state
        ORDER BY total_weak_facilities DESC
        LIMIT 15
    '''
    resource_df = query_data(resource_query)
    
    if not resource_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                resource_df,
                x='state',
                y='total_weak_facilities',
                title='States with Most Weak Facilities',
                color='avg_gap_score',
                color_continuous_scale='Reds',
                labels={'total_weak_facilities': 'Weak Facilities', 'state': 'State'}
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Resource Priority Ranking")
            st.dataframe(
                resource_df.head(10),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "total_weak_facilities": st.column_config.NumberColumn("Weak Facilities", format="%d"),
                    "total_underserved": st.column_config.NumberColumn("Underserved Pop", format="%d"),
                    "avg_gap_score": st.column_config.NumberColumn("Avg Gap", format="%.2f")
                }
            )

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(
    "<div style='text-align: center;'>"
    "<p>HealthGPT Care Gap Trust Planner | Powered by Databricks Lakebase PostgreSQL | "
    f"Data refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
    "</div>",
    unsafe_allow_html=True
)