import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from datetime import datetime
import os

# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER
# Exact Design Match - Based on app design.png
# ============================================================================

st.set_page_config(
    page_title="HealthGPT Care Gap Trust Planner",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"  # Hide sidebar for clean design
)

# ============================================================================
# CUSTOM CSS - EXACT DESIGN MATCH
# ============================================================================

st.markdown("""
<style>
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Main container */
    .main .block-container {
        padding-top: 0rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 100%;
    }
    
    /* Header section with gradient */
    .app-header {
        background: linear-gradient(135deg, #1e88e5 0%, #1976d2 50%, #1565c0 100%);
        padding: 2rem 3rem;
        margin: -3rem -3rem 2rem -3rem;
        border-radius: 0 0 15px 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .app-logo {
        font-size: 2.5rem;
        font-weight: 800;
        color: white;
        display: inline-block;
        margin-bottom: 0.5rem;
    }
    
    .app-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: white;
        margin: 0.5rem 0;
        text-align: center;
    }
    
    .app-subtitle {
        font-size: 1.1rem;
        color: rgba(255, 255, 255, 0.9);
        text-align: center;
        font-weight: 400;
        margin-top: 0.3rem;
    }
    
    /* Navigation tabs */
    .nav-tabs {
        display: flex;
        justify-content: center;
        gap: 0.5rem;
        margin: 2rem 0 1.5rem 0;
        flex-wrap: wrap;
    }
    
    .nav-tab {
        padding: 0.8rem 1.5rem;
        background: white;
        border: 2px solid #e0e0e0;
        border-radius: 25px;
        cursor: pointer;
        transition: all 0.3s;
        font-weight: 600;
        color: #424242;
        text-align: center;
    }
    
    .nav-tab:hover {
        border-color: #1976d2;
        background: #e3f2fd;
        color: #1976d2;
    }
    
    .nav-tab-active {
        background: #1976d2;
        color: white;
        border-color: #1976d2;
    }
    
    /* KPI Cards */
    .kpi-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-left: 4px solid #1976d2;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 800;
        color: #1976d2;
        margin: 0.5rem 0;
    }
    
    .kpi-label {
        font-size: 0.95rem;
        color: #757575;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .kpi-icon {
        font-size: 2rem;
        margin-bottom: 0.5rem;
    }
    
    /* Content cards */
    .content-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin-bottom: 1.5rem;
    }
    
    .card-title {
        font-size: 1.4rem;
        font-weight: 700;
        color: #212121;
        margin-bottom: 1rem;
        padding-bottom: 0.8rem;
        border-bottom: 2px solid #e0e0e0;
    }
    
    /* Data table styling */
    .dataframe {
        font-size: 0.9rem;
    }
    
    .dataframe thead th {
        background-color: #1976d2 !important;
        color: white !important;
        font-weight: 600;
        padding: 0.8rem !important;
    }
    
    .dataframe tbody tr:nth-child(even) {
        background-color: #f5f5f5;
    }
    
    .dataframe tbody tr:hover {
        background-color: #e3f2fd;
    }
    
    /* Footer */
    .app-footer {
        text-align: center;
        padding: 2rem 0 1rem 0;
        color: #757575;
        font-size: 0.9rem;
        border-top: 1px solid #e0e0e0;
        margin-top: 3rem;
    }
    
    /* Filters sidebar */
    .filter-section {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin-bottom: 1.5rem;
    }
    
    .filter-title {
        font-size: 1.1rem;
        font-weight: 700;
        color: #212121;
        margin-bottom: 1rem;
    }
    
    /* Severity badges */
    .badge-critical {
        background-color: #d32f2f;
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .badge-high {
        background-color: #f57c00;
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .badge-moderate {
        background-color: #fbc02d;
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_connection():
    """Connect to Databricks SQL Warehouse"""
    try:
        conn = sql.connect(
            server_hostname=st.secrets.get("databricks_host", os.environ.get("DATABRICKS_SERVER_HOSTNAME")),
            http_path=st.secrets.get("databricks_http_path", os.environ.get("DATABRICKS_HTTP_PATH")),
            access_token=st.secrets.get("databricks_token", os.environ.get("DATABRICKS_TOKEN"))
        )
        return conn
    except Exception as e:
        st.error(f"❌ Connection failed: {str(e)}")
        return None

@st.cache_data(ttl=300)
def query_data(query):
    """Execute query with caching (5 min TTL)"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.error(f"❌ Query failed: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# HEADER
# ============================================================================

st.markdown("""
<div class="app-header">
    <div class="app-logo">🏥 HealthGPT</div>
    <div class="app-title">Care Gap Trust Planner</div>
    <div class="app-subtitle">AI-Powered Healthcare Gap Analysis & Trust Scoring</div>
</div>
""", unsafe_allow_html=True)

# ============================================================================
# NAVIGATION TABS
# ============================================================================

# Initialize session state for tab selection
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = 'Overview Dashboard'

tabs = [
    "📊 Overview Dashboard",
    "🗺️ Geographic View", 
    "🏥 Facility Details",
    "📋 Gap Analysis",
    "⚙️ Model Insights",
    "📝 Action Planner"
]

# Create navigation buttons
cols = st.columns(len(tabs))
for idx, (col, tab) in enumerate(zip(cols, tabs)):
    with col:
        if st.button(tab, key=f"tab_{idx}", use_container_width=True):
            st.session_state.active_tab = tab

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

with st.sidebar:
    st.markdown('<div class="filter-title">🔍 Filters</div>', unsafe_allow_html=True)
    
    states_df = query_data("SELECT DISTINCT state FROM healthgpt_pg.public.care_gap_summary ORDER BY state")
    capabilities_df = query_data("SELECT DISTINCT capability FROM healthgpt_pg.public.care_gap_summary ORDER BY capability")
    
    selected_states = st.multiselect(
        "Select States",
        options=states_df['state'].tolist() if not states_df.empty else [],
        default=[]
    )
    
    selected_capabilities = st.multiselect(
        "Select Capabilities",
        options=capabilities_df['capability'].tolist() if not capabilities_df.empty else [],
        default=[]
    )
    
    st.markdown("---")
    st.markdown("**Data Source:** Unity Catalog")
    st.markdown("**Catalog:** `healthgpt_pg.public`")
    st.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d"))

# ============================================================================
# PAGE 1: OVERVIEW DASHBOARD
# ============================================================================

if st.session_state.active_tab == "📊 Overview Dashboard":
    
    # KPI Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    kpi_query = """
        SELECT 
            COUNT(*) as total_gaps,
            ROUND(AVG(gap_score), 1) as avg_score,
            SUM(CASE WHEN gap_severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
            SUM(total_facilities) as total_facilities
        FROM healthgpt_pg.public.care_gap_summary
    """
    kpi_df = query_data(kpi_query)
    
    if not kpi_df.empty:
        with col1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-icon">📊</div>
                <div class="kpi-label">Total Care Gaps</div>
                <div class="kpi-value">{int(kpi_df['total_gaps'].iloc[0]):,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-icon">📈</div>
                <div class="kpi-label">Avg Gap Score</div>
                <div class="kpi-value">{float(kpi_df['avg_score'].iloc[0]):.1f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-icon">⚠️</div>
                <div class="kpi-label">Critical Gaps</div>
                <div class="kpi-value">{int(kpi_df['critical_count'].iloc[0]):,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-icon">🏥</div>
                <div class="kpi-label">Total Facilities</div>
                <div class="kpi-value">{int(kpi_df['total_facilities'].iloc[0]):,}</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Top Critical Care Gaps
    st.markdown('<div class="content-card"><div class="card-title">🔥 Top 10 Critical Care Gaps</div>', unsafe_allow_html=True)
    
    top_gaps_query = """
        SELECT 
            state,
            capability,
            gap_score,
            gap_severity,
            total_facilities,
            strong_count,
            weak_count,
            intervention_urgency
        FROM healthgpt_pg.public.care_gap_summary
        ORDER BY gap_score DESC
        LIMIT 10
    """
    top_gaps_df = query_data(top_gaps_query)
    
    if not top_gaps_df.empty:
        st.dataframe(top_gaps_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Risk Indicators Section
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown('<div class="content-card"><div class="card-title">⚠️ Risk Indicators</div>', unsafe_allow_html=True)
        
        risk_query = "SELECT * FROM healthgpt_pg.public.risk_indicators ORDER BY risk_pct DESC"
        risk_df = query_data(risk_query)
        
        if not risk_df.empty:
            fig = px.bar(
                risk_df, 
                x='risk_pct', 
                y='indicator_name',
                orientation='h',
                title='',
                color='priority',
                color_discrete_map={'HIGH': '#d32f2f', 'MODERATE': '#f57c00', 'LOW': '#fbc02d'}
            )
            fig.update_layout(
                xaxis_title="Risk Percentage (%)",
                yaxis_title="",
                showlegend=True,
                height=400,
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="content-card"><div class="card-title">📊 Gap Severity Distribution</div>', unsafe_allow_html=True)
        
        severity_query = """
            SELECT gap_severity, COUNT(*) as count
            FROM healthgpt_pg.public.care_gap_summary
            GROUP BY gap_severity
            ORDER BY count DESC
        """
        severity_df = query_data(severity_query)
        
        if not severity_df.empty:
            fig = px.pie(
                severity_df,
                values='count',
                names='gap_severity',
                title='',
                color='gap_severity',
                color_discrete_map={'CRITICAL': '#d32f2f', 'HIGH': '#f57c00', 'MODERATE': '#fbc02d', 'LOW': '#66bb6a'}
            )
            fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# PAGE 2: GEOGRAPHIC VIEW
# ============================================================================

elif st.session_state.active_tab == "🗺️ Geographic View":
    
    st.markdown('<div class="content-card"><div class="card-title">🗺️ State-Level Care Gap Analysis</div>', unsafe_allow_html=True)
    
    geo_query = """
        SELECT 
            state,
            COUNT(DISTINCT capability) as num_capabilities,
            ROUND(AVG(gap_score), 1) as avg_gap_score,
            SUM(total_facilities) as total_facilities,
            SUM(CASE WHEN gap_severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_gaps
        FROM healthgpt_pg.public.care_gap_summary
        WHERE geography_type = 'state'
        GROUP BY state
        ORDER BY avg_gap_score DESC
    """
    geo_df = query_data(geo_query)
    
    if not geo_df.empty:
        # Bar chart
        fig = px.bar(
            geo_df.head(20),
            x='state',
            y='avg_gap_score',
            title='Top 20 States by Average Gap Score',
            color='critical_gaps',
            color_continuous_scale='Reds'
        )
        fig.update_layout(height=500, margin=dict(l=0, r=0, t=50, b=0))
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Data table
        st.dataframe(geo_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# PAGE 3: FACILITY DETAILS
# ============================================================================

elif st.session_state.active_tab == "🏥 Facility Details":
    
    st.markdown('<div class="content-card"><div class="card-title">🏥 Facility Trust & Capability Analysis</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        facility_state = st.selectbox(
            "Select State",
            options=states_df['state'].tolist() if not states_df.empty else []
        )
    
    with col2:
        facility_capability = st.selectbox(
            "Select Capability",
            options=capabilities_df['capability'].tolist() if not capabilities_df.empty else []
        )
    
    if facility_state and facility_capability:
        facility_query = f"""
            SELECT 
                facility_name,
                city,
                rule_trust_signal,
                ml_trust_signal,
                ROUND(ai_probability, 2) as ai_probability,
                review_priority,
                evidence_count,
                data_quality
            FROM healthgpt_pg.public.facility_detail
            WHERE state = '{facility_state}' 
              AND capability = '{facility_capability}'
            ORDER BY ml_trust_score DESC
            LIMIT 50
        """
        facilities_df = query_data(facility_query)
        
        if not facilities_df.empty:
            st.markdown(f"### Found {len(facilities_df)} facilities")
            st.dataframe(facilities_df, use_container_width=True, hide_index=True)
        else:
            st.info("No facilities found for the selected filters.")
    
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# PAGE 4: GAP ANALYSIS
# ============================================================================

elif st.session_state.active_tab == "📋 Gap Analysis":
    
    st.markdown('<div class="content-card"><div class="card-title">📋 Detailed Gap Analysis by Capability</div>', unsafe_allow_html=True)
    
    capability_query = """
        SELECT 
            capability,
            ROUND(AVG(gap_score), 1) as avg_gap,
            SUM(total_facilities) as total_facilities,
            COUNT(DISTINCT state) as num_states,
            SUM(CASE WHEN gap_severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count
        FROM healthgpt_pg.public.care_gap_summary
        GROUP BY capability
        ORDER BY avg_gap DESC
    """
    capability_df = query_data(capability_query)
    
    if not capability_df.empty:
        # Bar chart
        fig = px.bar(
            capability_df,
            x='capability',
            y='avg_gap',
            title='',
            color='avg_gap',
            color_continuous_scale='Reds'
        )
        fig.update_layout(
            xaxis_title="Healthcare Capability",
            yaxis_title="Average Gap Score",
            height=500,
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Data table
        st.dataframe(capability_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# PAGE 5: MODEL INSIGHTS
# ============================================================================

elif st.session_state.active_tab == "⚙️ Model Insights":
    
    st.markdown('<div class="content-card"><div class="card-title">⚙️ AI Model Performance & Insights</div>', unsafe_allow_html=True)
    
    st.info("ℹ️ This application uses algorithmic scoring with rule-based and ML-enhanced trust signals.")
    
    st.markdown("### Scoring Distribution")
    
    score_query = """
        SELECT 
            gap_severity,
            COUNT(*) as count,
            ROUND(AVG(gap_score), 1) as avg_score,
            ROUND(AVG(ml_gap_score), 1) as avg_ml_score
        FROM healthgpt_pg.public.care_gap_summary
        GROUP BY gap_severity
        ORDER BY avg_score DESC
    """
    score_df = query_data(score_query)
    
    if not score_df.empty:
        st.dataframe(score_df, use_container_width=True, hide_index=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig1 = px.bar(score_df, x='gap_severity', y='count', title='Gap Count by Severity')
            fig1.update_layout(height=400)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            fig2 = px.scatter(score_df, x='avg_score', y='avg_ml_score', 
                            size='count', text='gap_severity',
                            title='Rule Score vs ML Score')
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# PAGE 6: ACTION PLANNER
# ============================================================================

elif st.session_state.active_tab == "📝 Action Planner":
    
    st.markdown('<div class="content-card"><div class="card-title">📝 Intervention Action Planner</div>', unsafe_allow_html=True)
    
    urgent_query = """
        SELECT 
            state,
            capability,
            gap_score,
            gap_severity,
            intervention_urgency,
            total_facilities,
            weak_count,
            underserved_pop
        FROM healthgpt_pg.public.care_gap_summary
        WHERE intervention_urgency = 'URGENT'
        ORDER BY gap_score DESC
        LIMIT 25
    """
    urgent_df = query_data(urgent_query)
    
    if not urgent_df.empty:
        st.warning(f"⚠️ **{len(urgent_df)} gaps require URGENT intervention**")
        st.markdown("<br>", unsafe_allow_html=True)
        st.dataframe(urgent_df, use_container_width=True, hide_index=True)
    else:
        st.success("✅ No urgent interventions required at this time.")
    
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("""
<div class="app-footer">
    <p><strong>HealthGPT Care Gap Trust Planner</strong> | Powered by Unity Catalog & Databricks</p>
    <p>Data Source: <code>healthgpt_pg.public</code> | Built for DAIS 2026 Hackathon</p>
</div>
""", unsafe_allow_html=True)
