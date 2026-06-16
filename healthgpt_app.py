import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
from datetime import datetime
from databricks.sdk import WorkspaceClient

# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER
# Design-matched UI with Lakebase authentication
# ============================================================================

st.set_page_config(
    page_title="HealthGPT Care Gap Trust Planner",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# CUSTOM CSS - EXACT DESIGN MATCH
# ============================================================================

st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    .main .block-container {
        padding-top: 0rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 100%;
    }
    
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
    
    .app-footer {
        text-align: center;
        padding: 2rem 0 1rem 0;
        color: #757575;
        font-size: 0.9rem;
        border-top: 1px solid #e0e0e0;
        margin-top: 3rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# LAKEBASE CONNECTION - Using existing authentication method
# ============================================================================

PGHOST = os.environ.get("PGHOST", "ep-wild-snow-d8k94scg.database.us-east-2.cloud.databricks.com")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "healthgpt")
PGUSER = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or os.environ.get("DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID", "54db6394-5ecf-408e-a05f-ddb1ba14b4b2")
ENDPOINT_NAME = "projects/hackthon/branches/production/endpoints/primary"

@st.cache_resource(ttl=900)
def get_lakebase_token():
    """Generate Lakebase database credential token via REST API"""
    w = WorkspaceClient()
    try:
        st.info(f"🔑 Generating token for endpoint: {ENDPOINT_NAME}")
        result = w.api_client.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": ENDPOINT_NAME}
        )
        token = result.get("token") or result.get("access_token")
        if token:
            st.success(f"✅ Token generated: {len(token)} chars")
        else:
            st.error("❌ No token in response")
        return token
    except Exception as e:
        st.error(f"❌ Unable to generate database credentials: {e}")
        return None

@st.cache_resource
def get_connection():
    """Connect to Lakebase PostgreSQL"""
    st.info("🔍 DEBUG: Connection Parameters")
    st.write(f"Host: {PGHOST}")
    st.write(f"Port: {PGPORT}")
    st.write(f"Database: {PGDATABASE}")
    st.write(f"User: {PGUSER}")
    st.write(f"Endpoint: {ENDPOINT_NAME}")
    
    # Show environment variables
    st.write("Environment Variables:")
    st.write(f"  PGUSER: {os.environ.get('PGUSER', 'NOT_SET')}")
    st.write(f"  DATABRICKS_CLIENT_ID: {os.environ.get('DATABRICKS_CLIENT_ID', 'NOT_SET')}")
    st.write(f"  DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID: {os.environ.get('DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID', 'NOT_SET')}")
    
    token = get_lakebase_token()
    if not token:
        st.error("❌ No token available")
        return None
        
    st.info("🔌 Attempting PostgreSQL connection...")
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
        st.success("✅ PostgreSQL connection successful!")
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"❌ OperationalError: {str(e)}")
        st.error("This usually means authorization failed or the database is unreachable")
        return None
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
        st.error(f"Query failed: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# DEBUG: APP STARTUP - SHOW IMMEDIATELY (NO DB CONNECTION YET)
# ============================================================================

st.sidebar.markdown("### 🐞 Debug Info")
st.sidebar.write("✅ App started successfully")
st.sidebar.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔧 Configuration")
st.sidebar.write(f"**Host:** {PGHOST}")
st.sidebar.write(f"**Port:** {PGPORT}")
st.sidebar.write(f"**Database:** {PGDATABASE}")
st.sidebar.write(f"**Endpoint:** {ENDPOINT_NAME}")
st.sidebar.markdown("---")
st.sidebar.markdown("### 👤 User Info")
st.sidebar.write(f"**PGUSER var:** {os.environ.get('PGUSER', 'NOT_SET')}")
st.sidebar.write(f"**CLIENT_ID var:** {os.environ.get('DATABRICKS_CLIENT_ID', 'NOT_SET')}")
st.sidebar.write(f"**SP_CLIENT_ID var:** {os.environ.get('DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID', 'NOT_SET')}")
st.sidebar.write(f"**Computed PGUSER:** {PGUSER}")

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
# CONNECTION STATUS CHECK
# ============================================================================

st.info("🔍 Testing database connection...")
try:
    conn_test = get_connection()
    if conn_test:
        st.success("✅ Database connected successfully!")
    else:
        st.error("❌ Database connection failed - check sidebar for debug info")
except Exception as e:
    st.error(f"❌ Connection error: {str(e)}")

# ============================================================================
# NAVIGATION TABS
# ============================================================================

if 'active_tab' not in st.session_state:
    st.session_state.active_tab = '📊 Overview Dashboard'

tabs = [
    "📊 Overview Dashboard",
    "🗺️ Geographic View", 
    "🏥 Facility Details",
    "📋 Gap Analysis",
    "⚙️ Model Insights",
    "📝 Action Planner"
]

cols = st.columns(len(tabs))
for idx, (col, tab) in enumerate(zip(cols, tabs)):
    with col:
        if st.button(tab, key=f"tab_{idx}", use_container_width=True):
            st.session_state.active_tab = tab

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

with st.sidebar:
    st.markdown("---")
    st.markdown("### 🔍 Filters")
    
    # Try to load filters, but don't crash if connection fails
    try:
        states_df = query_data("SELECT DISTINCT state FROM care_gap_summary ORDER BY state")
        capabilities_df = query_data("SELECT DISTINCT capability FROM care_gap_summary ORDER BY capability")
    except Exception as e:
        st.error(f"⚠️ Filter load failed: {str(e)[:100]}")
        states_df = pd.DataFrame()
        capabilities_df = pd.DataFrame()
    
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
    st.markdown("**Data Source:** Lakebase PostgreSQL")
    st.markdown("**Database:** `healthgpt`")
    st.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M"))

# ============================================================================
# PAGE 1: OVERVIEW DASHBOARD
# ============================================================================

if st.session_state.active_tab == "📊 Overview Dashboard":
    
    # KPI Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    kpi_query = """
        SELECT 
            COUNT(*) as total_gaps,
            ROUND(AVG(gap_score)::numeric, 1) as avg_score,
            SUM(CASE WHEN gap_severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
            SUM(total_facilities) as total_facilities
        FROM care_gap_summary
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
            state, capability, gap_score, gap_severity,
            total_facilities, strong_count, weak_count, intervention_urgency
        FROM care_gap_summary
        ORDER BY gap_score DESC LIMIT 10
    """
    top_gaps_df = query_data(top_gaps_query)
    
    if not top_gaps_df.empty:
        st.dataframe(top_gaps_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Risk Indicators
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown('<div class="content-card"><div class="card-title">⚠️ Risk Indicators</div>', unsafe_allow_html=True)
        
        risk_query = "SELECT * FROM risk_indicators ORDER BY risk_pct DESC"
        risk_df = query_data(risk_query)
        
        if not risk_df.empty:
            fig = px.bar(
                risk_df, 
                x='risk_pct', 
                y='indicator_name',
                orientation='h',
                color='priority',
                color_discrete_map={'HIGH': '#d32f2f', 'MODERATE': '#f57c00', 'LOW': '#fbc02d'}
            )
            fig.update_layout(xaxis_title="Risk %", yaxis_title="", showlegend=True, height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="content-card"><div class="card-title">📊 Gap Severity Distribution</div>', unsafe_allow_html=True)
        
        severity_query = """
            SELECT gap_severity, COUNT(*) as count
            FROM care_gap_summary GROUP BY gap_severity
        """
        severity_df = query_data(severity_query)
        
        if not severity_df.empty:
            fig = px.pie(severity_df, values='count', names='gap_severity',
                        color='gap_severity',
                        color_discrete_map={'CRITICAL': '#d32f2f', 'HIGH': '#f57c00', 
                                          'MODERATE': '#fbc02d', 'LOW': '#66bb6a'})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.active_tab == "🗺️ Geographic View":
    st.markdown('<div class="content-card"><div class="card-title">🗺️ State-Level Care Gap Analysis</div>', unsafe_allow_html=True)
    
    geo_query = """
        SELECT state, COUNT(DISTINCT capability) as num_capabilities,
               ROUND(AVG(gap_score)::numeric, 1) as avg_gap_score,
               SUM(total_facilities) as total_facilities,
               SUM(CASE WHEN gap_severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_gaps
        FROM care_gap_summary WHERE geography_type = 'state'
        GROUP BY state ORDER BY avg_gap_score DESC
    """
    geo_df = query_data(geo_query)
    
    if not geo_df.empty:
        fig = px.bar(geo_df.head(20), x='state', y='avg_gap_score',
                    title='Top 20 States by Avg Gap Score', color='critical_gaps')
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(geo_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.active_tab == "🏥 Facility Details":
    st.markdown('<div class="content-card"><div class="card-title">🏥 Facility Trust & Capability</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    states_df = query_data("SELECT DISTINCT state FROM care_gap_summary ORDER BY state")
    capabilities_df = query_data("SELECT DISTINCT capability FROM care_gap_summary ORDER BY capability")
    
    with col1:
        facility_state = st.selectbox("Select State", 
            options=states_df['state'].tolist() if not states_df.empty else [])
    with col2:
        facility_capability = st.selectbox("Select Capability",
            options=capabilities_df['capability'].tolist() if not capabilities_df.empty else [])
    
    if facility_state and facility_capability:
        facility_query = f"""
            SELECT facility_name, city, rule_trust_signal, ml_trust_signal,
                   ROUND(ai_probability::numeric, 2) as ai_probability, 
                   review_priority, evidence_count
            FROM facility_detail
            WHERE state = '{facility_state}' AND capability = '{facility_capability}'
            ORDER BY ml_trust_score DESC LIMIT 50
        """
        facilities_df = query_data(facility_query)
        
        if not facilities_df.empty:
            st.markdown(f"### Found {len(facilities_df)} facilities")
            st.dataframe(facilities_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.active_tab == "📋 Gap Analysis":
    st.markdown('<div class="content-card"><div class="card-title">📋 Detailed Gap Analysis</div>', unsafe_allow_html=True)
    
    capability_query = """
        SELECT capability, ROUND(AVG(gap_score)::numeric, 1) as avg_gap,
               SUM(total_facilities) as total_facilities, COUNT(DISTINCT state) as num_states
        FROM care_gap_summary GROUP BY capability ORDER BY avg_gap DESC
    """
    capability_df = query_data(capability_query)
    
    if not capability_df.empty:
        fig = px.bar(capability_df, x='capability', y='avg_gap',
                    color='avg_gap', color_continuous_scale='Reds')
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(capability_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.active_tab == "⚙️ Model Insights":
    st.markdown('<div class="content-card"><div class="card-title">⚙️ AI Model Performance</div>', unsafe_allow_html=True)
    st.info("ℹ️ Algorithmic scoring with rule-based and ML-enhanced trust signals")
    
    score_query = """
        SELECT gap_severity, COUNT(*) as count,
               ROUND(AVG(gap_score)::numeric, 1) as avg_score
        FROM care_gap_summary GROUP BY gap_severity ORDER BY avg_score DESC
    """
    score_df = query_data(score_query)
    
    if not score_df.empty:
        st.dataframe(score_df, use_container_width=True, hide_index=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.active_tab == "📝 Action Planner":
    st.markdown('<div class="content-card"><div class="card-title">📝 Intervention Action Planner</div>', unsafe_allow_html=True)
    
    urgent_query = """
        SELECT state, capability, gap_score, gap_severity, intervention_urgency,
               total_facilities, weak_count, underserved_pop
        FROM care_gap_summary WHERE intervention_urgency = 'URGENT'
        ORDER BY gap_score DESC LIMIT 25
    """
    urgent_df = query_data(urgent_query)
    
    if not urgent_df.empty:
        st.warning(f"⚠️ **{len(urgent_df)} gaps require URGENT intervention**")
        st.dataframe(urgent_df, use_container_width=True, hide_index=True)
    else:
        st.success("✅ No urgent interventions required")
    
    st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown("""
<div class="app-footer">
    <p><strong>HealthGPT Care Gap Trust Planner</strong> | Powered by Lakebase & Databricks</p>
    <p>Data: Lakebase PostgreSQL | DAIS 2026 Hackathon</p>
</div>
""", unsafe_allow_html=True)
