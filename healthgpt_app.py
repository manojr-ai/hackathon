import streamlit as st
import pandas as pd
import plotly.express as px
from databricks import sql
from datetime import datetime
import os

# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER - Unity Catalog Backend
# ============================================================================
# Backend: Unity Catalog tables in healthgpt_pg catalog (Lakebase)
# Date: June 2026
# ============================================================================

st.set_page_config(
    page_title="HealthGPT Care Gap Trust Planner",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown('''
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
</style>
''', unsafe_allow_html=True)

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
        st.error(f"Connection failed: {str(e)}")
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
# SIDEBAR
# ============================================================================

st.sidebar.markdown("# 🏥 HealthGPT")
st.sidebar.markdown("**Care Gap Trust Planner**")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["📊 Overview Dashboard", "🗺️ Geographic View", "🏥 Facility Details", 
     "📋 Gap Analysis", "⚙️ Model Insights", "📝 Action Planner"],
    index=0
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Filters")

states_df = query_data("SELECT DISTINCT state FROM healthgpt_pg.public.care_gap_summary ORDER BY state")
capabilities_df = query_data("SELECT DISTINCT capability FROM healthgpt_pg.public.care_gap_summary ORDER BY capability")

selected_states = st.sidebar.multiselect(
    "States",
    options=states_df['state'].tolist() if not states_df.empty else [],
    default=[]
)

selected_capabilities = st.sidebar.multiselect(
    "Healthcare Capabilities",
    options=capabilities_df['capability'].tolist() if not capabilities_df.empty else [],
    default=[]
)

# ============================================================================
# PAGE 1: OVERVIEW DASHBOARD
# ============================================================================

if page == "📊 Overview Dashboard":
    st.markdown('<div class="main-header">📊 Healthcare Care Gap Overview</div>', unsafe_allow_html=True)
    
    st.markdown("### 🎯 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    query = "SELECT COUNT(*) as total_gaps, AVG(gap_score) as avg_score FROM healthgpt_pg.public.care_gap_summary"
    kpi_df = query_data(query)
    
    if not kpi_df.empty:
        with col1:
            st.metric("Total Care Gaps", f"{kpi_df['total_gaps'].iloc[0]:,}")
        with col2:
            st.metric("Average Gap Score", f"{kpi_df['avg_score'].iloc[0]:.1f}")
    
    critical_query = "SELECT COUNT(*) as critical FROM healthgpt_pg.public.care_gap_summary WHERE gap_severity = 'CRITICAL'"
    critical_df = query_data(critical_query)
    
    if not critical_df.empty:
        with col3:
            st.metric("Critical Gaps", f"{critical_df['critical'].iloc[0]:,}")
    
    facilities_query = "SELECT SUM(total_facilities) as total FROM healthgpt_pg.public.care_gap_summary"
    facilities_df = query_data(facilities_query)
    
    if not facilities_df.empty:
        with col4:
            st.metric("Total Facilities", f"{facilities_df['total'].iloc[0]:,}")
    
    st.markdown("### 🔥 Top 10 Critical Care Gaps")
    top_gaps_query = '''
        SELECT state, capability, gap_score, gap_severity,
               total_facilities, strong_count, intervention_urgency
        FROM healthgpt_pg.public.care_gap_summary
        ORDER BY gap_score DESC LIMIT 10
    '''
    
    top_gaps_df = query_data(top_gaps_query)
    if not top_gaps_df.empty:
        st.dataframe(top_gaps_df, use_container_width=True, hide_index=True)
    
    st.markdown("### ⚠️ Risk Indicators")
    risk_query = "SELECT * FROM healthgpt_pg.public.risk_indicators ORDER BY risk_pct DESC"
    risk_df = query_data(risk_query)
    
    if not risk_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(risk_df, x='risk_pct', y='indicator_name', orientation='h',
                        title='Risk Factor Prevalence', color='priority')
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.dataframe(risk_df[['indicator_name', 'risk_pct', 'priority']], 
                        use_container_width=True, hide_index=True)

elif page == "🗺️ Geographic View":
    st.markdown('<div class="main-header">🗺️ Geographic Care Gap Distribution</div>', unsafe_allow_html=True)
    
    map_query = "SELECT * FROM healthgpt_pg.public.care_gap_summary WHERE geography_type = 'state' ORDER BY gap_score DESC"
    map_df = query_data(map_query)
    
    if not map_df.empty:
        st.dataframe(map_df, use_container_width=True, hide_index=True)

elif page == "🏥 Facility Details":
    st.markdown('<div class="main-header">🏥 Facility Trust & Capability Details</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        facility_state = st.selectbox("Select State", 
            options=states_df['state'].tolist() if not states_df.empty else [])
    with col2:
        facility_capability = st.selectbox("Select Capability",
            options=capabilities_df['capability'].tolist() if not capabilities_df.empty else [])
    
    if facility_state and facility_capability:
        facility_query = f'''
            SELECT facility_name, city, rule_trust_signal, ml_trust_signal,
                   ai_probability, review_priority, evidence_count
            FROM healthgpt_pg.public.facility_detail
            WHERE state = '{facility_state}' AND capability = '{facility_capability}'
            ORDER BY ml_trust_score DESC LIMIT 50
        '''
        facilities_df = query_data(facility_query)
        if not facilities_df.empty:
            st.markdown(f"### Found {len(facilities_df)} facilities")
            st.dataframe(facilities_df, use_container_width=True, hide_index=True)

elif page == "📋 Gap Analysis":
    st.markdown('<div class="main-header">📋 Detailed Gap Analysis</div>', unsafe_allow_html=True)
    
    capability_query = '''
        SELECT capability, AVG(gap_score) as avg_gap,
               SUM(total_facilities) as total_facilities, COUNT(*) as num_states
        FROM healthgpt_pg.public.care_gap_summary
        GROUP BY capability ORDER BY avg_gap DESC
    '''
    capability_df = query_data(capability_query)
    
    if not capability_df.empty:
        fig = px.bar(capability_df, x='capability', y='avg_gap',
                    title='Average Gap Score by Healthcare Capability',
                    color='avg_gap', color_continuous_scale='Reds')
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(capability_df, use_container_width=True, hide_index=True)

elif page == "⚙️ Model Insights":
    st.markdown('<div class="main-header">⚙️ AI Model Performance & Insights</div>', unsafe_allow_html=True)
    
    st.info("This application uses algorithmic scoring for demo purposes.")
    
    score_query = '''
        SELECT gap_severity, COUNT(*) as count, AVG(gap_score) as avg_score
        FROM healthgpt_pg.public.care_gap_summary 
        GROUP BY gap_severity ORDER BY avg_score DESC
    '''
    score_df = query_data(score_query)
    if not score_df.empty:
        st.dataframe(score_df, use_container_width=True, hide_index=True)

elif page == "📝 Action Planner":
    st.markdown('<div class="main-header">📝 Intervention Action Planner</div>', unsafe_allow_html=True)
    
    urgent_query = '''
        SELECT state, capability, gap_score, gap_severity, intervention_urgency, total_facilities
        FROM healthgpt_pg.public.care_gap_summary
        WHERE intervention_urgency = 'URGENT' ORDER BY gap_score DESC LIMIT 20
    '''
    urgent_df = query_data(urgent_query)
    
    if not urgent_df.empty:
        st.warning(f"⚠️ {len(urgent_df)} gaps require URGENT intervention")
        st.dataframe(urgent_df, use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown("<div style='text-align: center;'><p>HealthGPT Care Gap Trust Planner | Powered by Unity Catalog</p></div>", 
           unsafe_allow_html=True)
