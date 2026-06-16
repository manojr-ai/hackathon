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
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stDataFrame {
        border: 1px solid #e0e0e0;
        border-radius: 5px;
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
# Use service principal client ID as PGUSER when in app context
PGUSER = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or os.environ.get("DATABRICKS_SERVICE_PRINCIPAL_CLIENT_ID", "54db6394-5ecf-408e-a05f-ddb1ba14b4b2")
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

@st.cache_resource
def get_connection():
    """Connect to Lakebase PostgreSQL"""
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

@st.cache_data(ttl=300)
def query_data(query):
    """Execute query with caching (5 min TTL)"""
    conn = get_connection()
    if conn is None:
        st.error("Database connection not available")
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
st.sidebar.markdown("*Powered by Lakebase*")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["📊 Overview Dashboard", "🗺️ Geographic View", "🏥 Facility Details", 
     "📋 Gap Analysis", "⚙️ Model Insights", "📝 Action Planner"],
    index=0
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Filters")

states_df = query_data("SELECT DISTINCT state FROM public.care_gap_summary ORDER BY state")
capabilities_df = query_data("SELECT DISTINCT capability FROM public.care_gap_summary ORDER BY capability")

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

# Build WHERE clause for filters
where_clauses = []
if selected_states:
    states_str = "', '".join(selected_states)
    where_clauses.append(f"state IN ('{states_str}')")
if selected_capabilities:
    caps_str = "', '".join(selected_capabilities)
    where_clauses.append(f"capability IN ('{caps_str}')")
where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""

# ============================================================================
# PAGE 1: OVERVIEW DASHBOARD
# ============================================================================

if page == "📊 Overview Dashboard":
    st.markdown('<div class="main-header">📊 Healthcare Care Gap Overview</div>', unsafe_allow_html=True)
    
    st.markdown("### 🎯 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    query = f"SELECT COUNT(*) as total_gaps, AVG(gap_score) as avg_score FROM public.care_gap_summary WHERE 1=1 {where_clause}"
    kpi_df = query_data(query)
    
    if not kpi_df.empty:
        with col1:
            st.metric("Total Care Gaps", f"{int(kpi_df['total_gaps'].iloc[0]):,}")
        with col2:
            st.metric("Average Gap Score", f"{float(kpi_df['avg_score'].iloc[0]):.1f}")
    
    critical_query = f"SELECT COUNT(*) as critical FROM public.care_gap_summary WHERE gap_severity = 'CRITICAL' {where_clause}"
    critical_df = query_data(critical_query)
    
    if not critical_df.empty:
        with col3:
            st.metric("Critical Gaps", f"{int(critical_df['critical'].iloc[0]):,}", delta="High Priority", delta_color="inverse")
    
    facilities_query = f"SELECT SUM(total_facilities) as total FROM public.care_gap_summary WHERE 1=1 {where_clause}"
    facilities_df = query_data(facilities_query)
    
    if not facilities_df.empty:
        with col4:
            st.metric("Total Facilities", f"{int(facilities_df['total'].iloc[0]):,}")
    
    st.markdown("---")
    st.markdown("### 🔥 Top 10 Critical Care Gaps")
    top_gaps_query = f'''
        SELECT state, capability, gap_score, gap_severity,
               total_facilities, strong_count, weak_count, intervention_urgency
        FROM public.care_gap_summary
        WHERE 1=1 {where_clause}
        ORDER BY gap_score DESC LIMIT 10
    '''
    
    top_gaps_df = query_data(top_gaps_query)
    if not top_gaps_df.empty:
        # Format and display
        top_gaps_df['gap_score'] = top_gaps_df['gap_score'].round(2)
        st.dataframe(
            top_gaps_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "gap_score": st.column_config.ProgressColumn(
                    "Gap Score",
                    format="%.2f",
                    min_value=0,
                    max_value=100,
                ),
                "gap_severity": st.column_config.TextColumn(
                    "Severity",
                    help="Care gap severity level"
                )
            }
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### ⚠️ Risk Indicators")
        risk_query = "SELECT indicator_name, risk_pct, priority FROM public.risk_indicators ORDER BY risk_pct DESC"
        risk_df = query_data(risk_query)
        
        if not risk_df.empty:
            fig = px.bar(
                risk_df,
                x='risk_pct',
                y='indicator_name',
                orientation='h',
                title='Risk Factor Prevalence (%)',
                color='priority',
                color_discrete_map={'HIGH': '#ff4444', 'MEDIUM': '#ffaa00', 'LOW': '#44ff44'},
                labels={'risk_pct': 'Risk %', 'indicator_name': 'Risk Indicator'}
            )
            fig.update_layout(height=400, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 📊 Gap Severity Distribution")
        severity_query = f'''
            SELECT gap_severity, COUNT(*) as count
            FROM public.care_gap_summary
            WHERE 1=1 {where_clause}
            GROUP BY gap_severity
            ORDER BY count DESC
        '''
        severity_df = query_data(severity_query)
        
        if not severity_df.empty:
            fig = px.pie(
                severity_df,
                values='count',
                names='gap_severity',
                title='Care Gaps by Severity',
                color='gap_severity',
                color_discrete_map={'CRITICAL': '#d32f2f', 'HIGH': '#f57c00', 'MEDIUM': '#fbc02d', 'LOW': '#689f38'}
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# PAGE 2: GEOGRAPHIC VIEW
# ============================================================================

elif page == "🗺️ Geographic View":
    st.markdown('<div class="main-header">🗺️ Geographic Care Gap Distribution</div>', unsafe_allow_html=True)
    
    st.markdown("### State-Level Care Gap Analysis")
    
    map_query = f'''
        SELECT state, capability, gap_score, gap_severity, total_facilities,
               strong_count, weak_count, supply_adequacy, underserved_pop
        FROM public.care_gap_summary
        WHERE geography_type = 'state' {where_clause}
        ORDER BY gap_score DESC
    '''
    map_df = query_data(map_query)
    
    if not map_df.empty:
        # State summary
        state_summary = map_df.groupby('state').agg({
            'gap_score': 'mean',
            'total_facilities': 'sum',
            'underserved_pop': 'sum'
        }).reset_index()
        state_summary.columns = ['state', 'avg_gap_score', 'total_facilities', 'underserved_pop']
        state_summary = state_summary.sort_values('avg_gap_score', ascending=False)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("#### States by Average Gap Score")
            fig = px.bar(
                state_summary.head(15),
                x='avg_gap_score',
                y='state',
                orientation='h',
                title='Top 15 States by Care Gap Score',
                color='avg_gap_score',
                color_continuous_scale='Reds',
                labels={'avg_gap_score': 'Average Gap Score', 'state': 'State'}
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### State Statistics")
            st.dataframe(
                state_summary[['state', 'avg_gap_score', 'total_facilities']].head(15),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "avg_gap_score": st.column_config.NumberColumn(
                        "Avg Gap Score",
                        format="%.2f"
                    ),
                    "total_facilities": st.column_config.NumberColumn(
                        "Facilities",
                        format="%d"
                    )
                }
            )
        
        st.markdown("---")
        st.markdown("### Detailed Care Gap Data by State & Capability")
        
        # Heatmap visualization
        pivot_df = map_df.pivot_table(
            values='gap_score',
            index='state',
            columns='capability',
            aggfunc='mean'
        ).fillna(0)
        
        fig = px.imshow(
            pivot_df,
            labels=dict(x="Healthcare Capability", y="State", color="Gap Score"),
            x=pivot_df.columns,
            y=pivot_df.index,
            color_continuous_scale='RdYlGn_r',
            title='Care Gap Heatmap: States vs Capabilities',
            aspect="auto"
        )
        fig.update_layout(height=800)
        st.plotly_chart(fig, use_container_width=True)
        
        # Raw data table
        with st.expander("📋 View Raw Data"):
            st.dataframe(map_df, use_container_width=True, hide_index=True)

# ============================================================================
# PAGE 3: FACILITY DETAILS
# ============================================================================

elif page == "🏥 Facility Details":
    st.markdown('<div class="main-header">🏥 Facility Trust & Capability Details</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        facility_state = st.selectbox(
            "Select State",
            options=["All"] + (states_df['state'].tolist() if not states_df.empty else [])
        )
    with col2:
        facility_capability = st.selectbox(
            "Select Capability",
            options=["All"] + (capabilities_df['capability'].tolist() if not capabilities_df.empty else [])
        )
    
    # Build filter
    facility_where = []
    if facility_state != "All":
        facility_where.append(f"state = '{facility_state}'")
    if facility_capability != "All":
        facility_where.append(f"capability = '{facility_capability}'")
    facility_where_clause = " AND " + " AND ".join(facility_where) if facility_where else ""
    
    facility_query = f'''
        SELECT facility_name, city, state, capability,
               rule_trust_signal, rule_confidence, ml_trust_signal, ml_trust_score,
               ai_probability, dual_score_agreement, review_priority, evidence_count,
               latitude, longitude, data_quality
        FROM public.facility_detail
        WHERE 1=1 {facility_where_clause}
        ORDER BY ml_trust_score DESC
        LIMIT 100
    '''
    facilities_df = query_data(facility_query)
    
    if not facilities_df.empty:
        st.markdown(f"### Found {len(facilities_df)} facilities")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            strong_count = len(facilities_df[facilities_df['ml_trust_signal'] == 'strong'])
            st.metric("Strong Trust", strong_count)
        with col2:
            partial_count = len(facilities_df[facilities_df['ml_trust_signal'] == 'partial'])
            st.metric("Partial Trust", partial_count)
        with col3:
            weak_count = len(facilities_df[facilities_df['ml_trust_signal'] == 'weak'])
            st.metric("Weak Trust", weak_count)
        with col4:
            urgent_count = len(facilities_df[facilities_df['review_priority'] == 'urgent'])
            st.metric("Need Review", urgent_count)
        
        # Trust signal distribution
        col1, col2 = st.columns(2)
        
        with col1:
            trust_dist = facilities_df['ml_trust_signal'].value_counts().reset_index()
            trust_dist.columns = ['trust_signal', 'count']
            fig = px.pie(
                trust_dist,
                values='count',
                names='trust_signal',
                title='ML Trust Signal Distribution',
                color='trust_signal',
                color_discrete_map={'strong': '#4caf50', 'partial': '#ff9800', 'weak': '#f44336'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            agreement_dist = facilities_df['dual_score_agreement'].value_counts().reset_index()
            agreement_dist.columns = ['agreement', 'count']
            fig = px.bar(
                agreement_dist,
                x='agreement',
                y='count',
                title='Rule-ML Score Agreement',
                color='count',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Facilities table
        st.markdown("### Facility List")
        st.dataframe(
            facilities_df[[
                'facility_name', 'city', 'state', 'capability',
                'ml_trust_signal', 'ai_probability', 'rule_confidence',
                'review_priority', 'evidence_count', 'data_quality'
            ]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "ai_probability": st.column_config.ProgressColumn(
                    "AI Probability",
                    format="%.3f",
                    min_value=0,
                    max_value=1,
                ),
                "rule_confidence": st.column_config.NumberColumn(
                    "Rule Score",
                    format="%d"
                )
            }
        )
        
        # Map view
        if not facilities_df[['latitude', 'longitude']].isna().all().all():
            with st.expander("🗺️ View Facilities on Map"):
                map_data = facilities_df.dropna(subset=['latitude', 'longitude'])
                fig = px.scatter_mapbox(
                    map_data,
                    lat='latitude',
                    lon='longitude',
                    color='ml_trust_signal',
                    hover_name='facility_name',
                    hover_data=['city', 'capability', 'ai_probability'],
                    color_discrete_map={'strong': 'green', 'partial': 'orange', 'weak': 'red'},
                    zoom=4,
                    height=600
                )
                fig.update_layout(mapbox_style="open-street-map")
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No facilities found for the selected filters.")

# ============================================================================
# PAGE 4: GAP ANALYSIS
# ============================================================================

elif page == "📋 Gap Analysis":
    st.markdown('<div class="main-header">📋 Detailed Gap Analysis</div>', unsafe_allow_html=True)
    
    st.markdown("### Gap Score Distribution by Healthcare Capability")
    
    capability_query = f'''
        SELECT capability, AVG(gap_score) as avg_gap, AVG(ml_gap_score) as avg_ml_gap,
               SUM(total_facilities) as total_facilities,
               AVG(strong_pct) as avg_strong_pct,
               COUNT(*) as num_states
        FROM public.care_gap_summary
        WHERE 1=1 {where_clause}
        GROUP BY capability
        ORDER BY avg_gap DESC
    '''
    capability_df = query_data(capability_query)
    
    if not capability_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                capability_df,
                x='capability',
                y='avg_gap',
                title='Average Gap Score by Healthcare Capability',
                color='avg_gap',
                color_continuous_scale='Reds',
                labels={'avg_gap': 'Avg Gap Score', 'capability': 'Capability'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                capability_df,
                x='capability',
                y='avg_strong_pct',
                title='Strong Evidence Coverage (%)',
                color='avg_strong_pct',
                color_continuous_scale='Greens',
                labels={'avg_strong_pct': 'Strong %', 'capability': 'Capability'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(
            capability_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "avg_gap": st.column_config.NumberColumn("Avg Gap Score", format="%.2f"),
                "avg_ml_gap": st.column_config.NumberColumn("Avg ML Gap", format="%.2f"),
                "avg_strong_pct": st.column_config.NumberColumn("Strong %", format="%.2f")
            }
        )
    
    st.markdown("---")
    st.markdown("### Supply vs Demand Analysis")
    
    supply_query = f'''
        SELECT supply_adequacy, COUNT(*) as count, AVG(gap_score) as avg_gap
        FROM public.care_gap_summary
        WHERE 1=1 {where_clause}
        GROUP BY supply_adequacy
        ORDER BY count DESC
    '''
    supply_df = query_data(supply_query)
    
    if not supply_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                supply_df,
                values='count',
                names='supply_adequacy',
                title='Supply Adequacy Distribution',
                color='supply_adequacy',
                color_discrete_map={'adequate': '#4caf50', 'moderate': '#ff9800', 'inadequate': '#f44336'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                supply_df,
                x='supply_adequacy',
                y='avg_gap',
                title='Average Gap Score by Supply Adequacy',
                color='avg_gap',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# PAGE 5: MODEL INSIGHTS
# ============================================================================

elif page == "⚙️ Model Insights":
    st.markdown('<div class="main-header">⚙️ AI Model Performance & Insights</div>', unsafe_allow_html=True)
    
    st.info("This application uses dual scoring: Rule-based + ML algorithmic predictions for demo purposes.")
    
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

elif page == "📝 Action Planner":
    st.markdown('<div class="main-header">📝 Intervention Action Planner</div>', unsafe_allow_html=True)
    
    st.markdown("### 🚨 Urgent Interventions Required")
    
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