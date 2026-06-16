# HealthGPT Care Gap Trust Planner - Deployment Guide

## Overview

Streamlit app with 6 interactive screens connected to Lakebase PostgreSQL:

1. **📊 Overview Dashboard** - Key metrics, top gaps, risk indicators
2. **🗺️ Geographic View** - State-level analysis with heatmaps
3. **🏥 Facility Details** - Facility trust scores and map visualization
4. **📋 Gap Analysis** - Supply/demand analysis by capability
5. **⚙️ Model Insights** - Dual scoring system performance
6. **📝 Action Planner** - Urgent interventions and resource allocation

## Files Created

* `healthgpt_lakebase_app.py` - Main Streamlit application (655 lines)
* `app.yaml` - Databricks app configuration
* `APP_DEPLOYMENT.md` - This file

## Data Sources

**Lakebase PostgreSQL Database:**
* Host: `ep-wild-snow-d8k94scg.database.us-east-2.cloud.databricks.com`
* Port: `5432`
* Database: `healthgpt`
* Tables:
  - `care_gap_summary` (923 rows)
  - `facility_detail` (27,978 rows)
  - `risk_indicators` (10 rows)

## Deployment Options

### Option 1: Deploy as Databricks App (Recommended)

```bash
# Navigate to app directory
cd /Workspace/Users/manoj.rayalla@acuitybrands.com/hackathon

# Create the app
databricks apps create healthgpt-care-gap-planner

# Deploy the app
databricks apps deploy healthgpt-care-gap-planner \
  --source-code-path ./

# Start the app
databricks apps start healthgpt-care-gap-planner

# Get the app URL
databricks apps get healthgpt-care-gap-planner --output JSON
```

### Option 2: Run in Databricks Notebook

1. Open a notebook
2. Install Streamlit (if needed):
   ```python
   %pip install streamlit psycopg2-binary plotly
   ```
3. Run the app:
   ```python
   !streamlit run /Workspace/Users/manoj.rayalla@acuitybrands.com/hackathon/healthgpt_lakebase_app.py --server.port=8501
   ```

### Option 3: Deploy via UI

1. Navigate to: **Machine Learning → Apps**
2. Click **Create App**
3. Select **Streamlit**
4. Choose the `healthgpt_lakebase_app.py` file
5. Configure resources and deploy

## Features Implemented

### 1. Overview Dashboard
- 4 KPI metrics (total gaps, avg score, critical gaps, facilities)
- Top 10 critical care gaps table with progress bars
- Risk indicator bar chart
- Gap severity pie chart

### 2. Geographic View
- State-level summary bar chart (top 15 states)
- State statistics table
- Heatmap: States vs Capabilities
- Raw data explorer

### 3. Facility Details
- State & capability filters
- Summary metrics (strong/partial/weak trust counts)
- Trust signal distribution pie chart
- Rule-ML agreement bar chart
- Detailed facility list with AI probability progress bars
- Interactive map view (if lat/long available)

### 4. Gap Analysis
- Average gap score by capability (bar chart)
- Strong evidence coverage % (bar chart)
- Detailed capability statistics table
- Supply adequacy pie chart
- Average gap by supply adequacy

### 5. Model Insights
- Dual scoring agreement metrics
- Confidence level pie chart
- Score divergence bar chart
- Gap severity sunburst diagram
- Severity by confidence table

### 6. Action Planner
- Urgent interventions count and list
- Priority matrix scatter plot (gap score vs underserved pop)
- Urgency by capability bar chart
- Resource allocation recommendations
- States with most weak facilities
- Resource priority ranking table

## Interactive Features

* **Sidebar Filters**: Filter by states and capabilities (applied across all pages)
* **Dynamic Queries**: All visualizations respond to filter changes
* **Caching**: 5-minute TTL for data queries (300 seconds)
* **Connection Pooling**: Lakebase connection cached for 1 hour
* **Responsive Layout**: Wide mode with proper column layouts
* **Color Coding**:
  - Red/Orange: Critical/High severity
  - Green: Strong trust signals
  - Blue: Information displays

## Authentication

The app uses **OAuth token-based authentication** to connect to Lakebase:

1. Generates JWT token via Databricks REST API:
   ```
   POST /api/2.0/postgres/credentials
   Body: {"endpoint": "projects/hackthon/branches/production/endpoints/primary"}
   ```
2. Uses token as PostgreSQL password
3. Token cached for 1 hour
4. Automatic refresh on expiry

## Performance Optimizations

* **Query caching** with 5-minute TTL
* **Connection pooling** for Lakebase
* **Lazy loading** - data fetched only when needed
* **Limit clauses** on large result sets (50-100 rows)
* **Aggregations** performed in database (not in Python)

## Troubleshooting

### Connection Errors

```python
# Test Lakebase connection manually
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
result = w.api_client.do(
    "POST",
    "/api/2.0/postgres/credentials",
    body={"endpoint": "projects/hackthon/branches/production/endpoints/primary"}
)
print(result)
```

### Token Issues

* Verify endpoint name is correct: `projects/hackthon/branches/production/endpoints/primary`
* Check Lakebase project is running
* Ensure user has access to the Lakebase project

### Query Errors

* Check table names match exactly (case-sensitive in PostgreSQL)
* Verify column names exist in tables
* Test queries directly in Lakebase SQL editor first

## Next Steps

1. **Test the app** with actual Lakebase data
2. **Customize styling** (colors, fonts, logos)
3. **Add authentication** (if needed for external access)
4. **Set up monitoring** (track usage, errors)
5. **Schedule data refresh** (if using cached tables)
6. **Add export features** (CSV download for reports)
7. **Implement drill-down** (click facility → detailed view)
8. **Add notifications** (email alerts for critical gaps)

## Support

For issues or questions:
* Check Lakebase sync notebook for data pipeline issues
* Review Databricks app logs: `databricks apps logs healthgpt-care-gap-planner`
* Verify data freshness in Lakebase tables

---

**Created**: June 16, 2026  
**Version**: 1.0  
**Author**: Genie Code AI Assistant