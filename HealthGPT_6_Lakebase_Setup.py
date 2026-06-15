# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup and Configuration
# ============================================================================
# HEALTHGPT LAKEBASE SETUP - PostgreSQL Backend for App
# ============================================================================
# Purpose: Sync Delta tables to Lakebase (PostgreSQL) for fast app queries
# Lakebase Project ID: 06f92301-4b5f-47eb-95cb-bc02c23df5b8
# Strategy: Create PostgreSQL tables with indexes, sync from Delta
# ============================================================================

import psycopg2
from pyspark.sql import functions as F
from datetime import datetime
import os

print("="*80)
print("HEALTHGPT LAKEBASE SETUP")
print("="*80)
print(f"\n📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Lakebase connection details
LAKEBASE_PROJECT_ID = "06f92301-4b5f-47eb-95cb-bc02c23df5b8"
LAKEBASE_HOST = "lakebase-06f92301-4b5f-47eb-95cb-bc02c23df5b8.cloud.databricks.com"
LAKEBASE_PORT = 5432
LAKEBASE_DATABASE = "healthgpt"

# Get Databricks token for authentication
try:
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    print("\n✓ Databricks authentication available")
except:
    print("\n⚠️ Running in local mode - will need manual token")

print(f"\n🎯 Target: Lakebase Project {LAKEBASE_PROJECT_ID}")
print(f"   Database: {LAKEBASE_DATABASE}")
print(f"   Strategy: PostgreSQL tables + indexes for sub-second queries")

# Source Delta tables
SOURCE_SCHEMA = "workspace.healthgpt"

# COMMAND ----------

# DBTITLE 1,Create Lakebase Database Schema
# ============================================================================
# CREATE DATABASE IN LAKEBASE
# ============================================================================
# Note: Database should be created via Lakebase UI first, then we create tables
# ============================================================================

print("\n" + "="*80)
print("LAKEBASE DATABASE SETUP")
print("="*80)

print("\n📝 Instructions to create Lakebase database:")
print("   1. Go to: https://dbc-f49e9aec-67ba.cloud.databricks.com/lakebase/projects/06f92301-4b5f-47eb-95cb-bc02c23df5b8")
print("   2. Click 'Create Database'")
print("   3. Name: healthgpt")
print("   4. Description: HealthGPT Care Gap Trust Planner backend")
print("   5. Click 'Create'")

print("\n⏳ Assuming database 'healthgpt' is created...")
print("\n🛠️ We'll create the following PostgreSQL tables:")
print("   1. care_gap_summary (923 rows) - Primary dashboard data")
print("   2. facility_detail (27,978 rows) - Facility evidence detail")
print("   3. facility_evidence (75,687 rows) - Keyword evidence")
print("   4. risk_indicators (10 rows) - Risk factors")
print("   5. ml_model_registry (3 rows) - Model metadata")
print("   6-10. planner_* tables - User workspace")
