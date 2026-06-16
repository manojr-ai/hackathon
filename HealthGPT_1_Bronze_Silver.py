# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup and Imports
# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER - NOTEBOOK 1: BRONZE + SILVER LAYERS
# Track 2: Medical Desert Planner
# ============================================================================
# Purpose: Load raw facility data and clean/standardize for downstream processing
# Output: workspace.healthgpt.facilities_bronze, workspace.healthgpt.facilities_silver
# ============================================================================

import json
import re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("="*80)
print("HEALTHGPT NOTEBOOK 1: BRONZE + SILVER LAYERS")
print("="*80)
print(f"\n📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n✓ Imports loaded")

# Ensure schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.healthgpt")
print("✓ Schema verified: workspace.healthgpt")

# COMMAND ----------

# DBTITLE 1,Bronze Layer: Load Raw Facility Data
# ============================================================================
# BRONZE LAYER: Load Raw Facility Data
# ============================================================================
print("\n" + "="*80)
print("BRONZE LAYER: LOADING RAW FACILITY DATA")
print("="*80)

SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
BRONZE_TABLE = "workspace.healthgpt.facilities_bronze"

print(f"\n📂 Source: {SOURCE_TABLE}")
print(f"🎯 Target: {BRONZE_TABLE}")

# Load raw data - preserve all columns for traceability
df_bronze = spark.table(SOURCE_TABLE)

raw_count = df_bronze.count()
raw_cols = len(df_bronze.columns)

print(f"\n✓ Loaded {raw_count:,} facilities")
print(f"✓ Columns: {raw_cols}")

# Write to Bronze (overwrite for idempotency)
df_bronze.write.mode("overwrite").saveAsTable(BRONZE_TABLE)

# Verify
bronze_count = spark.table(BRONZE_TABLE).count()
print(f"\n✅ Bronze table created: {BRONZE_TABLE}")
print(f"   • Records: {bronze_count:,}")
print(f"   • Columns: {raw_cols}")

# COMMAND ----------

# DBTITLE 1,Silver Layer: Parse JSON and Standardize
# ============================================================================
# SILVER LAYER: Clean and Standardize Facility Data
# ============================================================================
print("\n" + "="*80)
print("SILVER LAYER: CLEANING AND STANDARDIZING DATA")
print("="*80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

print(f"\n📂 Source: {BRONZE_TABLE}")
print(f"🎯 Target: {SILVER_TABLE}")

# Helper function to parse JSON arrays
def parse_json_array_udf(column_name):
    def parse_json(text):
        if text is None or text == '':
            return []
        try:
            parsed = json.loads(text) if isinstance(text, str) else text
            return parsed if isinstance(parsed, list) else []
        except:
            return []
    return F.udf(parse_json, ArrayType(StringType()))

# Load bronze
df = spark.table(BRONZE_TABLE)

print("\n🔧 Parsing JSON arrays...")

# Parse JSON array columns and concatenate to text
df_silver = df.withColumn(
    "specialties_list", parse_json_array_udf("specialties")(F.col("specialties"))
).withColumn(
    "specialties_text", F.concat_ws(", ", F.col("specialties_list"))
).withColumn(
    "procedures_list", parse_json_array_udf("procedure")(F.col("procedure"))
).withColumn(
    "procedures_text", F.concat_ws(", ", F.col("procedures_list"))
).withColumn(
    "equipment_list", parse_json_array_udf("equipment")(F.col("equipment"))
).withColumn(
    "equipment_text", F.concat_ws(", ", F.col("equipment_list"))
).withColumn(
    "capabilities_list", parse_json_array_udf("capability")(F.col("capability"))
).withColumn(
    "capabilities_text", F.concat_ws(", ", F.col("capabilities_list"))
)

print("✓ JSON arrays parsed")
print("\n🌍 Standardizing geography...")

# Standardize geography
df_silver = df_silver.withColumn(
    "state", F.upper(F.trim(F.col("address_stateOrRegion")))
).withColumn(
    "city", F.initcap(F.trim(F.col("address_city")))
).withColumn(
    "pin_code", F.trim(F.col("address_zipOrPostcode"))
)

print("✓ Geography standardized")
print("\n📊 Adding data quality flags...")

# Data quality flags
df_silver = df_silver.withColumn(
    "has_description", F.when(F.length(F.col("description")) > 0, 1).otherwise(0)
).withColumn(
    "has_specialties", F.when(F.size(F.col("specialties_list")) > 0, 1).otherwise(0)
).withColumn(
    "has_procedures", F.when(F.size(F.col("procedures_list")) > 0, 1).otherwise(0)
).withColumn(
    "has_equipment", F.when(F.size(F.col("equipment_list")) > 0, 1).otherwise(0)
).withColumn(
    "data_quality",
    F.when(
        (F.col("has_description") + F.col("has_specialties") + 
         F.col("has_procedures") + F.col("has_equipment")) >= 3, "COMPLETE"
    ).when(
        (F.col("has_description") + F.col("has_specialties") + 
         F.col("has_procedures") + F.col("has_equipment")) == 2, "PARTIAL"
    ).otherwise("SPARSE")
)

print("✓ Quality flags added")
print("\n✂️ Selecting relevant columns...")

# Select relevant columns
df_silver = df_silver.select(
    F.col("unique_id").alias("facility_id"),
    F.col("name").alias("facility_name"),
    "state", "city", "pin_code",
    F.col("address_line1"), F.col("address_line2"),
    F.col("latitude"), F.col("longitude"),
    F.col("description"),
    "specialties_text", "procedures_text", "equipment_text", "capabilities_text",
    F.col("numberDoctors").alias("number_doctors"), 
    F.col("capacity").alias("bed_capacity"),
    F.col("officialWebsite"), F.col("source_urls"),
    "data_quality",
    F.current_timestamp().alias("processed_date")
)

print("✓ Columns selected")
print("\n🔄 Deduplicating...")

# Deduplicate
window = Window.partitionBy("facility_id").orderBy(F.col("processed_date").desc())
df_silver = df_silver.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

silver_count = df_silver.count()
print(f"\n✓ Processed {silver_count:,} facilities")

# Quality distribution
quality_dist = df_silver.groupBy("data_quality").count().orderBy("data_quality").collect()
print("\n📊 Data Quality Distribution:")
for row in quality_dist:
    print(f"   {row.data_quality}: {row['count']:,}")

# Write Silver
df_silver.write.mode("overwrite").saveAsTable(SILVER_TABLE)

print(f"\n✅ Silver table created: {SILVER_TABLE}")
print(f"   • Records: {silver_count:,}")
print(f"\n📅 Notebook 1 Complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n➡️  Next: Run Notebook 2 (Gold Layer)")

# COMMAND ----------

# DBTITLE 1,Backup Silver Table
# ============================================================================
# BACKUP SILVER TABLE
# ============================================================================
print("\n" + "="*80)
print("BACKING UP SILVER TABLE")
print("="*80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"
BACKUP_TABLE = "workspace.healthgpt.facilities_silver_backup"

print(f"\n📂 Source: {SILVER_TABLE}")
print(f"💾 Backup: {BACKUP_TABLE}")

# Create backup table
spark.sql(f"""
    CREATE OR REPLACE TABLE {BACKUP_TABLE}
    AS SELECT * FROM {SILVER_TABLE}
""")

# Verify backup
original_count = spark.table(SILVER_TABLE).count()
backup_count = spark.table(BACKUP_TABLE).count()

print(f"\n✅ Backup created successfully!")
print(f"   • Original table: {original_count:,} records")
print(f"   • Backup table: {backup_count:,} records")
print(f"   • Backup location: {BACKUP_TABLE}")

# Show data quality distribution in original
print("\n📊 Data Quality Distribution (Original):")
quality_dist = spark.table(SILVER_TABLE).groupBy("data_quality").count().orderBy("data_quality").collect()
for row in quality_dist:
    print(f"   {row.data_quality}: {row['count']:,}")

# COMMAND ----------

# DBTITLE 1,Delete Non-Complete Records from Silver
# ============================================================================
# DELETE NON-COMPLETE RECORDS FROM SILVER TABLE
# ============================================================================
print("\n" + "="*80)
print("DELETING NON-COMPLETE RECORDS FROM SILVER TABLE")
print("="*80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

print(f"\n🗑️  Target: {SILVER_TABLE}")
print(f"⚠️  Condition: data_quality != 'COMPLETE'")

# Count records before deletion
before_count = spark.table(SILVER_TABLE).count()
complete_count = spark.table(SILVER_TABLE).filter("data_quality = 'COMPLETE'").count()
non_complete_count = spark.table(SILVER_TABLE).filter("data_quality != 'COMPLETE'").count()

print(f"\n📊 Before deletion:")
print(f"   • Total records: {before_count:,}")
print(f"   • COMPLETE records: {complete_count:,}")
print(f"   • Non-COMPLETE records (to delete): {non_complete_count:,}")

# Execute DELETE statement
print(f"\n🛡️  Executing DELETE...")
spark.sql(f"""
    DELETE FROM {SILVER_TABLE}
    WHERE data_quality != 'COMPLETE' OR upper(state) not in (
  'ANDHRA PRADESH', 'ARUNACHAL PRADESH', 'ASSAM', 'BIHAR', 'CHHATTISGARH', 'GOA', 'GUJARAT', 
  'HARYANA', 'HIMACHAL PRADESH', 'JHARKHAND', 'KARNATAKA', 'KERALA', 'MADHYA PRADESH', 
  'MAHARASHTRA', 'MANIPUR', 'MEGHALAYA', 'MIZORAM', 'NAGALAND', 'ODISHA', 'PUNJAB', 
  'RAJASTHAN', 'SIKKIM', 'TAMIL NADU', 'TELANGANA', 'TRIPURA', 'UTTAR PRADESH', 
  'UTTARAKHAND', 'WEST BENGAL', 'DELHI', 'PUDUCHERRY', 'JAMMU AND KASHMIR', 'LADAKH', 
  'ANDAMAN AND NICOBAR ISLANDS', 'CHANDIGARH', 'DADRA AND NAGAR HAVELI AND DAMAN AND DIU', 
  'LAKSHADWEEP'
)
""")

# Count records after deletion
after_count = spark.table(SILVER_TABLE).count()
deleted_count = before_count - after_count

print(f"\n✅ Deletion complete!")
print(f"   • Records deleted: {deleted_count:,}")
print(f"   • Records remaining: {after_count:,}")
print(f"   • All remaining records have data_quality = 'COMPLETE'")

# Verify - all remaining records should be COMPLETE
quality_check = spark.table(SILVER_TABLE).groupBy("data_quality").count().collect()
print(f"\n🔍 Verification:")
for row in quality_check:
    print(f"   {row.data_quality}: {row['count']:,}")

# COMMAND ----------

# DBTITLE 1,Create App-Ready Lakebase Tables
# ============================================================================
# APP-READY LAKEBASE TABLES (Summary tables for fast app queries)
# ============================================================================
print("\n" + "="*80)
print("CREATING APP-READY LAKEBASE TABLES")
print("="*80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

# 1. STATE SUMMARY (For Overview & Map screens)
print("\n🌍 Creating state_summary table...")
df_state_summary = spark.table(SILVER_TABLE).groupBy("state").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("data_quality") == "COMPLETE", 1).otherwise(0)).alias("complete_facilities"),
    F.sum(F.when(F.col("data_quality") == "PARTIAL", 1).otherwise(0)).alias("partial_facilities"),
    F.sum(F.when(F.col("data_quality") == "SPARSE", 1).otherwise(0)).alias("sparse_facilities"),
    F.countDistinct("city").alias("cities_count"),
    F.avg("latitude").alias("center_lat"),
    F.avg("longitude").alias("center_lon")
).withColumn(
    "data_completeness_pct", 
    (F.col("complete_facilities") / F.col("total_facilities")) * 100
).withColumn(
    "gap_score",
    100 - F.col("data_completeness_pct")  # Simple gap score: 100 - completeness
).withColumn(
    "gap_severity",
    F.when(F.col("gap_score") > 75, "CRITICAL")
     .when(F.col("gap_score") > 50, "HIGH")
     .when(F.col("gap_score") > 25, "MEDIUM")
     .otherwise("LOW")
).orderBy(F.col("gap_score").desc())

df_state_summary.write.mode("overwrite").saveAsTable("workspace.healthgpt.state_summary")
print(f"✅ state_summary created: {df_state_summary.count()} states")

# 2. FACILITY SUMMARY (For Facility Evidence Detail screen)
print("\n🏝️ Creating facility_summary table...")
df_facility_summary = spark.table(SILVER_TABLE).select(
    "facility_id",
    "facility_name",
    "state",
    "city",
    "latitude",
    "longitude",
    "data_quality",
    "number_doctors",
    "bed_capacity",
    "officialWebsite",
    F.length("description").alias("description_length"),
    F.length("specialties_text").alias("specialties_length"),
    F.length("procedures_text").alias("procedures_length"),
    F.length("equipment_text").alias("equipment_length"),
    F.when(
        (F.length("description") > 100) & 
        (F.length("specialties_text") > 50) &
        (F.length("procedures_text") > 50), "HIGH"
    ).when(
        (F.length("description") > 50) | 
        (F.length("specialties_text") > 25), "MEDIUM"
    ).otherwise("LOW").alias("evidence_strength")
)

df_facility_summary.write.mode("overwrite").saveAsTable("workspace.healthgpt.facility_summary")
print(f"✅ facility_summary created: {df_facility_summary.count():,} facilities")

# 3. RISK INDICATORS (For Overview screen)
print("\n⚠️ Creating risk_indicators_summary table...")

total_facilities = spark.table(SILVER_TABLE).count()
total_states = spark.table(SILVER_TABLE).select("state").distinct().count()
avg_facilities_per_state = total_facilities / total_states

risk_data = [
    {
        "indicator_id": 1,
        "indicator_name": "Sparse data quality",
        "risk_value": spark.table(SILVER_TABLE).filter(F.col("data_quality") == "SPARSE").count(),
        "risk_pct": (spark.table(SILVER_TABLE).filter(F.col("data_quality") == "SPARSE").count() / total_facilities) * 100,
        "priority": "MEDIUM"
    },
    {
        "indicator_id": 2,
        "indicator_name": "Missing location data",
        "risk_value": spark.table(SILVER_TABLE).filter(F.col("latitude").isNull()).count(),
        "risk_pct": (spark.table(SILVER_TABLE).filter(F.col("latitude").isNull()).count() / total_facilities) * 100,
        "priority": "HIGH"
    },
    {
        "indicator_id": 3,
        "indicator_name": "No official website",
        "risk_value": spark.table(SILVER_TABLE).filter(F.col("officialWebsite").isNull()).count(),
        "risk_pct": (spark.table(SILVER_TABLE).filter(F.col("officialWebsite").isNull()).count() / total_facilities) * 100,
        "priority": "LOW"
    },
    {
        "indicator_id": 4,
        "indicator_name": "Missing capacity data",
        "risk_value": spark.table(SILVER_TABLE).filter(F.col("bed_capacity").isNull()).count(),
        "risk_pct": (spark.table(SILVER_TABLE).filter(F.col("bed_capacity").isNull()).count() / total_facilities) * 100,
        "priority": "MEDIUM"
    },
    {
        "indicator_id": 5,
        "indicator_name": "Missing doctor count",
        "risk_value": spark.table(SILVER_TABLE).filter(F.col("number_doctors").isNull()).count(),
        "risk_pct": (spark.table(SILVER_TABLE).filter(F.col("number_doctors").isNull()).count() / total_facilities) * 100,
        "priority": "LOW"
    },
    {
        "indicator_id": 6,
        "indicator_name": "Low facility density",
        "risk_value": spark.table(SILVER_TABLE).groupBy("state").count().filter(F.col("count") < avg_facilities_per_state * 0.5).count(),
        "risk_pct": (spark.table(SILVER_TABLE).groupBy("state").count().filter(F.col("count") < avg_facilities_per_state * 0.5).count() / total_states) * 100,
        "priority": "HIGH"
    }
]

df_risk_indicators = spark.createDataFrame(risk_data).withColumn(
    "analysis_date", F.current_timestamp()
).orderBy(F.col("risk_pct").desc())

df_risk_indicators.write.mode("overwrite").saveAsTable("workspace.healthgpt.risk_indicators_summary")
print(f"✅ risk_indicators_summary created: {df_risk_indicators.count()} indicators")

print("\n" + "="*80)
print("APP-READY LAKEBASE TABLES COMPLETE")
print("="*80)
print("\nCreated tables:")
print("  • workspace.healthgpt.state_summary (for Overview & Map)")
print("  • workspace.healthgpt.facility_summary (for Evidence Detail)")
print("  • workspace.healthgpt.risk_indicators_summary (for Overview)")
print("\n➡️ Next: Build Streamlit app (healthgpt_app.py)")
