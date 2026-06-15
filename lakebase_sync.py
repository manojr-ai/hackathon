# ============================================================================
# LAKEBASE DATA SYNC SCRIPT
# ============================================================================
# Run this after creating tables in Lakebase to sync data from Delta tables
# ============================================================================

import psycopg2
import pandas as pd
from pyspark.sql import functions as F

print("="*80)
print("LAKEBASE DATA SYNC")
print("="*80)

# TODO: Set your Lakebase credentials
LAKEBASE_USER = "your_user_here"  # Replace with actual user
LAKEBASE_PASSWORD = "your_password_here"  # Replace with actual password

print("\n🔗 Connecting to Lakebase...")
conn = psycopg2.connect(
    host="lakebase-06f92301-4b5f-47eb-95cb-bc02c23df5b8.cloud.databricks.com",
    port=5432,
    database="healthgpt",
    user=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD
)
print("✓ Connected to Lakebase")

# ===========================================================================
# TABLE 1: care_gap_summary
# ===========================================================================
print("\n📊 Syncing Table 1: care_gap_summary...")

df1 = spark.table("workspace.healthgpt.care_gap_silver_ml").select(
    "state",
    "capability",
    F.col("composite_gap_score").cast("decimal(5,2)").alias("gap_score"),
    "gap_severity",
    "confidence_level",
    "intervention_urgency",
    F.col("total_facilities").cast("int").alias("total_facilities"),
    F.col("strong_count").cast("int").alias("strong_count"),
    F.col("partial_count").cast("int").alias("partial_count"),
    F.col("weak_count").cast("int").alias("weak_count"),
    F.col("strong_pct").cast("decimal(5,2)").alias("strong_pct"),
    F.col("ml_gap_score").cast("decimal(5,2)").alias("ml_gap_score"),
    F.col("estimated_underserved_population").alias("underserved_pop"),
    "supply_adequacy",
    "city",
    "geography_type"
).toPandas()

cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE care_gap_summary;")

for _, row in df1.iterrows():
    cursor.execute('''
        INSERT INTO care_gap_summary VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', tuple(row))

conn.commit()
print(f"✓ Synced {len(df1):,} rows to care_gap_summary")

# ===========================================================================
# TABLE 2: facility_detail
# ===========================================================================
print("\n🏥 Syncing Table 2: facility_detail...")

df2 = spark.table("workspace.healthgpt.facilities_silver_ml").select(
    "facility_id",
    "facility_name",
    "state",
    "city",
    "capability",
    "rule_trust_signal",
    F.col("rule_confidence_score").alias("rule_confidence"),
    "ml_trust_signal",
    F.col("ml_trust_score").cast("int").alias("ml_trust_score"),
    F.col("ml_capability_probability").alias("ai_probability"),
    "ml_capability_confidence",
    "dual_score_agreement",
    F.col("trust_delta").cast("int").alias("trust_delta"),
    "score_divergence",
    "review_priority",
    F.col("evidence_count").cast("int").alias("evidence_count"),
    F.col("latitude").cast("decimal(10,6)").alias("latitude"),
    F.col("longitude").cast("decimal(10,6)").alias("longitude"),
    "data_quality"
).toPandas()

cursor.execute("TRUNCATE TABLE facility_detail;")

for _, row in df2.iterrows():
    cursor.execute('''
        INSERT INTO facility_detail VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', tuple(row))

conn.commit()
print(f"✓ Synced {len(df2):,} rows to facility_detail")

# ===========================================================================
# TABLE 3: risk_indicators
# ===========================================================================
print("\n⚠️  Syncing Table 3: risk_indicators...")

df3 = spark.table("workspace.healthgpt.gap_risk_indicators").select(
    "indicator_name",
    F.col("risk_pct").cast("decimal(5,2)").alias("risk_pct"),
    "priority",
    "description"
).toPandas()

cursor.execute("TRUNCATE TABLE risk_indicators;")

for _, row in df3.iterrows():
    cursor.execute('''
        INSERT INTO risk_indicators VALUES (%s,%s,%s,%s)
    ''', tuple(row))

conn.commit()
print(f"✓ Synced {len(df3):,} rows to risk_indicators")

# ===========================================================================
# CLEANUP
# ===========================================================================
cursor.close()
conn.close()

print("\n" + "="*80)
print("✅ DATA SYNC COMPLETE")
print("="*80)
print(f"\nSynced tables:")
print(f"  • care_gap_summary: {len(df1):,} rows")
print(f"  • facility_detail: {len(df2):,} rows")
print(f"  • risk_indicators: {len(df3):,} rows")
print("\n🚀 App is ready to use Lakebase!")
