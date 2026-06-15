# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup and Imports
# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER - NOTEBOOK 4: ML-ENHANCED SILVER
# Track 2: Medical Desert Planner
# ============================================================================
# Purpose: Sync ML predictions back to Silver layer for fast app queries
#          Create denormalized tables with Rule + ML scores side-by-side
# Input: facilities_silver, ml_capability_predictions, ml_trust_predictions,
#        ml_care_need_predictions, facility_trust_scores, care_gap_gold
# Output: facilities_silver_ml (for Facility Evidence Detail screen),
#         care_gap_silver_ml (for Overview & Map screens),
#         planner_workspace tables (for Scenario Planner)
# ============================================================================

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("="*80)
print("HEALTHGPT NOTEBOOK 4: ML-ENHANCED SILVER")
print("="*80)
print(f"\n📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("✓ Imports loaded")

# COMMAND ----------

# DBTITLE 1,Facility-Level ML-Enhanced Silver (For Screen 3: Evidence Detail)
# ============================================================================
# FACILITY-LEVEL ML-ENHANCED SILVER (For UI Screen 3: Facility Evidence Detail)
# ============================================================================
print("\n" + "="*80)
print("FACILITY-LEVEL ML-ENHANCED SILVER")
print("="*80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"
TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"
ML_CAPABILITY_TABLE = "workspace.healthgpt.ml_capability_predictions"
ML_TRUST_TABLE = "workspace.healthgpt.ml_trust_predictions"

print("\n🔧 Joining Rule-Based + ML scores at facility level...")

# Start with rule-based trust scores
df_facility_ml = spark.table(TRUST_TABLE).select(
    "facility_id",
    "capability",
    "facility_name",
    "state",
    "city",
    F.col("trust_signal").alias("rule_trust_signal"),
    F.col("confidence_score").alias("rule_confidence_score"),
    "evidence_count",
    "field_diversity"
)

# Join ML capability predictions
df_facility_ml = df_facility_ml.join(
    spark.table(ML_CAPABILITY_TABLE).select(
        "facility_id",
        "capability",
        F.col("ai_probability").alias("ml_capability_probability"),
        F.col("ml_confidence").alias("ml_capability_confidence")
    ),
    on=["facility_id", "capability"],
    how="left"
)

# Join ML trust predictions
df_facility_ml = df_facility_ml.join(
    spark.table(ML_TRUST_TABLE).select(
        "facility_id",
        "capability",
        F.col("ml_trust_signal"),
        F.col("ml_confidence_score").alias("ml_trust_score"),
        F.col("agreement")
    ),
    on=["facility_id", "capability"],
    how="left"
)

# Add facility metadata
df_facility_ml = df_facility_ml.join(
    spark.table(SILVER_TABLE).select(
        "facility_id",
        "latitude",
        "longitude",
        "officialWebsite",
        "data_quality",
        "number_doctors",
        "bed_capacity"
    ),
    on="facility_id",
    how="left"
)

# Add derived fields for UI
df_facility_ml = df_facility_ml.withColumn(
    "dual_score_agreement",
    F.coalesce(F.col("agreement"), F.lit("UNKNOWN"))
).withColumn(
    "trust_delta",
    F.col("ml_trust_score") - F.col("rule_confidence_score")
).withColumn(
    "score_divergence",
    F.when(F.abs(F.col("trust_delta")) > 20, "HIGH")
     .when(F.abs(F.col("trust_delta")) > 10, "MEDIUM")
     .otherwise("LOW")
).withColumn(
    "review_priority",
    F.when(
        (F.col("dual_score_agreement") == "DISAGREE") & (F.col("score_divergence") == "HIGH"), "URGENT"
    ).when(
        F.col("dual_score_agreement") == "DISAGREE", "HIGH"
    ).otherwise("NORMAL")
).withColumn(
    "sync_date", F.current_timestamp()
)

facility_ml_count = df_facility_ml.count()
print(f"\n✓ Created {facility_ml_count:,} facility-capability records with dual scoring")

# Save
FACILITY_ML_TABLE = "workspace.healthgpt.facilities_silver_ml"
df_facility_ml.write.mode("overwrite").saveAsTable(FACILITY_ML_TABLE)

print(f"\n✅ Facility ML-Enhanced Silver created: {FACILITY_ML_TABLE}")
print(f"   • Records: {facility_ml_count:,}")

# Show dual scoring stats
agreement_count = df_facility_ml.filter(F.col("dual_score_agreement") == "AGREE").count()
agreement_pct = (agreement_count / facility_ml_count) * 100
print(f"\n🔄 Dual Scoring Agreement: {agreement_pct:.1f}%")

urgent_review = df_facility_ml.filter(F.col("review_priority") == "URGENT").count()
print(f"   • Urgent reviews needed: {urgent_review:,}")

# COMMAND ----------

# DBTITLE 1,Geographic ML-Enhanced Silver (For Screens 1 & 2: Overview + Map)
# ============================================================================
# GEOGRAPHIC ML-ENHANCED SILVER (For UI Screens 1 & 2: Overview + Care Gap Map)
# ============================================================================
print("\n" + "="*80)
print("GEOGRAPHIC ML-ENHANCED SILVER")
print("="*80)

GAP_TABLE = "workspace.healthgpt.care_gap_gold"
ML_NEED_TABLE = "workspace.healthgpt.ml_care_need_predictions"

print("\n🌍 Joining Rule-Based Gap Scores + ML Need Predictions...")

# Join rule-based gap scores with ML care need predictions
df_gap_ml = spark.table(GAP_TABLE).join(
    spark.table(ML_NEED_TABLE).select(
        "state",
        "capability",
        F.col("predicted_need_score").alias("ml_need_score"),
        F.col("current_supply_score").alias("ml_supply_score"),
        F.col("demand_supply_gap").alias("ml_gap_score"),
        "supply_adequacy",
        "estimated_underserved_population"
    ),
    on=["state", "capability"],
    how="left"
)

# Add composite metrics
df_gap_ml = df_gap_ml.withColumn(
    "composite_gap_score",
    (F.col("gap_score") * 0.6) + (F.coalesce(F.col("ml_gap_score"), F.lit(0)) * 0.4)
).withColumn(
    "gap_severity",
    F.when(F.col("composite_gap_score") > 75, "CRITICAL")
     .when(F.col("composite_gap_score") > 50, "HIGH")
     .when(F.col("composite_gap_score") > 25, "MEDIUM")
     .otherwise("LOW")
).withColumn(
    "intervention_urgency",
    F.when(
        (F.col("gap_severity") == "CRITICAL") & (F.col("confidence_level") == "HIGH"), "IMMEDIATE"
    ).when(
        F.col("gap_severity").isin(["CRITICAL", "HIGH"]), "URGENT"
    ).when(
        F.col("gap_severity") == "MEDIUM", "MODERATE"
    ).otherwise("LOW")
).withColumn(
    "sync_date", F.current_timestamp()
)

gap_ml_count = df_gap_ml.count()
print(f"\n✓ Created {gap_ml_count} geographic care gap records with ML enhancements")

# Save
GAP_ML_TABLE = "workspace.healthgpt.care_gap_silver_ml"
df_gap_ml.write.mode("overwrite").saveAsTable(GAP_ML_TABLE)

print(f"\n✅ Geographic ML-Enhanced Silver created: {GAP_ML_TABLE}")
print(f"   • Records: {gap_ml_count}")

# Show severity distribution
severity_dist = df_gap_ml.groupBy("gap_severity").count().orderBy("gap_severity").collect()
print("\n📊 Gap Severity Distribution:")
for row in severity_dist:
    pct = (row['count'] / gap_ml_count) * 100
    print(f"   {row.gap_severity}: {row['count']} ({pct:.1f}%)")

# Show top 5 urgent interventions
print("\n⚠️ TOP 5 URGENT INTERVENTIONS:")
top_urgent = spark.table(GAP_ML_TABLE).filter(
    F.col("intervention_urgency") == "IMMEDIATE"
).orderBy(F.col("composite_gap_score").desc()).limit(5).collect()

for i, row in enumerate(top_urgent, 1):
    print(f"   {i}. {row.state} - {row.capability}: Composite Gap={row.composite_gap_score:.1f}, Underserved={row.estimated_underserved_population:,}")

# COMMAND ----------

# DBTITLE 1,Create Planner Workspace Tables (For Screen 5: Scenario Planner)
# ============================================================================
# PLANNER WORKSPACE TABLES (For UI Screen 5: Scenario Planner)
# ============================================================================
print("\n" + "="*80)
print("PLANNER WORKSPACE TABLES")
print("="*80)

print("\n📋 Creating planner persistence tables...")

# 1. Planner Notes
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_notes (
  note_id STRING,
  facility_id STRING,
  capability STRING,
  state STRING,
  note_text STRING,
  note_type STRING,  -- OBSERVATION, CONCERN, ACTION_ITEM
  created_by STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
) USING DELTA
""")
print("✓ planner_notes table created")

# 2. Planner Overrides (manual trust adjustments)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_overrides (
  override_id STRING,
  facility_id STRING,
  capability STRING,
  original_trust_signal STRING,
  override_trust_signal STRING,
  override_reason STRING,
  created_by STRING,
  created_date TIMESTAMP
) USING DELTA
""")
print("✓ planner_overrides table created")

# 3. Planner Scenarios (what-if analysis)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_scenarios (
  scenario_id STRING,
  scenario_name STRING,
  scenario_description STRING,
  state STRING,
  capability STRING,
  hypothetical_changes STRING,  -- JSON: [{"facility_id": "F001", "new_trust": "strong"}]
  projected_gap_score DOUBLE,
  projected_underserved INT,
  created_by STRING,
  created_date TIMESTAMP
) USING DELTA
""")
print("✓ planner_scenarios table created")

# 4. Planner Shortlists (facilities marked for intervention)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_shortlists (
  shortlist_id STRING,
  facility_id STRING,
  capability STRING,
  intervention_type STRING,  -- UPGRADE, BUILD_NEW, MOBILE_UNIT, TRAINING
  estimated_cost STRING,  -- LOW, MEDIUM, HIGH
  priority_rank INT,
  created_by STRING,
  created_date TIMESTAMP
) USING DELTA
""")
print("✓ planner_shortlists table created")

# 5. Planner Review Decisions (human review workflow)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_review_decisions (
  review_id STRING,
  facility_id STRING,
  capability STRING,
  review_status STRING,  -- PENDING, APPROVED, REJECTED, NEEDS_MORE_INFO
  reviewer_notes STRING,
  reviewed_by STRING,
  reviewed_date TIMESTAMP
) USING DELTA
""")
print("✓ planner_review_decisions table created")

print(f"\n✅ All 5 planner workspace tables created")
print(f"\n📅 Notebook 4 Complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n➡️ Next: Run Notebook 5 (Summary & Validation)")

# COMMAND ----------


