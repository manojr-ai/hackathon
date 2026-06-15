# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Phase 1 Overview
# MAGIC %md
# MAGIC # HealthGPT Policy Command Center - Phase 1
# MAGIC ## Data Foundation: Bronze → Silver → Gold Pipeline
# MAGIC
# MAGIC This notebook implements the medallion architecture for NFHS-5 district health indicators.
# MAGIC
# MAGIC ### Pipeline Overview:
# MAGIC - **Bronze**: Raw NFHS data (already exists: `nfhs_5_district_health_indicators`)
# MAGIC - **Silver**: Cleaned and normalized indicators  
# MAGIC - **Gold**: Feature-engineered risk flags and composites for ML models
# MAGIC
# MAGIC ### Data Flow:
# MAGIC Raw CSV → Bronze (698 districts) → Silver (cleaned) → Gold (ML-ready features)
# MAGIC
# MAGIC ### Architecture:
# MAGIC ```
# MAGIC Bronze Layer          Silver Layer           Gold Layer
# MAGIC ├─ Raw NFHS data  →  ├─ Cleaned values  →  ├─ Risk scores
# MAGIC ├─ 109 indicators    ├─ Null handling      ├─ Domain composites
# MAGIC ├─ 698 districts     ├─ Type conversion    ├─ Binary flags
# MAGIC └─ Data quality      └─ Standardization    └─ ML features
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Configuration and Setup
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

# Configuration
CATALOG = "databricks_virtue_foundation_dataset_dais_2026"
SCHEMA = "virtue_foundation_dataset"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.nfhs_5_district_health_indicators"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.health_indicators_silver"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.health_indicators_gold"

print("✓ Pipeline configuration loaded")
print(f"  Bronze: {BRONZE_TABLE}")
print(f"  Silver: {SILVER_TABLE}")
print(f"  Gold: {GOLD_TABLE}")

# COMMAND ----------

# DBTITLE 1,Inspect Bronze Data Quality
# MAGIC %sql
# MAGIC -- Verify Bronze table exists and check data quality issues
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_districts,
# MAGIC   COUNT(DISTINCT state_ut) as total_states,
# MAGIC   COUNT(*) - COUNT(mothers_who_had_at_least_4_anc_visits_lb5y_pct) as missing_anc,
# MAGIC   COUNT(*) - COUNT(non_pregnant_w15_49_who_are_anaemic_lt_12_0_g_dl_22_pct) as missing_anemia
# MAGIC FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.nfhs_5_district_health_indicators

# COMMAND ----------

# DBTITLE 1,Silver Layer - Data Cleaning
# MAGIC %md
# MAGIC ## Step 1: Bronze → Silver Transformation
# MAGIC
# MAGIC Clean data quality issues:
# MAGIC - Remove parentheses `()` from percentage values 
# MAGIC - Replace asterisks `*` with NULL
# MAGIC - Convert string percentages to DOUBLE
# MAGIC - Handle missing values
# MAGIC - Standardize district and state names
# MAGIC - Add data quality scoring

# COMMAND ----------

# DBTITLE 1,Silver Transformation - Data Cleaning
# Read Bronze table
bronze_df = spark.table(BRONZE_TABLE)

print(f"Bronze table loaded: {bronze_df.count()} districts")

# UDF to clean percentage strings: "(64.2)" -> 64.2, "*" -> NULL
def clean_percentage(value):
    if value is None:
        return None
    value_str = str(value).strip()
    if value_str in ['*', '(*)']:
        return None
    # Remove parentheses
    value_str = value_str.replace('(', '').replace(')', '').strip()
    if value_str == '':
        return None
    try:
        return float(value_str)
    except:
        return None

clean_pct_udf = F.udf(clean_percentage, DoubleType())

# Apply cleaning to all key indicators
silver_df = bronze_df.select(
    F.col("district_name"),
    F.col("state_ut"),
    F.col("households_surveyed"),
    F.col("women_15_49_interviewed"),
    F.col("men_15_54_interviewed"),
    
    # Clean key indicators for HealthGPT
    clean_pct_udf(F.col("mothers_who_had_at_least_4_anc_visits_lb5y_pct")).alias("anc_4_visits_pct"),
    F.col("non_pregnant_w15_49_who_are_anaemic_lt_12_0_g_dl_22_pct").alias("women_anemia_pct"),
    clean_pct_udf(F.col("child_u5_who_are_stunted_height_for_age_18_pct")).alias("child_stunting_pct"),
    clean_pct_udf(F.col("child_12_23m_fully_vaccinated_based_on_information_from_eit_pct")).alias("vaccination_full_pct"),
    F.col("hh_use_improved_sanitation_pct").alias("sanitation_improved_pct"),
    F.col("institutional_birth_5y_pct").alias("institutional_birth_pct"),
    clean_pct_udf(F.col("mothers_who_consumed_ifa_for_100_days_or_more_when_they_wer_pct")).alias("ifa_100_days_pct"),
    clean_pct_udf(F.col("child_6_59m_who_are_anaemic_lt_11_0_g_dl_22_pct")).alias("child_anemia_pct"),
    F.col("hh_improved_water_pct").alias("water_improved_pct"),
    F.col("households_using_clean_fuel_for_cooking_pct").alias("clean_fuel_pct"),
    F.col("women_age_15_49_who_are_literate_pct").alias("women_literacy_pct"),
    clean_pct_udf(F.col("w20_24_married_before_age_18_years_pct")).alias("child_marriage_pct"),
    
    # Add timestamp
    F.current_timestamp().alias("silver_created_at")
)

# Calculate data quality score
silver_df = silver_df.withColumn(
    "missing_count",
    (F.when(F.col("anc_4_visits_pct").isNull(), 1).otherwise(0) +
     F.when(F.col("women_anemia_pct").isNull(), 1).otherwise(0) +
     F.when(F.col("child_stunting_pct").isNull(), 1).otherwise(0) +
     F.when(F.col("vaccination_full_pct").isNull(), 1).otherwise(0) +
     F.when(F.col("sanitation_improved_pct").isNull(), 1).otherwise(0))
).withColumn(
    "data_quality_score",
    (5 - F.col("missing_count")) / 5.0 * 100
)

# Create unique district ID
silver_df = silver_df.withColumn(
    "district_id",
    F.concat_ws("_", 
                F.regexp_replace(F.lower(F.col("state_ut")), " ", "_"), 
                F.regexp_replace(F.lower(F.col("district_name")), " ", "_"))
)

print(f"✓ Silver transformation complete")
print(f"  Rows: {silver_df.count()}")

# COMMAND ----------

# DBTITLE 1,Write Silver Table
# Write to Silver table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"✓ Silver table created: {SILVER_TABLE}")

# Display sample
display(spark.table(SILVER_TABLE).limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Layer - Feature Engineering
# MAGIC %md
# MAGIC ## Step 2: Silver → Gold Transformation
# MAGIC
# MAGIC Feature engineering for ML models:
# MAGIC - **Composite risk scores** by health domain (maternal, child, sanitation)
# MAGIC - **Binary risk flags** using evidence-based thresholds
# MAGIC - **Normalized features** (0-100 scale)
# MAGIC - **Overall risk scoring** with weighted components
# MAGIC - **Risk categorization** (LOW, MEDIUM, HIGH, CRITICAL)
# MAGIC
# MAGIC ### Risk Flag Thresholds:
# MAGIC - **High Anemia**: Women anemia > 50%
# MAGIC - **Low ANC**: ANC 4+ visits < 50%
# MAGIC - **Low Vaccination**: Full vaccination < 75%
# MAGIC - **Poor Sanitation**: Improved sanitation < 70%
# MAGIC - **High Stunting**: Child stunting > 35%

# COMMAND ----------

# DBTITLE 1,Gold Feature Engineering
# Read Silver table
silver_df = spark.table(SILVER_TABLE)

# Gold transformation with feature engineering
gold_df = silver_df.select(
    F.col("district_id"),
    F.col("district_name"),
    F.col("state_ut"),
    
    # Core indicators
    F.col("anc_4_visits_pct"),
    F.col("women_anemia_pct"),
    F.col("child_stunting_pct"),
    F.col("vaccination_full_pct"),
    F.col("sanitation_improved_pct"),
    F.col("institutional_birth_pct"),
    F.col("ifa_100_days_pct"),
    F.col("child_anemia_pct"),
    F.col("water_improved_pct"),
    F.col("clean_fuel_pct"),
    F.col("women_literacy_pct"),
    F.col("child_marriage_pct"),
    F.col("data_quality_score")
)

# Fill nulls with state-level median
window_state = Window.partitionBy("state_ut")

gold_df = gold_df.withColumn(
    "anc_4_visits_pct",
    F.coalesce(F.col("anc_4_visits_pct"), 
               F.percentile_approx("anc_4_visits_pct", 0.5).over(window_state))
).withColumn(
    "women_anemia_pct",
    F.coalesce(F.col("women_anemia_pct"), 
               F.percentile_approx("women_anemia_pct", 0.5).over(window_state))
).withColumn(
    "vaccination_full_pct",
    F.coalesce(F.col("vaccination_full_pct"), 
               F.percentile_approx("vaccination_full_pct", 0.5).over(window_state))
)

# Composite scores by health domain
gold_df = gold_df.withColumn(
    "maternal_care_composite",
    (F.coalesce(F.col("anc_4_visits_pct"), F.lit(50.0)) * 0.5 + 
     F.coalesce(F.col("institutional_birth_pct"), F.lit(70.0)) * 0.3 +
     F.coalesce(F.col("ifa_100_days_pct"), F.lit(40.0)) * 0.2)
).withColumn(
    "child_health_composite",
    (100 - F.coalesce(F.col("child_stunting_pct"), F.lit(30.0))) * 0.4 +
    F.coalesce(F.col("vaccination_full_pct"), F.lit(70.0)) * 0.4 +
    (100 - F.coalesce(F.col("child_anemia_pct"), F.lit(50.0))) * 0.2
).withColumn(
    "sanitation_composite",
    (F.coalesce(F.col("sanitation_improved_pct"), F.lit(60.0)) * 0.5 +
     F.coalesce(F.col("water_improved_pct"), F.lit(70.0)) * 0.3 +
     F.coalesce(F.col("clean_fuel_pct"), F.lit(50.0)) * 0.2)
)

print("✓ Composite health scores calculated")

# COMMAND ----------

# DBTITLE 1,Binary Risk Flags and Overall Scoring
# Binary risk flags (evidence-based thresholds)
gold_df = gold_df.withColumn(
    "high_anemia_flag",
    F.when(F.col("women_anemia_pct") > 50, True).otherwise(False)
).withColumn(
    "low_anc_flag",
    F.when(F.col("anc_4_visits_pct") < 50, True).otherwise(False)
).withColumn(
    "low_vaccination_flag",
    F.when(F.col("vaccination_full_pct") < 75, True).otherwise(False)
).withColumn(
    "poor_sanitation_flag",
    F.when(F.col("sanitation_improved_pct") < 70, True).otherwise(False)
).withColumn(
    "high_child_stunting_flag",
    F.when(F.col("child_stunting_pct") > 35, True).otherwise(False)
).withColumn(
    "high_child_marriage_flag",
    F.when(F.col("child_marriage_pct") > 20, True).otherwise(False)
)

# Count triggered risk flags
gold_df = gold_df.withColumn(
    "triggered_risk_count",
    (F.col("high_anemia_flag").cast("int") +
     F.col("low_anc_flag").cast("int") +
     F.col("low_vaccination_flag").cast("int") +
     F.col("poor_sanitation_flag").cast("int") +
     F.col("high_child_stunting_flag").cast("int"))
)

# Overall risk score (weighted composite, 0-100 scale)
gold_df = gold_df.withColumn(
    "overall_risk_score",
    F.round(
        # Invert maternal and child health (higher composite = lower risk)
        ((100 - F.col("maternal_care_composite")) * 0.35 +
         (100 - F.col("child_health_composite")) * 0.30 +
         (100 - F.col("sanitation_composite")) * 0.20 +
         F.col("women_anemia_pct") * 0.15),
        1
    )
).withColumn(
    "risk_category",
    F.when(F.col("overall_risk_score") >= 75, "CRITICAL")
     .when(F.col("overall_risk_score") >= 60, "HIGH")
     .when(F.col("overall_risk_score") >= 40, "MEDIUM")
     .otherwise("LOW")
)

# Domain-specific risk scores (0-100 scale, higher = worse)
gold_df = gold_df.withColumn(
    "maternal_risk_score",
    F.round(100 - F.col("maternal_care_composite"), 0)
).withColumn(
    "anemia_risk_score",
    F.round(F.col("women_anemia_pct"), 0)
).withColumn(
    "child_nutrition_risk_score",
    F.round(F.coalesce(F.col("child_stunting_pct"), F.lit(30.0)), 0)
).withColumn(
    "vaccination_risk_score",
    F.round(100 - F.coalesce(F.col("vaccination_full_pct"), F.lit(70.0)), 0)
).withColumn(
    "sanitation_risk_score",
    F.round(100 - F.col("sanitation_composite"), 0)
)

# Add metadata
gold_df = gold_df.withColumn("gold_created_at", F.current_timestamp())

print(f"✓ Gold feature engineering complete")
print(f"  Districts processed: {gold_df.count()}")

# COMMAND ----------

# DBTITLE 1,Write Gold Table
# Write to Gold table
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

print(f"✓ Gold table created: {GOLD_TABLE}")
print(f"  Ready for ML model training and Genie integration!")

# Display high-risk districts
display(spark.table(GOLD_TABLE).select(
    "district_name", "state_ut", "overall_risk_score", "risk_category",
    "maternal_risk_score", "anemia_risk_score", "vaccination_risk_score",
    "high_anemia_flag", "low_anc_flag", "triggered_risk_count"
).orderBy(F.desc("overall_risk_score")).limit(20))

# COMMAND ----------

# DBTITLE 1,Validation: High-Risk Districts
# MAGIC %sql
# MAGIC -- Find CRITICAL and HIGH risk districts with triggered flags
# MAGIC SELECT 
# MAGIC   district_name,
# MAGIC   state_ut,
# MAGIC   overall_risk_score,
# MAGIC   risk_category,
# MAGIC   maternal_risk_score,
# MAGIC   anemia_risk_score,
# MAGIC   vaccination_risk_score,
# MAGIC   triggered_risk_count,
# MAGIC   CASE 
# MAGIC     WHEN high_anemia_flag THEN 'High Anemia, '
# MAGIC     ELSE ''
# MAGIC   END ||
# MAGIC   CASE 
# MAGIC     WHEN low_anc_flag THEN 'Low ANC, '
# MAGIC     ELSE ''
# MAGIC   END ||
# MAGIC   CASE 
# MAGIC     WHEN low_vaccination_flag THEN 'Low Vaccination, '
# MAGIC     ELSE ''
# MAGIC   END AS triggered_flags
# MAGIC FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.health_indicators_gold
# MAGIC WHERE risk_category IN ('CRITICAL', 'HIGH')
# MAGIC ORDER BY overall_risk_score DESC
# MAGIC LIMIT 25

# COMMAND ----------

# DBTITLE 1,Data Quality and Risk Distribution
# MAGIC %sql
# MAGIC -- Data quality summary by risk category
# MAGIC SELECT 
# MAGIC   risk_category,
# MAGIC   COUNT(*) as district_count,
# MAGIC   ROUND(AVG(overall_risk_score), 1) as avg_risk_score,
# MAGIC   ROUND(AVG(data_quality_score), 1) as avg_data_quality,
# MAGIC   SUM(CASE WHEN high_anemia_flag THEN 1 ELSE 0 END) as high_anemia_count,
# MAGIC   SUM(CASE WHEN low_anc_flag THEN 1 ELSE 0 END) as low_anc_count,
# MAGIC   SUM(CASE WHEN low_vaccination_flag THEN 1 ELSE 0 END) as low_vaccination_count,
# MAGIC   SUM(CASE WHEN poor_sanitation_flag THEN 1 ELSE 0 END) as poor_sanitation_count
# MAGIC FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.health_indicators_gold
# MAGIC GROUP BY risk_category
# MAGIC ORDER BY 
# MAGIC   CASE risk_category
# MAGIC     WHEN 'CRITICAL' THEN 1
# MAGIC     WHEN 'HIGH' THEN 2
# MAGIC     WHEN 'MEDIUM' THEN 3
# MAGIC     ELSE 4
# MAGIC   END

# COMMAND ----------

# DBTITLE 1,Demo Districts - Nicobars Profile
# MAGIC %sql
# MAGIC -- Check the Nicobars district profile (from UI mockup)
# MAGIC SELECT 
# MAGIC   district_name,
# MAGIC   state_ut,
# MAGIC   overall_risk_score,
# MAGIC   risk_category,
# MAGIC   
# MAGIC   -- Key indicators
# MAGIC   ROUND(anc_4_visits_pct, 1) as anc_4_visits,
# MAGIC   ROUND(women_anemia_pct, 1) as women_anemia,
# MAGIC   ROUND(vaccination_full_pct, 1) as vaccination,
# MAGIC   ROUND(sanitation_improved_pct, 1) as sanitation,
# MAGIC   ROUND(child_stunting_pct, 1) as child_stunting,
# MAGIC   
# MAGIC   -- Domain risk scores
# MAGIC   maternal_risk_score,
# MAGIC   anemia_risk_score,
# MAGIC   vaccination_risk_score,
# MAGIC   sanitation_risk_score,
# MAGIC   
# MAGIC   -- Triggered flags
# MAGIC   high_anemia_flag,
# MAGIC   low_anc_flag,
# MAGIC   low_vaccination_flag,
# MAGIC   triggered_risk_count
# MAGIC FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.health_indicators_gold
# MAGIC WHERE district_name LIKE '%Nicobar%'
# MAGIC ORDER BY district_name

# COMMAND ----------

# DBTITLE 1,Phase 1 Complete
# MAGIC %md
# MAGIC ## ✅ Phase 1 Complete!
# MAGIC
# MAGIC ### Created Tables:
# MAGIC 1. **Silver**: `health_indicators_silver` - Cleaned indicators (698 districts)
# MAGIC    - Data type normalization
# MAGIC    - Null handling with state-level imputation
# MAGIC    - Data quality scoring
# MAGIC
# MAGIC 2. **Gold**: `health_indicators_gold` - ML-ready features with risk scores
# MAGIC    - Overall risk score (0-100) and risk category (LOW/MEDIUM/HIGH/CRITICAL)
# MAGIC    - Domain-specific risk scores (maternal, anemia, vaccination, sanitation, child nutrition)
# MAGIC    - Binary risk flags for rule-based policy triggers
# MAGIC    - Composite health indices
# MAGIC    - Triggered risk count
# MAGIC
# MAGIC ### Key Features in Gold Table:
# MAGIC | Feature | Description | Use Case |
# MAGIC |---------|-------------|----------|
# MAGIC | `overall_risk_score` | 0-100 composite score | Main dashboard KPI |
# MAGIC | `risk_category` | CRITICAL/HIGH/MEDIUM/LOW | Color-coding and prioritization |
# MAGIC | `maternal_risk_score` | Domain score for maternal health | Spider chart radar |
# MAGIC | `anemia_risk_score` | Anemia prevalence score | Policy trigger |
# MAGIC | `vaccination_risk_score` | Vaccination coverage gap | Policy trigger |
# MAGIC | `high_anemia_flag` | Boolean trigger | Rules engine input |
# MAGIC | `low_anc_flag` | Boolean trigger | Rules engine input |
# MAGIC | `triggered_risk_count` | Count of active flags | Severity indicator |
# MAGIC
# MAGIC ### Data Lineage:
# MAGIC ```
# MAGIC Bronze (Raw NFHS-5)
# MAGIC   ↓
# MAGIC   └─ 698 districts, 109 indicators
# MAGIC      ↓ clean_percentage UDF
# MAGIC      ↓ null handling
# MAGIC      ↓ type conversion
# MAGIC Silver (Cleaned)
# MAGIC   ↓
# MAGIC   └─ Standardized schema, data quality scores
# MAGIC      ↓ composite calculation
# MAGIC      ↓ risk flag thresholds
# MAGIC      ↓ domain scoring
# MAGIC Gold (ML-Ready)
# MAGIC   ↓
# MAGIC   └─ Features for XGBoost models
# MAGIC   └─ Flags for Rules Engine
# MAGIC   └─ Scores for Genie responses
# MAGIC ```
# MAGIC
# MAGIC ### Next Steps:
# MAGIC - **Phase 2**: Train XGBoost models (Risk Predictor, Policy Ranker, Impact Simulator)
# MAGIC - **Phase 3**: Create Genie Space for natural language policy queries
# MAGIC - **Phase 4**: Build Streamlit UI (Dashboard, Risk Radar, Policy Brief, What-If Simulator)
# MAGIC
# MAGIC ### Git Workflow:
# MAGIC ```bash
# MAGIC # Next: Commit to feature branch
# MAGIC cd /Workspace/Repos/hackathon
# MAGIC git checkout -b feature/healthgpt-phase1
# MAGIC git add .
# MAGIC git commit -m "Phase 1: Bronze->Silver->Gold pipeline for HealthGPT"
# MAGIC git push origin feature/healthgpt-phase1
# MAGIC ```

# COMMAND ----------


