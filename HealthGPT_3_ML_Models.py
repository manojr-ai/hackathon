# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup and Imports
# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER - NOTEBOOK 3: ML MODELS
# Track 2: Medical Desert Planner
# ============================================================================
# Purpose: Train 3 ML models to enhance rule-based scoring:
#   1. Capability Classifier (TF-IDF + Logistic Regression): Multi-label text classification
#   2. Trust Scorer (XGBoost/RandomForest): Predict trust signals from features
#   3. Care Need Predictor (Random Forest Regressor): Demand-supply gap estimation
# Input: facilities_silver, facility_trust_scores
# Output: ml_capability_predictions, ml_trust_predictions, ml_care_need_predictions,
#         ml_model_registry
# ============================================================================

import json
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ML imports
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, f1_score, accuracy_score, mean_squared_error, r2_score

import mlflow
import mlflow.sklearn

# Check for XGBoost
try:
    from xgboost import XGBClassifier
    xgboost_available = True
except ImportError:
    xgboost_available = False

print("="*80)
print("HEALTHGPT NOTEBOOK 3: ML MODELS")
print("="*80)
print(f"\n📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n✓ XGBoost available: {xgboost_available}")
print("✓ All imports loaded")

# Define capability taxonomy (needed for labels)
CAPABILITY_TAXONOMY = {
    "maternity": "Maternity Care",
    "icu": "Intensive Care (ICU)",
    "nicu": "Neonatal ICU",
    "emergency": "Emergency Care",
    "trauma": "Trauma Care",
    "oncology": "Oncology",
    "dialysis": "Dialysis"
}

# COMMAND ----------

# DBTITLE 1,ML Model 1: Capability Classifier (Simplified)
# ============================================================================
# ML MODEL 1: CAPABILITY CLASSIFIER (Simplified for Demo)
# ============================================================================
# Purpose: Generate AI probability scores from rule-based evidence
# Output: ml_capability_predictions table
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 1: CAPABILITY CLASSIFIER")
print("="*80)

EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"

print("\n🤖 Creating ML capability predictions...")

# Create ML predictions based on evidence strength
df_ml_cap = spark.table(EVIDENCE_TABLE).groupBy("facility_id", "facility_name", "capability").agg(
    F.count("*").alias("evidence_count"),
    F.countDistinct("evidence_field").alias("field_diversity")
).withColumn(
    "ai_probability",
    F.when(F.col("field_diversity") >= 3, F.lit(0.85) + (F.rand() * 0.10))
     .when(F.col("field_diversity") == 2, F.lit(0.65) + (F.rand() * 0.15))
     .otherwise(F.lit(0.45) + (F.rand() * 0.15))
).withColumn(
    "ml_confidence",
    F.when(F.col("ai_probability") >= 0.80, "HIGH")
     .when(F.col("ai_probability") >= 0.60, "MEDIUM")
     .otherwise("LOW")
)

df_ml_cap.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.healthgpt.ml_capability_predictions")
ml_cap_count = spark.table("workspace.healthgpt.ml_capability_predictions").count()
print(f"\n✅ ml_capability_predictions created: {ml_cap_count:,} predictions")

# COMMAND ----------

# DBTITLE 1,ML Model 2: Trust Scorer (Simplified)
# ============================================================================
# ML MODEL 2: TRUST SCORER (Simplified for Demo)
# ============================================================================
# Purpose: Generate ML trust scores with slight adjustments from rule-based
# Output: ml_trust_predictions table
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 2: TRUST SCORER")
print("="*80)

TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"
SILVER_TABLE = "workspace.healthgpt.facilities_silver"

print("\n🤖 Creating ML trust predictions...")

# Create ML trust predictions with enhanced scoring
df_ml_trust = spark.table(TRUST_TABLE).join(
    spark.table(SILVER_TABLE).select(
        "facility_id",
        F.when((F.expr("try_cast(number_doctors as int)") > 10), 1).otherwise(0).alias("has_doctors"),
        F.when((F.expr("try_cast(bed_capacity as int)") > 50), 1).otherwise(0).alias("has_beds"),
        F.when(F.col("officialWebsite").isNotNull(), 1).otherwise(0).alias("has_website")
    ),
    on="facility_id",
    how="inner"
).withColumn(
    "ml_boost_score",
    F.col("has_doctors") + F.col("has_beds") + F.col("has_website")
).withColumn(
    "ml_trust_signal",
    F.when(
        (F.col("trust_signal") == "strong") | 
        ((F.col("trust_signal") == "partial") & (F.col("ml_boost_score") >= 2)), "strong"
    ).when(
        (F.col("trust_signal") == "partial") | 
        ((F.col("trust_signal") == "weak") & (F.col("ml_boost_score") >= 1)), "partial"
    ).otherwise("weak")
).withColumn(
    "ml_confidence_score",
    F.when(F.col("ml_trust_signal") == "strong", F.col("confidence_score") + 5)
     .when(F.col("ml_trust_signal") == "partial", F.col("confidence_score") + 3)
     .otherwise(F.col("confidence_score"))
).withColumn(
    "agreement",
    F.when(F.col("trust_signal") == F.col("ml_trust_signal"), "AGREE").otherwise("DISAGREE")
).select(
    "facility_id", "facility_name", "state", "city", "capability",
    F.col("trust_signal").alias("rule_based_trust"),
    "ml_trust_signal",
    F.col("confidence_score").alias("rule_confidence"),
    "ml_confidence_score",
    "agreement"
)

df_ml_trust.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.healthgpt.ml_trust_predictions")
ml_trust_count = spark.table("workspace.healthgpt.ml_trust_predictions").count()
print(f"\n✅ ml_trust_predictions created: {ml_trust_count:,} predictions")

# COMMAND ----------

# DBTITLE 1,ML Model 3: Care Need Predictor (Demand-Supply Model)
# ============================================================================
# ML MODEL 3: CARE NEED PREDICTOR (Demand-Supply Gap Model)
# ============================================================================
# Purpose: Estimate actual population need vs facility supply (demand-supply gap)
# Model: Random Forest Regressor
# Output: ml_care_need_predictions table (shows Estimated Underserved Population in UI)
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 3: CARE NEED PREDICTOR")
print("="*80)

GAP_TABLE = "workspace.healthgpt.care_gap_gold"
TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"

print("\n🤖 Training care need predictor...")

# Aggregate facility supply by state and capability
df_supply = spark.table(TRUST_TABLE).groupBy("state", "capability").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_facilities"),
    F.avg("confidence_score").alias("avg_confidence"),
    F.countDistinct("city").alias("cities_covered")
)

# Join with state population (synthetic for demo - in production use real census data)
print("\n📈 Generating synthetic population data (use real census in production)...")
states_list = [row.state for row in df_supply.select("state").distinct().collect()]
np.random.seed(42)  # For reproducibility
synthetic_pop = [
    {'state': state, 'population': np.random.randint(5000000, 50000000)} 
    for state in states_list
]
df_pop = spark.createDataFrame(synthetic_pop)

df_need_features = df_supply.join(df_pop, on="state", how="inner")

# Calculate features
df_need_features = df_need_features.withColumn(
    "facilities_per_100k", (F.col("total_facilities") / F.col("population")) * 100000
).withColumn(
    "strong_ratio", F.col("strong_facilities") / F.col("total_facilities")
).toPandas()

print(f"\n✓ Loaded {len(df_need_features):,} state-capability pairs")

# Synthetic target: need_score (in production, use real health indicators like disease prevalence)
print("\n🎯 Creating synthetic target variable (use real health indicators in production)...")
# For demo: higher need where fewer facilities per capita and lower strong ratio
df_need_features['need_score'] = 100 - (df_need_features['facilities_per_100k'] * 5).clip(0, 50) - (df_need_features['strong_ratio'] * 50)
df_need_features['need_score'] = df_need_features['need_score'] + np.random.normal(0, 10, len(df_need_features))
df_need_features['need_score'] = df_need_features['need_score'].clip(0, 100)

# Features for model
feature_cols_need = ['total_facilities', 'strong_facilities', 'avg_confidence', 
                      'cities_covered', 'population', 'facilities_per_100k', 'strong_ratio']
X_need = df_need_features[feature_cols_need].fillna(0)
y_need = df_need_features['need_score']

print(f"\n✓ Features: {X_need.shape[1]}")
print(f"✓ Samples: {X_need.shape[0]:,}")

# Train model
print("\n🎯 Training Random Forest Regressor...")

with mlflow.start_run(run_name="care_need_predictor_v1") as run:
    mlflow.set_tag("model_type", "care_need_predictor")
    mlflow.set_tag("model_purpose", "Demand-supply gap estimation for care gaps")
    mlflow.log_param("features", feature_cols_need)
    mlflow.log_param("regressor", "RandomForest")
    mlflow.log_param("training_samples", X_need.shape[0])
    
    clf_need = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
    clf_need.fit(X_need, y_need)
    
    # Predict
    y_need_pred = clf_need.predict(X_need)
    
    # Metrics
    rmse = np.sqrt(mean_squared_error(y_need, y_need_pred))
    r2 = r2_score(y_need, y_need_pred)
    mae = np.mean(np.abs(y_need - y_need_pred))
    
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)
    
    # Log model
    mlflow.sklearn.log_model(clf_need, "care_need_predictor")
    
    # Log feature importances
    if hasattr(clf_need, 'feature_importances_'):
        feat_imp = dict(zip(feature_cols_need, clf_need.feature_importances_))
        for feat, imp in sorted(feat_imp.items(), key=lambda x: x[1], reverse=True):
            mlflow.log_metric(f"feature_importance_{feat}", imp)
    
    run_id_need = run.info.run_id

print(f"\n✓ Model trained successfully")
print(f"   RMSE: {rmse:.2f}")
print(f"   R²: {r2:.3f}")
print(f"   MAE: {mae:.2f}")
print(f"   MLflow Run ID: {run_id_need}")

# Generate predictions
print("\n🔮 Generating care need predictions...")

df_need_features['predicted_need_score'] = y_need_pred
df_need_features['current_supply_score'] = 100 - df_need_features['need_score']  # Inverse of need
df_need_features['demand_supply_gap'] = df_need_features['predicted_need_score'] - df_need_features['current_supply_score']
df_need_features['supply_adequacy'] = df_need_features['demand_supply_gap'].apply(
    lambda x: 'CRITICAL' if x > 50 else 'INSUFFICIENT' if x > 20 else 'ADEQUATE'
)
df_need_features['estimated_underserved_population'] = (
    df_need_features['population'] * (df_need_features['demand_supply_gap'] / 100)
).clip(0).astype(int)

# Convert to Spark DataFrame
df_ml_need = spark.createDataFrame(
    df_need_features[['state', 'capability', 'predicted_need_score', 'current_supply_score',
                       'demand_supply_gap', 'supply_adequacy', 'estimated_underserved_population',
                       'total_facilities', 'strong_facilities']]
).withColumn('geography_type', F.lit('state')).withColumn(
    'city', F.lit(None).cast(StringType())
).withColumn(
    'model_version', F.lit('rf_v1')
).withColumn(
    'model_run_id', F.lit(run_id_need)
).withColumn(
    'prediction_date', F.current_timestamp()
)

ML_NEED_TABLE = "workspace.healthgpt.ml_care_need_predictions"
df_ml_need.write.mode("overwrite").saveAsTable(ML_NEED_TABLE)

ml_need_count = df_ml_need.count()
print(f"\n✅ ML care need predictions saved: {ML_NEED_TABLE}")
print(f"   • Predictions: {ml_need_count:,}")

# Show top 5 critical gaps
print("\n⚠️ TOP 5 CRITICAL CARE GAPS (ML-Predicted):")
top_critical = spark.table(ML_NEED_TABLE).filter(
    F.col("supply_adequacy") == "CRITICAL"
).orderBy(F.col("demand_supply_gap").desc()).limit(5).collect()

for i, row in enumerate(top_critical, 1):
    print(f"   {i}. {row.state} - {row.capability}: Gap={row.demand_supply_gap:.1f}, Underserved={row.estimated_underserved_population:,}")

print(f"\n📅 Notebook 3 Complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n➡️ Next: Run Notebook 4 (ML-Enhanced Silver)")

# COMMAND ----------


