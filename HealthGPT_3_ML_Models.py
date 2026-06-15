# Databricks notebook source
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

# DBTITLE 1,ML Model 1: Capability Classifier (Text Classification)
# ============================================================================
# ML MODEL 1: CAPABILITY CLASSIFIER (Multi-Label Text Classification)
# ============================================================================
# Purpose: Predict capability probabilities from facility text (UI shows "AI Probability Score: 85%")
# Model: TF-IDF + OneVsRest Logistic Regression
# Output: ml_capability_predictions table
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 1: CAPABILITY CLASSIFIER")
print("="*80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"
EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"

print("\n🤖 Training multi-label capability classifier...")

# Prepare training data from Silver
df_train = spark.table(SILVER_TABLE).select(
    "facility_id",
    "facility_name",
    F.concat_ws(" ", 
        F.coalesce(F.col("description"), F.lit("")),
        F.coalesce(F.col("specialties_text"), F.lit("")),
        F.coalesce(F.col("procedures_text"), F.lit("")),
        F.coalesce(F.col("equipment_text"), F.lit("")),
        F.coalesce(F.col("capabilities_text"), F.lit(""))
    ).alias("combined_text")
).filter(F.length(F.col("combined_text")) > 10).toPandas()

print(f"\n✓ Loaded {len(df_train):,} facilities for training")

# Create weak labels from rule-based evidence
print("\n🏷️ Creating weak labels from rule-based evidence...")
df_labels = spark.table(EVIDENCE_TABLE).groupBy("facility_id", "capability").agg(
    F.count("*").alias("evidence_count")
).toPandas()

# Pivot to multi-label format
label_matrix = df_labels.pivot(index='facility_id', columns='capability', values='evidence_count').fillna(0)
label_matrix = (label_matrix > 0).astype(int)  # Binary labels

# Merge with text
df_train = df_train.merge(label_matrix, left_on='facility_id', right_index=True, how='inner')

print(f"\n✓ Training set: {len(df_train):,} facilities with labels")
print(f"\n📊 Label distribution:")
for cap in CAPABILITY_TAXONOMY.keys():
    if cap in df_train.columns:
        count = df_train[cap].sum()
        pct = (count / len(df_train)) * 100
        print(f"   {cap}: {count:,} positive ({pct:.1f}%)")

# TF-IDF vectorization
print("\n🔢 Vectorizing text with TF-IDF...")
vectorizer = TfidfVectorizer(max_features=5000, ngram_range=(1, 2), min_df=2)
X = vectorizer.fit_transform(df_train['combined_text'])

# Prepare labels
capability_cols = [c for c in CAPABILITY_TAXONOMY.keys() if c in df_train.columns]
y = df_train[capability_cols].values

print(f"✓ Features: {X.shape[1]:,}")
print(f"✓ Samples: {X.shape[0]:,}")
print(f"✓ Labels: {len(capability_cols)} capabilities")

# Train multi-label classifier
print("\n🎯 Training OneVsRest Logistic Regression...")

with mlflow.start_run(run_name="capability_classifier_v1") as run:
    mlflow.set_tag("model_type", "capability_classifier")
    mlflow.set_tag("model_purpose", "Multi-label text classification for care capabilities")
    mlflow.log_param("vectorizer", "TfidfVectorizer")
    mlflow.log_param("max_features", 5000)
    mlflow.log_param("ngram_range", "(1,2)")
    mlflow.log_param("classifier", "LogisticRegression")
    mlflow.log_param("capabilities", len(capability_cols))
    mlflow.log_param("training_samples", X.shape[0])
    
    clf = OneVsRestClassifier(LogisticRegression(max_iter=1000, random_state=42))
    clf.fit(X, y)
    
    # Predict on training set (for evaluation)
    y_pred = clf.predict(X)
    y_proba = clf.predict_proba(X)
    
    # Calculate metrics
    f1_micro = f1_score(y, y_pred, average='micro')
    f1_macro = f1_score(y, y_pred, average='macro')
    f1_weighted = f1_score(y, y_pred, average='weighted')
    
    mlflow.log_metric("f1_micro", f1_micro)
    mlflow.log_metric("f1_macro", f1_macro)
    mlflow.log_metric("f1_weighted", f1_weighted)
    
    # Log model
    mlflow.sklearn.log_model(clf, "capability_classifier")
    mlflow.sklearn.log_model(vectorizer, "vectorizer")
    
    run_id = run.info.run_id
    
print(f"\n✓ Model trained successfully")
print(f"   F1 Micro: {f1_micro:.3f}")
print(f"   F1 Macro: {f1_macro:.3f}")
print(f"   F1 Weighted: {f1_weighted:.3f}")
print(f"   MLflow Run ID: {run_id}")

# Generate predictions for all facilities
print("\n🔮 Generating predictions for all facilities...")

df_predict = spark.table(SILVER_TABLE).select(
    "facility_id",
    F.concat_ws(" ",
        F.coalesce(F.col("description"), F.lit("")),
        F.coalesce(F.col("specialties_text"), F.lit("")),
        F.coalesce(F.col("procedures_text"), F.lit("")),
        F.coalesce(F.col("equipment_text"), F.lit("")),
        F.coalesce(F.col("capabilities_text"), F.lit(""))
    ).alias("combined_text")
).filter(F.length(F.col("combined_text")) > 10).toPandas()

X_pred = vectorizer.transform(df_predict['combined_text'])
y_proba_pred = clf.predict_proba(X_pred)

# Convert to DataFrame
ml_predictions = []
for idx, facility_id in enumerate(df_predict['facility_id']):
    for cap_idx, capability in enumerate(capability_cols):
        probability = y_proba_pred[idx][cap_idx]
        ml_predictions.append({
            'facility_id': facility_id,
            'capability': capability,
            'ml_probability': float(probability),
            'ml_confidence': 'HIGH' if probability > 0.7 else 'MEDIUM' if probability > 0.4 else 'LOW',
            'model_version': 'tfidf_logistic_v1',
            'model_run_id': run_id,
            'prediction_date': datetime.now()
        })

df_ml_predictions = spark.createDataFrame(ml_predictions)

ML_CAPABILITY_TABLE = "workspace.healthgpt.ml_capability_predictions"
df_ml_predictions.write.mode("overwrite").saveAsTable(ML_CAPABILITY_TABLE)

pred_count = df_ml_predictions.count()
print(f"\n✅ ML predictions saved: {ML_CAPABILITY_TABLE}")
print(f"   • Predictions: {pred_count:,}")
print(f"   • High confidence: {df_ml_predictions.filter(F.col('ml_confidence') == 'HIGH').count():,}")
print(f"   • Medium confidence: {df_ml_predictions.filter(F.col('ml_confidence') == 'MEDIUM').count():,}")
print(f"   • Low confidence: {df_ml_predictions.filter(F.col('ml_confidence') == 'LOW').count():,}")

# COMMAND ----------

# DBTITLE 1,ML Model 2: Trust Scorer (Gradient Boosting)
# ============================================================================
# ML MODEL 2: TRUST SCORER (XGBoost/RandomForest Classifier)
# ============================================================================
# Purpose: Predict trust signals (strong/partial/weak) from multi-signal features
# Model: XGBoost (if available) or RandomForest
# Output: ml_trust_predictions table (shows ML-enhanced Trust vs Rule-based Trust in UI)
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 2: TRUST SCORER")
print("="*80)

TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"
EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"
SILVER_TABLE = "workspace.healthgpt.facilities_silver"

print("\n🤖 Training trust scorer...")

# Load evidence and trust scores with features
print("\n🔧 Building feature set...")

df_trust_features = spark.table(TRUST_TABLE).join(
    spark.table(EVIDENCE_TABLE).groupBy("facility_id", "capability").agg(
        F.count("*").alias("evidence_count"),
        F.countDistinct("evidence_field").alias("field_diversity"),
        F.avg(F.length("evidence_text")).alias("avg_text_length")
    ),
    on=["facility_id", "capability"],
    how="inner"
).join(
    spark.table(SILVER_TABLE).select(
        "facility_id",
        F.when(F.col("number_doctors").isNotNull(), F.col("number_doctors")).otherwise(0).alias("num_doctors"),
        F.when(F.col("bed_capacity").isNotNull(), F.col("bed_capacity")).otherwise(0).alias("beds"),
        F.when(F.col("officialWebsite").isNotNull(), 1).otherwise(0).alias("has_website"),
        F.when(F.col("data_quality") == "COMPLETE", 2).when(F.col("data_quality") == "PARTIAL", 1).otherwise(0).alias("quality_score")
    ),
    on="facility_id",
    how="inner"
).toPandas()

print(f"\n✓ Loaded {len(df_trust_features):,} facility-capability pairs for training")

# Prepare features
feature_cols = ['evidence_count', 'field_diversity', 'avg_text_length', 'num_doctors', 'beds', 'has_website', 'quality_score']
X_trust = df_trust_features[feature_cols].fillna(0)
y_trust = df_trust_features['trust_signal']

# Encode labels
le = LabelEncoder()
y_trust_encoded = le.fit_transform(y_trust)

print(f"\n✓ Features: {X_trust.shape[1]}")
print(f"✓ Samples: {X_trust.shape[0]:,}")
print(f"✓ Classes: {list(le.classes_)}")

# Train model
print(f"\n🎯 Training classifier ({('XGBoost' if xgboost_available else 'RandomForest')})...")

with mlflow.start_run(run_name="trust_scorer_v1") as run:
    mlflow.set_tag("model_type", "trust_scorer")
    mlflow.set_tag("model_purpose", "Multi-class classification for evidence trust signals")
    mlflow.log_param("features", feature_cols)
    mlflow.log_param("training_samples", X_trust.shape[0])
    
    if xgboost_available:
        mlflow.log_param("classifier", "XGBoost")
        clf_trust = XGBClassifier(n_estimators=100, max_depth=5, random_state=42, eval_metric='logloss')
    else:
        mlflow.log_param("classifier", "RandomForest")
        clf_trust = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    
    clf_trust.fit(X_trust, y_trust_encoded)
    
    # Predict
    y_trust_pred = clf_trust.predict(X_trust)
    y_trust_proba = clf_trust.predict_proba(X_trust)
    
    # Metrics
    acc = accuracy_score(y_trust_encoded, y_trust_pred)
    f1 = f1_score(y_trust_encoded, y_trust_pred, average='weighted')
    
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("f1_weighted", f1)
    
    # Log model
    mlflow.sklearn.log_model(clf_trust, "trust_scorer")
    
    # Log feature importances
    if hasattr(clf_trust, 'feature_importances_'):
        feat_imp = dict(zip(feature_cols, clf_trust.feature_importances_))
        for feat, imp in sorted(feat_imp.items(), key=lambda x: x[1], reverse=True):
            mlflow.log_metric(f"feature_importance_{feat}", imp)
    
    run_id_trust = run.info.run_id

print(f"\n✓ Model trained successfully")
print(f"   Accuracy: {acc:.3f}")
print(f"   F1 Weighted: {f1:.3f}")
print(f"   MLflow Run ID: {run_id_trust}")

# Generate ML trust predictions
print("\n🔮 Generating ML trust predictions...")

# Predict on full dataset
X_trust_pred = df_trust_features[feature_cols].fillna(0)
y_trust_ml = clf_trust.predict(X_trust_pred)
y_trust_ml_proba = clf_trust.predict_proba(X_trust_pred)

# Decode predictions
df_trust_features['ml_trust_signal'] = le.inverse_transform(y_trust_ml)
df_trust_features['ml_trust_score'] = [proba.max() * 100 for proba in y_trust_ml_proba]

# Calculate delta (ML - Rule)
df_trust_features['trust_delta'] = df_trust_features['ml_trust_score'] - df_trust_features['confidence_score']

# Convert to Spark DataFrame
df_ml_trust = spark.createDataFrame(
    df_trust_features[['facility_id', 'capability', 'ml_trust_signal', 'ml_trust_score', 
                        'trust_signal', 'confidence_score', 'trust_delta', 'facility_name',
                        'state', 'city']]
).withColumn('model_version', F.lit('xgboost_v1' if xgboost_available else 'rf_v1')).withColumn(
    'model_run_id', F.lit(run_id_trust)
).withColumn(
    'prediction_date', F.current_timestamp()
)

ML_TRUST_TABLE = "workspace.healthgpt.ml_trust_predictions"
df_ml_trust.write.mode("overwrite").saveAsTable(ML_TRUST_TABLE)

ml_trust_count = df_ml_trust.count()
print(f"\n✅ ML trust predictions saved: {ML_TRUST_TABLE}")
print(f"   • Predictions: {ml_trust_count:,}")

# Show agreement vs disagreement
agreement = df_ml_trust.filter(
    F.col("ml_trust_signal") == F.col("trust_signal")
).count()
agreement_pct = (agreement / ml_trust_count) * 100
print(f"\n🔄 Rule vs ML Agreement: {agreement_pct:.1f}%")

# Show cases where ML is more confident
ml_higher = df_ml_trust.filter(F.col("trust_delta") > 10).count()
print(f"   • ML more confident: {ml_higher:,} cases")

ml_lower = df_ml_trust.filter(F.col("trust_delta") < -10).count()
print(f"   • Rule more confident: {ml_lower:,} cases")

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


