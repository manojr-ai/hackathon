# Databricks notebook source
# DBTITLE 1,Setup and Imports
# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER - COMPLETE PIPELINE WITH ML
# Track 2: Medical Desert Planner
# ============================================================================
# Architecture: Bronze → Silver → Gold + ML Models → ML-Enhanced Silver
# ============================================================================

import json
import re
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("="*80)
print("HEALTHGPT COMPLETE PIPELINE WITH ML INTEGRATION")
print("="*80)
print(f"\n📅 Pipeline Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n🎯 Target Schema: workspace.healthgpt")
print("\n✓ Imports loaded successfully")

# Ensure schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.healthgpt")
print("\n✓ Schema verified")

# COMMAND ----------

# DBTITLE 1,Bronze Layer: Raw Facility Data
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

# DBTITLE 1,Silver Layer: Clean and Standardize
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

# Standardize geography
df_silver = df_silver.withColumn(
    "state", F.upper(F.trim(F.col("address_stateOrRegion")))
).withColumn(
    "city", F.initcap(F.trim(F.col("address_city")))
).withColumn(
    "pin_code", F.trim(F.col("address_zipOrPostcode"))
)

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

# Select relevant columns
df_silver = df_silver.select(
    F.col("unique_id").alias("facility_id"),
    F.col("name").alias("facility_name"),
    "state", "city", "pin_code",
    F.col("address_line1"), F.col("address_line2"),
    F.col("latitude"), F.col("longitude"),
    F.col("description"),
    "specialties_text", "procedures_text", "equipment_text", "capabilities_text",
    F.col("number_doctors"), F.col("bed_capacity"),
    F.col("officialWebsite"), F.col("source_urls"),
    "data_quality",
    F.current_timestamp().alias("processed_date")
)

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

# COMMAND ----------

# DBTITLE 1,Gold Layer: Rule-Based Evidence Extraction & Trust Scoring
# ============================================================================
# GOLD LAYER: Rule-Based Evidence Extraction & Trust Scoring
# ============================================================================
print("\n" + "="*80)
print("GOLD LAYER: RULE-BASED EVIDENCE & TRUST SCORING")
print("="*80)

# Define capability taxonomy
CAPABILITY_TAXONOMY = {
    "maternity": ["maternity", "maternal", "obstetric", "labor", "delivery", "prenatal", 
                  "antenatal", "postnatal", "pregnancy", "childbirth", "gynecology", "obgyn",
                  "neonatal", "midwife", "cesarean", "c-section", "ante natal", "post natal"],
    "icu": ["icu", "intensive care", "critical care", "ventilator", "life support",
            "icu bed", "iccu", "intensive", "criticalcare"],
    "nicu": ["nicu", "neonatal intensive", "premature", "preterm", "newborn icu",
             "neonatal care", "incubator", "neonatal unit"],
    "emergency": ["emergency", "casualty", "24x7", "24/7", "trauma", "accident",
                  "emergency room", "er", "urgent care", "emergency ward", "casualty ward", "24 hours"],
    "trauma": ["trauma", "polytrauma", "trauma center", "trauma unit", "injury care",
               "accident care", "trauma surgery", "emergency surgery", "major injury"],
    "oncology": ["oncology", "cancer", "chemotherapy", "radiation", "radiotherapy",
                 "tumor", "malignancy", "chemo", "oncologist", "cancer treatment"],
    "dialysis": ["dialysis", "hemodialysis", "renal", "kidney", "nephrology",
                 "dialysis center", "kidney failure", "dialysis unit", "hemo dialysis"]
}

print("\n📊 Capability Taxonomy Defined: 7 capabilities")
for cap, keywords in CAPABILITY_TAXONOMY.items():
    print(f"   {cap}: {len(keywords)} keywords")

# Load silver
df_facilities = spark.table(SILVER_TABLE)

# Extract evidence per capability
evidence_records = []

for capability, keywords in CAPABILITY_TAXONOMY.items():
    print(f"\n🔍 Extracting evidence for: {capability}")
    
    # Search in multiple fields
    fields_to_search = [
        ("description", "description"),
        ("specialties_text", "specialties"),
        ("procedures_text", "procedures"),
        ("equipment_text", "equipment"),
        ("capabilities_text", "capabilities")
    ]
    
    for col_name, field_label in fields_to_search:
        # Build regex pattern for keywords
        pattern = "|".join([re.escape(kw) for kw in keywords])
        
        df_matches = df_facilities.filter(
            F.lower(F.col(col_name)).rlike(pattern.lower())
        ).select(
            "facility_id", "facility_name", "state", "city",
            col_name, "officialWebsite", "data_quality"
        ).withColumn(
            "capability", F.lit(capability)
        ).withColumn(
            "evidence_field", F.lit(field_label)
        ).withColumn(
            "evidence_text", F.col(col_name)
        )
        
        evidence_records.append(df_matches)

# Union all evidence
df_evidence = evidence_records[0]
for df in evidence_records[1:]:
    df_evidence = df_evidence.union(df)

evidence_count = df_evidence.count()
print(f"\n✓ Extracted {evidence_count:,} evidence records")

# Save evidence to table
EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"
df_evidence.write.mode("overwrite").saveAsTable(EVIDENCE_TABLE)
print(f"\n✅ Evidence table created: {EVIDENCE_TABLE}")

# ============================================================================
# TRUST SCORING: Rule-Based Logic
# ============================================================================
print("\n" + "="*80)
print("TRUST SCORING: RULE-BASED CLASSIFICATION")
print("="*80)

# Count evidence per facility-capability
df_evidence_agg = df_evidence.groupBy("facility_id", "capability").agg(
    F.count("*").alias("evidence_count"),
    F.countDistinct("evidence_field").alias("field_diversity"),
    F.first("facility_name").alias("facility_name"),
    F.first("state").alias("state"),
    F.first("city").alias("city"),
    F.first("data_quality").alias("data_quality")
)

# Apply trust scoring rules
df_trust = df_evidence_agg.withColumn(
    "trust_signal",
    F.when(
        (F.col("field_diversity") >= 3) & (F.col("evidence_count") >= 5), "strong"
    ).when(
        (F.col("field_diversity") >= 2) | ((F.col("field_diversity") == 1) & (F.col("evidence_count") >= 3)), "partial"
    ).otherwise("weak")
).withColumn(
    "confidence_score",
    F.when(F.col("trust_signal") == "strong", 90)
     .when(F.col("trust_signal") == "partial", 65)
     .otherwise(35)
)

trust_count = df_trust.count()
print(f"\n✓ Scored {trust_count:,} facility-capability pairs")

# Trust distribution
trust_dist = df_trust.groupBy("trust_signal").count().orderBy("trust_signal").collect()
print("\n📊 Trust Signal Distribution:")
for row in trust_dist:
    print(f"   {row.trust_signal}: {row['count']:,}")

# Save trust scores
TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"
df_trust.write.mode("overwrite").saveAsTable(TRUST_TABLE)
print(f"\n✅ Trust scores table created: {TRUST_TABLE}")

# ============================================================================
# GEOGRAPHIC AGGREGATION: Care Gap Scores
# ============================================================================
print("\n" + "="*80)
print("GEOGRAPHIC AGGREGATION: CALCULATING CARE GAP SCORES")
print("="*80)

# Calculate gap scores by state x capability
df_gaps = df_trust.groupBy("state", "capability").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_count"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_count"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_count"),
    F.avg("confidence_score").alias("avg_confidence")
).withColumn(
    "strong_pct", F.col("strong_count") / F.col("total_facilities") * 100
).withColumn(
    "partial_pct", F.col("partial_count") / F.col("total_facilities") * 100
).withColumn(
    "weak_pct", F.col("weak_count") / F.col("total_facilities") * 100
).withColumn(
    "coverage_score", 
    (F.col("strong_pct") * 1.0) + (F.col("partial_pct") * 0.5) + (F.col("weak_pct") * 0.2)
).withColumn(
    "gap_score", 100 - F.col("coverage_score")
).withColumn(
    "confidence_level",
    F.when(F.col("avg_confidence") >= 80, "HIGH")
     .when(F.col("avg_confidence") >= 65, "MEDIUM-HIGH")
     .when(F.col("avg_confidence") >= 50, "MEDIUM")
     .otherwise("LOW")
).withColumn(
    "geography_type", F.lit("state")
).withColumn(
    "city", F.lit(None).cast(StringType())
)

gap_count = df_gaps.count()
print(f"\n✓ Calculated {gap_count} state-level care gaps")

# Save gap scores
GAP_TABLE = "workspace.healthgpt.care_gap_gold"
df_gaps.write.mode("overwrite").saveAsTable(GAP_TABLE)
print(f"\n✅ Care gap table created: {GAP_TABLE}")

# Show top 5 maternity gaps
print("\n👶 TOP 5 MATERNITY CARE GAPS:")
top_maternity = spark.table(GAP_TABLE).filter(
    F.col("capability") == "maternity"
).orderBy(F.col("gap_score").desc()).limit(5).collect()

for i, row in enumerate(top_maternity, 1):
    print(f"   {i}. {row.state}: Gap={row.gap_score:.1f}/100, Confidence={row.confidence_level}")

# COMMAND ----------

# DBTITLE 1,ML Model 1: Capability Classifier (Text Classification)
# ============================================================================
# ML MODEL 1: CAPABILITY CLASSIFIER (Multi-Label Text Classification)
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 1: CAPABILITY CLASSIFIER")
print("="*80)

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, f1_score
import mlflow
import mlflow.sklearn

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
        print(f"   {cap}: {count:,} positive samples")

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
    mlflow.log_param("vectorizer", "TfidfVectorizer")
    mlflow.log_param("max_features", 5000)
    mlflow.log_param("classifier", "LogisticRegression")
    mlflow.log_param("capabilities", len(capability_cols))
    
    clf = OneVsRestClassifier(LogisticRegression(max_iter=1000, random_state=42))
    clf.fit(X, y)
    
    # Predict on training set (for evaluation)
    y_pred = clf.predict(X)
    y_proba = clf.predict_proba(X)
    
    # Calculate metrics
    f1_micro = f1_score(y, y_pred, average='micro')
    f1_macro = f1_score(y, y_pred, average='macro')
    
    mlflow.log_metric("f1_micro", f1_micro)
    mlflow.log_metric("f1_macro", f1_macro)
    
    # Log model
    mlflow.sklearn.log_model(clf, "capability_classifier")
    mlflow.sklearn.log_model(vectorizer, "vectorizer")
    
    run_id = run.info.run_id
    
print(f"\n✓ Model trained successfully")
print(f"   F1 Micro: {f1_micro:.3f}")
print(f"   F1 Macro: {f1_macro:.3f}")
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

# COMMAND ----------

# DBTITLE 1,ML Model 2: Trust Scorer (Gradient Boosting)
# ============================================================================
# ML MODEL 2: TRUST SCORER (XGBoost Classifier)
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 2: TRUST SCORER")
print("="*80)

try:
    from xgboost import XGBClassifier
    xgboost_available = True
except ImportError:
    print("⚠️ XGBoost not available, using RandomForest instead")
    from sklearn.ensemble import RandomForestClassifier
    xgboost_available = False

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, classification_report

print("\n🤖 Training trust scorer...")

# Load evidence and trust scores
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
print("\n🎯 Training classifier...")

with mlflow.start_run(run_name="trust_scorer_v1") as run:
    mlflow.set_tag("model_type", "trust_scorer")
    mlflow.log_param("features", feature_cols)
    
    if xgboost_available:
        mlflow.log_param("classifier", "XGBoost")
        clf_trust = XGBClassifier(n_estimators=100, max_depth=5, random_state=42)
    else:
        mlflow.log_param("classifier", "RandomForest")
        clf_trust = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    
    clf_trust.fit(X_trust, y_trust_encoded)
    
    # Predict
    y_trust_pred = clf_trust.predict(X_trust)
    y_trust_proba = clf_trust.predict_proba(X_trust)
    
    # Metrics
    acc = accuracy_score(y_trust_encoded, y_trust_pred)
    mlflow.log_metric("accuracy", acc)
    
    # Log model
    mlflow.sklearn.log_model(clf_trust, "trust_scorer")
    
    run_id_trust = run.info.run_id

print(f"\n✓ Model trained successfully")
print(f"   Accuracy: {acc:.3f}")
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

# Calculate delta
df_trust_features['trust_delta'] = df_trust_features['ml_trust_score'] - df_trust_features['confidence_score']

# Convert to Spark DataFrame
df_ml_trust = spark.createDataFrame(
    df_trust_features[['facility_id', 'capability', 'ml_trust_signal', 'ml_trust_score', 
                        'trust_signal', 'confidence_score', 'trust_delta']]
).withColumn('model_version', F.lit('xgboost_v1' if xgboost_available else 'rf_v1')).withColumn(
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

# COMMAND ----------

# DBTITLE 1,ML Model 3: Care Need Predictor (Demand-Supply Model)
# ============================================================================
# ML MODEL 3: CARE NEED PREDICTOR (Demand-Supply Gap Model)
# ============================================================================
print("\n" + "="*80)
print("ML MODEL 3: CARE NEED PREDICTOR")
print("="*80)

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

print("\n🤖 Training care need predictor...")

# Aggregate facility supply by state and capability
df_supply = spark.table(TRUST_TABLE).groupBy("state", "capability").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_facilities"),
    F.avg("confidence_score").alias("avg_confidence"),
    F.countDistinct("city").alias("cities_covered")
)

# Join with state population (synthetic for demo - in production use real data)
# Creating synthetic population data
states_list = [row.state for row in df_supply.select("state").distinct().collect()]
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

# Synthetic target: need_score (in production, use real health indicators)
# For demo: higher need where fewer facilities per capita
df_need_features['need_score'] = 100 - (df_need_features['facilities_per_100k'] * 10).clip(0, 100)
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
    mlflow.log_param("features", feature_cols_need)
    mlflow.log_param("regressor", "RandomForest")
    
    clf_need = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
    clf_need.fit(X_need, y_need)
    
    # Predict
    y_need_pred = clf_need.predict(X_need)
    
    # Metrics
    rmse = np.sqrt(mean_squared_error(y_need, y_need_pred))
    r2 = r2_score(y_need, y_need_pred)
    
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    
    # Log model
    mlflow.sklearn.log_model(clf_need, "care_need_predictor")
    
    run_id_need = run.info.run_id

print(f"\n✓ Model trained successfully")
print(f"   RMSE: {rmse:.2f}")
print(f"   R²: {r2:.3f}")
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
                       'demand_supply_gap', 'supply_adequacy', 'estimated_underserved_population']]
).withColumn('geography_type', F.lit('state')).withColumn(
    'city', F.lit(None).cast(StringType())
).withColumn(
    'model_version', F.lit('rf_v1')
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

# COMMAND ----------


