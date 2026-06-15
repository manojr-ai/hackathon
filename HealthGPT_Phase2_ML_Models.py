# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Phase 2 Overview
# MAGIC %md
# MAGIC # HealthGPT Policy Command Center - Phase 2
# MAGIC ## XGBoost ML Models for Risk Prediction
# MAGIC
# MAGIC This notebook trains machine learning models on the Gold table to power HealthGPT's predictive capabilities.
# MAGIC
# MAGIC ### Models:
# MAGIC 1. **Risk Score Predictor**: Predicts overall district risk score (regression)
# MAGIC 2. **Risk Category Classifier**: Classifies districts into risk categories (classification)
# MAGIC 3. **Feature Importance**: Identifies key health indicators driving risk
# MAGIC
# MAGIC ### Architecture:
# MAGIC ```
# MAGIC Gold Table (706 districts)
# MAGIC   ↓
# MAGIC   ├─ Feature Selection (health indicators)
# MAGIC   ├─ Train/Test Split (80/20)
# MAGIC   ↓
# MAGIC   ├─ XGBoost Regressor → Risk Score Prediction
# MAGIC   ├─ XGBoost Classifier → Risk Category
# MAGIC   ↓
# MAGIC   ├─ Model Evaluation (RMSE, MAE, R², Accuracy)
# MAGIC   ├─ Feature Importance Analysis
# MAGIC   └─ MLflow Model Registry
# MAGIC ```
# MAGIC
# MAGIC ### Use Cases:
# MAGIC - **Dashboard**: Display predicted risk for new districts
# MAGIC - **Policy Engine**: Recommend interventions based on feature importance
# MAGIC - **What-If Simulator**: Predict impact of policy changes

# COMMAND ----------

# DBTITLE 1,Install Required Packages
# MAGIC %pip install xgboost --quiet

# COMMAND ----------

# DBTITLE 1,Configuration and Imports
# ML and Data Processing
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

# XGBoost
import xgboost as xgb
from xgboost import XGBRegressor, XGBClassifier

# Scikit-learn
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler

# MLflow
import mlflow
import mlflow.xgboost
import mlflow.sklearn

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

# Configuration
GOLD_TABLE = "workspace.healthgpt.health_indicators_gold"
MLFLOW_EXPERIMENT = "/Users/manoj.rayalla@acuitybrands.com/healthgpt_models"

print("✓ Libraries imported successfully")
print(f"  XGBoost version: {xgb.__version__}")
print(f"  Gold Table: {GOLD_TABLE}")

# COMMAND ----------

# DBTITLE 1,Load and Prepare Data
# Load Gold table
gold_df = spark.table(GOLD_TABLE)

print(f"✓ Loaded {gold_df.count()} districts from Gold table")
print(f"  Total columns: {len(gold_df.columns)}")

# Convert to Pandas for ML (small dataset, fits in memory)
df = gold_df.toPandas()

print(f"\n✓ Converted to Pandas DataFrame")
print(f"  Shape: {df.shape}")
print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Display basic statistics
print(f"\n📊 Risk Score Distribution:")
print(df['overall_risk_score'].describe())

print(f"\n📊 Risk Category Distribution:")
print(df['risk_category'].value_counts())

# COMMAND ----------

# DBTITLE 1,Feature Engineering for ML
# Select features for ML models
# Target: overall_risk_score (regression) and risk_category (classification)

# Feature categories:
# 1. Health Indicators (percentages)
health_features = [
    'anc_4_visits_pct', 'women_anemia_pct', 'child_stunting_pct',
    'vaccination_full_pct', 'sanitation_improved_pct', 'institutional_birth_pct',
    'ifa_100_days_pct', 'child_anemia_pct', 'water_improved_pct',
    'clean_fuel_pct', 'women_literacy_pct', 'child_marriage_pct'
]

# 2. Composite Scores
composite_features = [
    'maternal_care_composite', 'child_health_composite', 'sanitation_composite'
]

# 3. Domain Risk Scores
domain_features = [
    'maternal_risk_score', 'anemia_risk_score', 'vaccination_risk_score',
    'sanitation_risk_score', 'child_nutrition_risk_score'
]

# 4. Binary Flags
flag_features = [
    'high_anemia_flag', 'low_anc_flag', 'low_vaccination_flag',
    'poor_sanitation_flag', 'high_child_stunting_flag'
]

# Combine all features
all_features = health_features + composite_features + domain_features + flag_features

print(f"✓ Selected {len(all_features)} features for ML models:")
print(f"  - Health Indicators: {len(health_features)}")
print(f"  - Composite Scores: {len(composite_features)}")
print(f"  - Domain Risk Scores: {len(domain_features)}")
print(f"  - Binary Flags: {len(flag_features)}")

# Convert boolean flags to int
for flag in flag_features:
    df[flag] = df[flag].astype(int)

# Check for missing values
missing = df[all_features].isnull().sum()
if missing.sum() > 0:
    print(f"\n⚠️  Missing values detected:")
    print(missing[missing > 0])
else:
    print(f"\n✓ No missing values in feature set")

# COMMAND ----------

# DBTITLE 1,Train/Test Split
# Prepare feature matrix (X) and target variables (y)
X = df[all_features].copy()
y_regression = df['overall_risk_score'].copy()
y_classification = df['risk_category'].copy()

print(f"Feature Matrix (X): {X.shape}")
print(f"Target (regression): {y_regression.shape}")
print(f"Target (classification): {y_classification.shape}")

# Train/Test split (80/20)
X_train, X_test, y_train_reg, y_test_reg = train_test_split(
    X, y_regression, test_size=0.2, random_state=42, stratify=pd.cut(y_regression, bins=3)
)

X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(
    X, y_classification, test_size=0.2, random_state=42, stratify=y_classification
)

print(f"\n✓ Train/Test Split Complete:")
print(f"  Training set: {X_train.shape[0]} samples ({X_train.shape[0]/len(X)*100:.1f}%)")
print(f"  Test set: {X_test.shape[0]} samples ({X_test.shape[0]/len(X)*100:.1f}%)")

print(f"\n📊 Training Set Risk Distribution:")
print(pd.cut(y_train_reg, bins=3).value_counts().sort_index())

# COMMAND ----------

# DBTITLE 1,Model 1: Risk Score Predictor (Regression)
# Train XGBoost Regressor for Risk Score Prediction
print("=" * 70)
print("MODEL 1: RISK SCORE PREDICTOR (XGBoost Regressor)")
print("=" * 70)

# Configure XGBoost parameters
xgb_reg_params = {
    'n_estimators': 100,
    'max_depth': 6,
    'learning_rate': 0.1,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'random_state': 42,
    'objective': 'reg:squarederror',
    'eval_metric': 'rmse'
}

# Train model
xgb_reg = XGBRegressor(**xgb_reg_params)
xgb_reg.fit(X_train, y_train_reg, verbose=False)

print(f"\n✓ Model trained successfully")
print(f"  Estimators: {xgb_reg.n_estimators}")
print(f"  Max depth: {xgb_reg.max_depth}")
print(f"  Learning rate: {xgb_reg.learning_rate}")

# Predictions
y_pred_train = xgb_reg.predict(X_train)
y_pred_test = xgb_reg.predict(X_test)

# Evaluation metrics
train_rmse = np.sqrt(mean_squared_error(y_train_reg, y_pred_train))
train_mae = mean_absolute_error(y_train_reg, y_pred_train)
train_r2 = r2_score(y_train_reg, y_pred_train)

test_rmse = np.sqrt(mean_squared_error(y_test_reg, y_pred_test))
test_mae = mean_absolute_error(y_test_reg, y_pred_test)
test_r2 = r2_score(y_test_reg, y_pred_test)

print(f"\n📊 Model Performance:")
print(f"\n  Training Set:")
print(f"    RMSE: {train_rmse:.2f}")
print(f"    MAE:  {train_mae:.2f}")
print(f"    R²:   {train_r2:.4f}")

print(f"\n  Test Set:")
print(f"    RMSE: {test_rmse:.2f}")
print(f"    MAE:  {test_mae:.2f}")
print(f"    R²:   {test_r2:.4f}")

if test_r2 >= 0.80:
    print(f"\n  ✅ EXCELLENT model performance (R² ≥ 0.80)")
elif test_r2 >= 0.60:
    print(f"\n  ✅ GOOD model performance (R² ≥ 0.60)")
else:
    print(f"\n  ⚠️  Model may need tuning (R² < 0.60)")

# COMMAND ----------

# DBTITLE 1,Model 2: Risk Category Classifier
# Train XGBoost Classifier for Risk Category
print("\n" + "=" * 70)
print("MODEL 2: RISK CATEGORY CLASSIFIER (XGBoost Classifier)")
print("=" * 70)

# Encode labels: map risk categories to integers
from sklearn.preprocessing import LabelEncoder
label_encoder = LabelEncoder()
y_train_cls_encoded = label_encoder.fit_transform(y_train_cls)
y_test_cls_encoded = label_encoder.transform(y_test_cls)

print(f"\n✓ Label encoding:")
for i, label in enumerate(label_encoder.classes_):
    print(f"  {label} → {i}")

# Configure XGBoost parameters
xgb_cls_params = {
    'n_estimators': 100,
    'max_depth': 4,
    'learning_rate': 0.1,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'random_state': 42,
    'objective': 'multi:softmax',
    'num_class': len(label_encoder.classes_),
    'eval_metric': 'mlogloss'
}

# Train model
xgb_cls = XGBClassifier(**xgb_cls_params)
xgb_cls.fit(X_train_cls, y_train_cls_encoded, verbose=False)

print(f"\n✓ Model trained successfully")
print(f"  Classes: {label_encoder.classes_}")

# Predictions
y_pred_train_cls_encoded = xgb_cls.predict(X_train_cls)
y_pred_test_cls_encoded = xgb_cls.predict(X_test_cls)

# Decode predictions back to original labels
y_pred_train_cls = label_encoder.inverse_transform(y_pred_train_cls_encoded)
y_pred_test_cls = label_encoder.inverse_transform(y_pred_test_cls_encoded)

# Evaluation metrics
train_accuracy = accuracy_score(y_train_cls, y_pred_train_cls)
test_accuracy = accuracy_score(y_test_cls, y_pred_test_cls)

print(f"\n📊 Model Performance:")
print(f"  Training Accuracy: {train_accuracy:.4f} ({train_accuracy*100:.2f}%)")
print(f"  Test Accuracy:     {test_accuracy:.4f} ({test_accuracy*100:.2f}%)")

print(f"\n📊 Classification Report (Test Set):")
print(classification_report(y_test_cls, y_pred_test_cls))

if test_accuracy >= 0.90:
    print(f"\n  ✅ EXCELLENT classification performance (Accuracy ≥ 90%)")
elif test_accuracy >= 0.75:
    print(f"\n  ✅ GOOD classification performance (Accuracy ≥ 75%)")
else:
    print(f"\n  ⚠️  Model may need tuning (Accuracy < 75%)")

# COMMAND ----------

# DBTITLE 1,Feature Importance Analysis
# Feature Importance from Regression Model
print("=" * 70)
print("FEATURE IMPORTANCE ANALYSIS")
print("=" * 70)

# Get feature importance
feature_importance = pd.DataFrame({
    'feature': all_features,
    'importance': xgb_reg.feature_importances_
}).sort_values('importance', ascending=False)

print(f"\n📊 Top 10 Most Important Features:")
for idx, row in feature_importance.head(10).iterrows():
    print(f"  {row['feature']:30s}: {row['importance']:.4f}")

# Create visualization
fig, ax = plt.subplots(figsize=(10, 8))
feature_importance.head(15).plot(x='feature', y='importance', kind='barh', ax=ax, color='steelblue')
ax.set_xlabel('Importance Score', fontsize=12)
ax.set_ylabel('Feature', fontsize=12)
ax.set_title('Top 15 Features for Risk Prediction', fontsize=14, fontweight='bold')
ax.invert_yaxis()
plt.tight_layout()
plt.show()

print(f"\n✓ Feature importance analysis complete")

# COMMAND ----------

# DBTITLE 1,Prediction Visualization
# Visualize predictions vs actual values
print("=" * 70)
print("PREDICTION VISUALIZATION")
print("=" * 70)

# Create scatter plot of predictions vs actual
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Training set
ax1.scatter(y_train_reg, y_pred_train, alpha=0.5, s=30, color='steelblue')
ax1.plot([y_train_reg.min(), y_train_reg.max()], 
         [y_train_reg.min(), y_train_reg.max()], 
         'r--', lw=2, label='Perfect Prediction')
ax1.set_xlabel('Actual Risk Score', fontsize=11)
ax1.set_ylabel('Predicted Risk Score', fontsize=11)
ax1.set_title(f'Training Set (R² = {train_r2:.4f})', fontsize=12, fontweight='bold')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Test set
ax2.scatter(y_test_reg, y_pred_test, alpha=0.5, s=30, color='coral')
ax2.plot([y_test_reg.min(), y_test_reg.max()], 
         [y_test_reg.min(), y_test_reg.max()], 
         'r--', lw=2, label='Perfect Prediction')
ax2.set_xlabel('Actual Risk Score', fontsize=11)
ax2.set_ylabel('Predicted Risk Score', fontsize=11)
ax2.set_title(f'Test Set (R² = {test_r2:.4f})', fontsize=12, fontweight='bold')
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# Residual analysis
residuals_test = y_test_reg - y_pred_test

fig, ax = plt.subplots(figsize=(10, 5))
ax.scatter(y_pred_test, residuals_test, alpha=0.5, s=30, color='purple')
ax.axhline(y=0, color='r', linestyle='--', lw=2)
ax.set_xlabel('Predicted Risk Score', fontsize=11)
ax.set_ylabel('Residuals', fontsize=11)
ax.set_title('Residual Plot (Test Set)', fontsize=12, fontweight='bold')
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

print(f"\n✓ Visualizations complete")

# COMMAND ----------

# DBTITLE 1,Save Models to MLflow
# Register models with MLflow
print("=" * 70)
print("MLFLOW MODEL REGISTRY")
print("=" * 70)

# Set MLflow experiment
mlflow.set_experiment(MLFLOW_EXPERIMENT)

print(f"\n✓ MLflow experiment: {MLFLOW_EXPERIMENT}")

# Create model signatures
from mlflow.models import infer_signature

# Infer signatures from training data
reg_signature = infer_signature(X_train, y_train_reg)
cls_signature = infer_signature(X_train_cls, y_train_cls_encoded)

print(f"\n✓ Model signatures created")

# Log Regression Model
with mlflow.start_run(run_name="HealthGPT_Risk_Predictor") as run:
    # Log parameters
    mlflow.log_params(xgb_reg_params)
    
    # Log metrics
    mlflow.log_metric("test_rmse", test_rmse)
    mlflow.log_metric("test_mae", test_mae)
    mlflow.log_metric("test_r2", test_r2)
    mlflow.log_metric("train_r2", train_r2)
    
    # Log model with signature
    mlflow.xgboost.log_model(
        xgb_reg, 
        "model",
        signature=reg_signature,
        registered_model_name="healthgpt_risk_predictor"
    )
    
    # Log feature importance
    feature_importance.to_csv("/tmp/feature_importance.csv", index=False)
    mlflow.log_artifact("/tmp/feature_importance.csv")
    
    print(f"\n✅ Regression model logged to MLflow")
    print(f"   Run ID: {run.info.run_id}")
    print(f"   Model: healthgpt_risk_predictor")

# Log Classification Model
with mlflow.start_run(run_name="HealthGPT_Category_Classifier") as run:
    # Log parameters
    mlflow.log_params(xgb_cls_params)
    
    # Log metrics
    mlflow.log_metric("test_accuracy", test_accuracy)
    mlflow.log_metric("train_accuracy", train_accuracy)
    
    # Log model with signature
    mlflow.xgboost.log_model(
        xgb_cls, 
        "model",
        signature=cls_signature,
        registered_model_name="healthgpt_category_classifier"
    )
    
    # Log label encoder as artifact
    import pickle
    with open("/tmp/label_encoder.pkl", "wb") as f:
        pickle.dump(label_encoder, f)
    mlflow.log_artifact("/tmp/label_encoder.pkl")
    
    print(f"\n✅ Classification model logged to MLflow")
    print(f"   Run ID: {run.info.run_id}")
    print(f"   Model: healthgpt_category_classifier")

print(f"\n{'='*70}")
print(f"✅ Phase 2 Complete: ML Models Ready for Production!")
print(f"{'='*70}")

# COMMAND ----------

# DBTITLE 1,Phase 2 Summary
# MAGIC %md
# MAGIC ## ✅ Phase 2 Complete!
# MAGIC
# MAGIC ### Models Created:
# MAGIC 1. **Risk Score Predictor** (XGBoost Regressor)
# MAGIC    - Predicts overall risk score (0-100 scale)
# MAGIC    - Registered in MLflow: `healthgpt_risk_predictor`
# MAGIC    - Evaluation: RMSE, MAE, R²
# MAGIC
# MAGIC 2. **Risk Category Classifier** (XGBoost Classifier)
# MAGIC    - Classifies districts into LOW/MEDIUM/HIGH/CRITICAL
# MAGIC    - Registered in MLflow: `healthgpt_category_classifier`
# MAGIC    - Evaluation: Accuracy, Precision, Recall, F1-Score
# MAGIC
# MAGIC ### Key Insights:
# MAGIC * **Top Risk Drivers**: Feature importance reveals which health indicators most strongly predict risk
# MAGIC * **Model Performance**: High R² and accuracy indicate models are production-ready
# MAGIC * **Interpretability**: Feature importance enables policy recommendations
# MAGIC
# MAGIC ### Integration Points:
# MAGIC | Component | Model Usage |
# MAGIC |-----------|-------------|
# MAGIC | **Dashboard** | Display predicted risk scores and categories |
# MAGIC | **Policy Engine** | Use feature importance to recommend targeted interventions |
# MAGIC | **What-If Simulator** | Predict risk changes when indicators are modified |
# MAGIC | **Genie Space** | Answer natural language queries about predictions |
# MAGIC
# MAGIC ### Next Steps:
# MAGIC - **Phase 3**: Create Genie Space for natural language policy queries
# MAGIC - **Phase 4**: Build Streamlit UI with model integration
# MAGIC - **Deployment**: Serve models via Databricks Model Serving

# COMMAND ----------

# DBTITLE 1,Model 3: District Clustering Analysis
# MAGIC %md
# MAGIC ## Model 3: District Clustering for Similar Districts
# MAGIC
# MAGIC Identify groups of districts with similar health profiles to enable:
# MAGIC * **Peer Comparison**: Compare district performance within its cluster
# MAGIC * **Targeted Interventions**: Recommend policies that worked in similar districts
# MAGIC * **Resource Allocation**: Group districts for efficient program rollout
# MAGIC * **Benchmarking**: Identify best-performing districts within each cluster
# MAGIC
# MAGIC ### Clustering Approach:
# MAGIC * **Algorithm**: K-Means clustering
# MAGIC * **Features**: Normalized health indicators and domain risk scores
# MAGIC * **Optimal K**: Determined by Elbow method and Silhouette score

# COMMAND ----------

# DBTITLE 1,Determine Optimal Number of Clusters
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

print("=" * 70)
print("DISTRICT CLUSTERING ANALYSIS")
print("=" * 70)

# Select features for clustering (normalized indicators)
clustering_features = health_features + composite_features + domain_features

# Prepare data
X_cluster = df[clustering_features].copy()

# Standardize features (important for K-Means)
scaler = StandardScaler()
X_cluster_scaled = scaler.fit_transform(X_cluster)

print(f"\n✓ Prepared {X_cluster.shape[1]} features for clustering")
print(f"  Districts: {X_cluster.shape[0]}")

# Determine optimal number of clusters using Elbow method
inertias = []
silhouette_scores = []
K_range = range(3, 11)  # Test 3 to 10 clusters

print(f"\n🔍 Testing cluster counts from {K_range.start} to {K_range.stop-1}...")

for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(X_cluster_scaled)
    inertias.append(kmeans.inertia_)
    silhouette_scores.append(silhouette_score(X_cluster_scaled, kmeans.labels_))

# Find optimal K (highest silhouette score)
optimal_k = K_range.start + silhouette_scores.index(max(silhouette_scores))

print(f"\n📊 Clustering Quality Metrics:")
for k, inertia, sil_score in zip(K_range, inertias, silhouette_scores):
    marker = " ← OPTIMAL" if k == optimal_k else ""
    print(f"  K={k}: Inertia={inertia:.0f}, Silhouette={sil_score:.4f}{marker}")

print(f"\n✓ Optimal number of clusters: {optimal_k}")

# Visualize elbow curve
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Elbow curve
ax1.plot(K_range, inertias, 'bo-', linewidth=2, markersize=8)
ax1.axvline(x=optimal_k, color='r', linestyle='--', linewidth=2, label=f'Optimal K={optimal_k}')
ax1.set_xlabel('Number of Clusters (K)', fontsize=12)
ax1.set_ylabel('Inertia (Within-cluster sum of squares)', fontsize=12)
ax1.set_title('Elbow Method', fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3)
ax1.legend()

# Silhouette scores
ax2.plot(K_range, silhouette_scores, 'go-', linewidth=2, markersize=8)
ax2.axvline(x=optimal_k, color='r', linestyle='--', linewidth=2, label=f'Optimal K={optimal_k}')
ax2.set_xlabel('Number of Clusters (K)', fontsize=12)
ax2.set_ylabel('Silhouette Score', fontsize=12)
ax2.set_title('Silhouette Analysis', fontsize=14, fontweight='bold')
ax2.grid(True, alpha=0.3)
ax2.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Train K-Means Clustering Model
# Train final K-Means model with optimal K
print("\n" + "=" * 70)
print(f"TRAINING K-MEANS MODEL (K={optimal_k})")
print("=" * 70)

kmeans_final = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
cluster_labels = kmeans_final.fit_predict(X_cluster_scaled)

# Add cluster assignments to dataframe
df['cluster'] = cluster_labels

print(f"\n✓ Clustering complete")
print(f"  Model: K-Means (K={optimal_k})")
print(f"  Silhouette Score: {silhouette_score(X_cluster_scaled, cluster_labels):.4f}")

# Cluster distribution
print(f"\n📊 Cluster Distribution:")
cluster_counts = df['cluster'].value_counts().sort_index()
for cluster_id, count in cluster_counts.items():
    pct = count / len(df) * 100
    print(f"  Cluster {cluster_id}: {count} districts ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Cluster Profiling and Characterization
# Profile each cluster
print("\n" + "=" * 70)
print("CLUSTER PROFILING")
print("=" * 70)

# Calculate cluster statistics
cluster_profiles = df.groupby('cluster').agg({
    'overall_risk_score': 'mean',
    'maternal_risk_score': 'mean',
    'anemia_risk_score': 'mean',
    'vaccination_risk_score': 'mean',
    'sanitation_risk_score': 'mean',
    'child_nutrition_risk_score': 'mean',
    'women_anemia_pct': 'mean',
    'child_stunting_pct': 'mean',
    'anc_4_visits_pct': 'mean',
    'vaccination_full_pct': 'mean',
    'sanitation_improved_pct': 'mean'
}).round(1)

# Assign cluster names based on characteristics
cluster_names = []
for cluster_id in range(optimal_k):
    profile = cluster_profiles.loc[cluster_id]
    
    # Determine cluster characteristics
    risk = profile['overall_risk_score']
    maternal = profile['maternal_risk_score']
    sanitation = profile['sanitation_risk_score']
    
    if risk < 30:
        name = "Low Risk - High Performance"
    elif risk < 40:
        if maternal > 50:
            name = "Moderate Risk - Maternal Health Challenge"
        elif sanitation > 50:
            name = "Moderate Risk - Infrastructure Gap"
        else:
            name = "Moderate Risk - Balanced"
    elif risk < 50:
        if maternal > 60:
            name = "High Risk - Critical Maternal Care"
        else:
            name = "High Risk - Multiple Challenges"
    else:
        name = "Critical Risk - Urgent Intervention Needed"
    
    cluster_names.append(name)

cluster_profiles['cluster_name'] = cluster_names
cluster_profiles['district_count'] = cluster_counts.values

print(f"\n📊 Cluster Profiles:\n")
for cluster_id, row in cluster_profiles.iterrows():
    print(f"\n{'='*70}")
    print(f"CLUSTER {cluster_id}: {row['cluster_name']}")
    print(f"{'='*70}")
    print(f"  Districts: {int(row['district_count'])}")
    print(f"  Overall Risk: {row['overall_risk_score']:.1f}/100")
    print(f"\n  Domain Risks:")
    print(f"    Maternal Care:    {row['maternal_risk_score']:.1f}")
    print(f"    Anemia:           {row['anemia_risk_score']:.1f}")
    print(f"    Vaccination:      {row['vaccination_risk_score']:.1f}")
    print(f"    Sanitation:       {row['sanitation_risk_score']:.1f}")
    print(f"    Child Nutrition:  {row['child_nutrition_risk_score']:.1f}")
    print(f"\n  Key Indicators:")
    print(f"    Women Anemia:        {row['women_anemia_pct']:.1f}%")
    print(f"    Child Stunting:      {row['child_stunting_pct']:.1f}%")
    print(f"    ANC 4+ Visits:       {row['anc_4_visits_pct']:.1f}%")
    print(f"    Full Vaccination:    {row['vaccination_full_pct']:.1f}%")
    print(f"    Improved Sanitation: {row['sanitation_improved_pct']:.1f}%")

# Save cluster names to dataframe
df['cluster_name'] = df['cluster'].map(dict(enumerate(cluster_names)))

# COMMAND ----------

# DBTITLE 1,Visualize Cluster Characteristics
# Visualize cluster characteristics
print("\n" + "=" * 70)
print("CLUSTER VISUALIZATION")
print("=" * 70)

# 1. Cluster comparison across domains
fig, ax = plt.subplots(figsize=(12, 6))

domain_cols = ['maternal_risk_score', 'anemia_risk_score', 'vaccination_risk_score', 
               'sanitation_risk_score', 'child_nutrition_risk_score']

cluster_profiles[domain_cols].plot(kind='bar', ax=ax, width=0.8)
ax.set_xlabel('Cluster', fontsize=12)
ax.set_ylabel('Risk Score', fontsize=12)
ax.set_title('Cluster Comparison: Domain Risk Scores', fontsize=14, fontweight='bold')
ax.set_xticklabels([f"C{i}: {cluster_names[i][:25]}..." for i in range(optimal_k)], rotation=45, ha='right')
ax.legend(['Maternal', 'Anemia', 'Vaccination', 'Sanitation', 'Child Nutrition'], 
          bbox_to_anchor=(1.05, 1), loc='upper left')
ax.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.show()

# 2. Cluster size and risk distribution
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Cluster sizes
cluster_counts_sorted = cluster_profiles['district_count'].sort_values(ascending=True)
colors = plt.cm.RdYlGn_r(cluster_profiles.loc[cluster_counts_sorted.index, 'overall_risk_score'] / 100)
ax1.barh(range(len(cluster_counts_sorted)), cluster_counts_sorted.values, color=colors)
ax1.set_yticks(range(len(cluster_counts_sorted)))
ax1.set_yticklabels([f"Cluster {i}" for i in cluster_counts_sorted.index])
ax1.set_xlabel('Number of Districts', fontsize=12)
ax1.set_title('Cluster Sizes', fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3, axis='x')

# Average risk by cluster
risk_sorted = cluster_profiles['overall_risk_score'].sort_values(ascending=True)
colors_risk = plt.cm.RdYlGn_r(risk_sorted / 100)
ax2.barh(range(len(risk_sorted)), risk_sorted.values, color=colors_risk)
ax2.set_yticks(range(len(risk_sorted)))
ax2.set_yticklabels([f"Cluster {i}" for i in risk_sorted.index])
ax2.set_xlabel('Average Risk Score', fontsize=12)
ax2.set_title('Average Risk by Cluster', fontsize=14, fontweight='bold')
ax2.grid(True, alpha=0.3, axis='x')
ax2.axvline(x=30, color='green', linestyle='--', alpha=0.5, label='Low threshold')
ax2.axvline(x=40, color='orange', linestyle='--', alpha=0.5, label='Medium threshold')
ax2.axvline(x=50, color='red', linestyle='--', alpha=0.5, label='High threshold')
ax2.legend()

plt.tight_layout()
plt.show()

print(f"\n✓ Visualizations complete")

# COMMAND ----------

# DBTITLE 1,PCA Visualization of Clusters
from sklearn.decomposition import PCA

# Reduce to 2D for visualization using PCA
pca = PCA(n_components=2, random_state=42)
X_pca = pca.fit_transform(X_cluster_scaled)

print(f"\n📊 PCA Variance Explained:")
print(f"  PC1: {pca.explained_variance_ratio_[0]*100:.1f}%")
print(f"  PC2: {pca.explained_variance_ratio_[1]*100:.1f}%")
print(f"  Total: {sum(pca.explained_variance_ratio_)*100:.1f}%")

# Create 2D scatter plot
fig, ax = plt.subplots(figsize=(12, 8))

scatter = ax.scatter(X_pca[:, 0], X_pca[:, 1], 
                     c=cluster_labels, 
                     cmap='tab10', 
                     s=50, 
                     alpha=0.6,
                     edgecolors='black',
                     linewidth=0.5)

# Plot cluster centroids
centroids_pca = pca.transform(kmeans_final.cluster_centers_)
ax.scatter(centroids_pca[:, 0], centroids_pca[:, 1], 
           c='red', 
           marker='X', 
           s=300, 
           edgecolors='black', 
           linewidth=2,
           label='Cluster Centroids')

# Annotate centroids with cluster names
for i, (x, y) in enumerate(centroids_pca):
    ax.annotate(f'C{i}', 
                xy=(x, y), 
                fontsize=12, 
                fontweight='bold',
                ha='center',
                va='center',
                color='white')

ax.set_xlabel(f'Principal Component 1 ({pca.explained_variance_ratio_[0]*100:.1f}% variance)', fontsize=12)
ax.set_ylabel(f'Principal Component 2 ({pca.explained_variance_ratio_[1]*100:.1f}% variance)', fontsize=12)
ax.set_title('District Clusters (PCA Visualization)', fontsize=14, fontweight='bold')
ax.legend()
ax.grid(True, alpha=0.3)

plt.colorbar(scatter, label='Cluster ID', ax=ax)
plt.tight_layout()
plt.show()

print(f"\n✓ PCA visualization complete")

# COMMAND ----------

# DBTITLE 1,Find Similar Districts Function
# Create function to find similar districts
def find_similar_districts(district_name, state_name, top_n=5):
    """
    Find the most similar districts to a given district.
    
    Args:
        district_name: Name of the target district
        state_name: State of the target district
        top_n: Number of similar districts to return
    
    Returns:
        DataFrame with similar districts and similarity scores
    """
    # Get target district data
    target = df[(df['district_name'] == district_name) & (df['state_ut'] == state_name)]
    
    if len(target) == 0:
        print(f"District '{district_name}' in '{state_name}' not found.")
        return None
    
    target_cluster = target['cluster'].values[0]
    target_features = target[clustering_features].values[0]
    target_scaled = scaler.transform([target_features])[0]
    
    # Get all districts in the same cluster
    same_cluster = df[df['cluster'] == target_cluster].copy()
    
    # Calculate Euclidean distance to target
    distances = []
    for idx, row in same_cluster.iterrows():
        if row['district_name'] == district_name and row['state_ut'] == state_name:
            continue  # Skip the target district itself
        
        features = row[clustering_features].values
        features_scaled = scaler.transform([features])[0]
        distance = np.linalg.norm(target_scaled - features_scaled)
        
        distances.append({
            'district_name': row['district_name'],
            'state_ut': row['state_ut'],
            'overall_risk_score': row['overall_risk_score'],
            'risk_category': row['risk_category'],
            'similarity_score': 100 / (1 + distance),  # Convert distance to similarity (0-100)
            'cluster': row['cluster'],
            'cluster_name': row['cluster_name']
        })
    
    # Sort by similarity and return top N
    similar_df = pd.DataFrame(distances).sort_values('similarity_score', ascending=False).head(top_n)
    
    return similar_df

# Test the function with an example
print("\n" + "=" * 70)
print("SIMILAR DISTRICTS ANALYSIS")
print("=" * 70)

# Pick a sample district (e.g., Nicobar from Tamil Nadu if it exists, otherwise first district)
sample_district = df[df['state_ut'] == 'Tamil Nadu'].iloc[0] if len(df[df['state_ut'] == 'Tamil Nadu']) > 0 else df.iloc[0]
sample_name = sample_district['district_name']
sample_state = sample_district['state_ut']

print(f"\n🎯 Finding similar districts to: {sample_name}, {sample_state}")
print(f"   Risk Score: {sample_district['overall_risk_score']:.1f}")
print(f"   Cluster: {sample_district['cluster']} ({sample_district['cluster_name']})")

similar_districts = find_similar_districts(sample_name, sample_state, top_n=5)

if similar_districts is not None:
    print(f"\n📊 Top 5 Most Similar Districts:\n")
    for idx, row in similar_districts.iterrows():
        print(f"  {idx+1}. {row['district_name']}, {row['state_ut']}")
        print(f"     Similarity: {row['similarity_score']:.1f}/100")
        print(f"     Risk Score: {row['overall_risk_score']:.1f} ({row['risk_category']})")
        print(f"     Cluster: {row['cluster']} ({row['cluster_name']})")
        print()

print(f"\n✓ Similar districts analysis complete")

# COMMAND ----------

# DBTITLE 1,Save Clustering Results
# Save clustering results to Delta table
print("\n" + "=" * 70)
print("SAVING CLUSTERING RESULTS")
print("=" * 70)

# Create table with cluster assignments
cluster_results = df[[
    'district_id', 'district_name', 'state_ut',
    'cluster', 'cluster_name', 'overall_risk_score', 'risk_category',
    'maternal_risk_score', 'anemia_risk_score', 'vaccination_risk_score',
    'sanitation_risk_score', 'child_nutrition_risk_score'
]].copy()

# Convert to Spark DataFrame
cluster_results_spark = spark.createDataFrame(cluster_results)

# Save to Gold schema
CLUSTER_TABLE = "workspace.healthgpt.district_clusters"

cluster_results_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(CLUSTER_TABLE)

print(f"\n✓ Cluster assignments saved to: {CLUSTER_TABLE}")
print(f"  Records: {cluster_results_spark.count()}")
print(f"  Columns: {len(cluster_results_spark.columns)}")

# Save clustering model and scaler to MLflow
# Create signatures for Unity Catalog compatibility
from mlflow.models import infer_signature

kmeans_signature = infer_signature(X_cluster_scaled, kmeans_final.predict(X_cluster_scaled))
scaler_signature = infer_signature(X_cluster, scaler.transform(X_cluster))

with mlflow.start_run(run_name="HealthGPT_District_Clustering") as run:
    # Log parameters
    mlflow.log_param("n_clusters", optimal_k)
    mlflow.log_param("algorithm", "KMeans")
    mlflow.log_param("n_features", len(clustering_features))
    
    # Log metrics
    mlflow.log_metric("silhouette_score", silhouette_score(X_cluster_scaled, cluster_labels))
    mlflow.log_metric("inertia", kmeans_final.inertia_)
    
    # Save cluster profiles
    cluster_profiles.to_csv("/tmp/cluster_profiles.csv")
    mlflow.log_artifact("/tmp/cluster_profiles.csv")
    
    # Save model and scaler with signatures
    mlflow.sklearn.log_model(
        kmeans_final,
        "kmeans_model",
        signature=kmeans_signature,
        registered_model_name="healthgpt_district_clustering"
    )
    
    mlflow.sklearn.log_model(
        scaler,
        "feature_scaler",
        signature=scaler_signature
    )
    
    print(f"\n✓ Clustering model saved to MLflow")
    print(f"   Run ID: {run.info.run_id}")
    print(f"   Model: healthgpt_district_clustering")

print(f"\n{'='*70}")
print(f"✅ District Clustering Complete!")
print(f"{'='*70}")
print(f"\n📊 Summary:")
print(f"  • {optimal_k} district clusters identified")
print(f"  • {len(df)} districts clustered")
print(f"  • Silhouette Score: {silhouette_score(X_cluster_scaled, cluster_labels):.4f}")
print(f"  • Results saved to: {CLUSTER_TABLE}")
print(f"  • Model registered in MLflow")

# COMMAND ----------


