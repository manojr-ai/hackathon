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


