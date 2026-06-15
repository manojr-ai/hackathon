# Databricks notebook source
# DBTITLE 1,Setup and Imports
# ============================================================================
# HEALTHGPT CARE GAP TRUST PLANNER - NOTEBOOK 2: GOLD LAYER (RULE-BASED)
# Track 2: Medical Desert Planner
# ============================================================================
# Purpose: Extract evidence using keyword taxonomy, apply rule-based trust scoring,
#          calculate geographic care gap scores, identify risk indicators
# Input: workspace.healthgpt.facilities_silver
# Output: facility_capability_evidence, facility_trust_scores, care_gap_gold,
#         gap_risk_indicators
# ============================================================================

import json
import re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("="*80)
print("HEALTHGPT NOTEBOOK 2: GOLD LAYER (RULE-BASED)")
print("="*80)
print(f"\n📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n✓ Imports loaded")

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

# COMMAND ----------

# DBTITLE 1,Define Capability Taxonomy
# ============================================================================
# CAPABILITY TAXONOMY: 7 Core Care Capabilities
# ============================================================================
print("\n" + "="*80)
print("CAPABILITY TAXONOMY")
print("="*80)

CAPABILITY_TAXONOMY = {
    "maternity": {
        "name": "Maternity Care",
        "keywords": ["maternity", "maternal", "obstetric", "labor", "delivery", "prenatal", 
                      "antenatal", "postnatal", "pregnancy", "childbirth", "gynecology", "obgyn",
                      "neonatal", "midwife", "cesarean", "c-section", "ante natal", "post natal"]
    },
    "icu": {
        "name": "Intensive Care (ICU)",
        "keywords": ["icu", "intensive care", "critical care", "ventilator", "life support",
                      "icu bed", "iccu", "intensive", "criticalcare"]
    },
    "nicu": {
        "name": "Neonatal ICU",
        "keywords": ["nicu", "neonatal intensive", "premature", "preterm", "newborn icu",
                     "neonatal care", "incubator", "neonatal unit"]
    },
    "emergency": {
        "name": "Emergency Care",
        "keywords": ["emergency", "casualty", "24x7", "24/7", "trauma", "accident",
                      "emergency room", "er", "urgent care", "emergency ward", "casualty ward", "24 hours"]
    },
    "trauma": {
        "name": "Trauma Care",
        "keywords": ["trauma", "polytrauma", "trauma center", "trauma unit", "injury care",
                     "accident care", "trauma surgery", "emergency surgery", "major injury"]
    },
    "oncology": {
        "name": "Oncology",
        "keywords": ["oncology", "cancer", "chemotherapy", "radiation", "radiotherapy",
                     "tumor", "malignancy", "chemo", "oncologist", "cancer treatment"]
    },
    "dialysis": {
        "name": "Dialysis",
        "keywords": ["dialysis", "hemodialysis", "renal", "kidney", "nephrology",
                     "dialysis center", "kidney failure", "dialysis unit", "hemo dialysis"]
    }
}

print("\n📊 Capability Taxonomy Defined: 7 capabilities")
for cap_id, cap_info in CAPABILITY_TAXONOMY.items():
    print(f"   {cap_info['name']}: {len(cap_info['keywords'])} keywords")

# COMMAND ----------

# DBTITLE 1,Extract Evidence from Facilities
# ============================================================================
# EVIDENCE EXTRACTION: Keyword-Based Mining
# ============================================================================
print("\n" + "="*80)
print("EVIDENCE EXTRACTION")
print("="*80)

# Load silver
df_facilities = spark.table(SILVER_TABLE)

print(f"\n✓ Loaded {df_facilities.count():,} facilities from Silver")

# Extract evidence per capability
evidence_records = []

for cap_id, cap_info in CAPABILITY_TAXONOMY.items():
    print(f"\n🔍 Extracting evidence for: {cap_info['name']}")
    
    keywords = cap_info['keywords']
    
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
            "capability", F.lit(cap_id)
        ).withColumn(
            "evidence_field", F.lit(field_label)
        ).withColumn(
            "evidence_text", F.col(col_name)
        ).withColumn(
            "extraction_date", F.current_timestamp()
        )
        
        match_count = df_matches.count()
        if match_count > 0:
            print(f"   {field_label}: {match_count:,} matches")
            evidence_records.append(df_matches)

# Union all evidence
if evidence_records:
    df_evidence = evidence_records[0]
    for df in evidence_records[1:]:
        df_evidence = df_evidence.union(df)
    
    evidence_count = df_evidence.count()
    print(f"\n✓ Extracted {evidence_count:,} total evidence records")
    
    # Save evidence to table
    EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"
    df_evidence.write.mode("overwrite").saveAsTable(EVIDENCE_TABLE)
    print(f"\n✅ Evidence table created: {EVIDENCE_TABLE}")
else:
    print("\n⚠️ No evidence found!")

# COMMAND ----------

# DBTITLE 1,Rule-Based Trust Scoring
# ============================================================================
# TRUST SCORING: Rule-Based Classification
# ============================================================================
print("\n" + "="*80)
print("TRUST SCORING: RULE-BASED CLASSIFICATION")
print("="*80)

EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"

# Count evidence per facility-capability
df_evidence_agg = spark.table(EVIDENCE_TABLE).groupBy("facility_id", "capability").agg(
    F.count("*").alias("evidence_count"),
    F.countDistinct("evidence_field").alias("field_diversity"),
    F.first("facility_name").alias("facility_name"),
    F.first("state").alias("state"),
    F.first("city").alias("city"),
    F.first("data_quality").alias("data_quality")
)

print(f"\n✓ Aggregated evidence for {df_evidence_agg.count():,} facility-capability pairs")

# Apply trust scoring rules
print("\n🎯 Applying trust scoring logic...")

df_trust = df_evidence_agg.withColumn(
    "trust_signal",
    F.when(
        (F.col("field_diversity") >= 3) & (F.col("evidence_count") >= 5), "strong"
    ).when(
        (F.col("field_diversity") >= 2) | 
        ((F.col("field_diversity") == 1) & (F.col("evidence_count") >= 3)), "partial"
    ).otherwise("weak")
).withColumn(
    "confidence_score",
    F.when(F.col("trust_signal") == "strong", 90)
     .when(F.col("trust_signal") == "partial", 65)
     .otherwise(35)
).withColumn(
    "scoring_date", F.current_timestamp()
)

trust_count = df_trust.count()
print(f"\n✓ Scored {trust_count:,} facility-capability pairs")

# Trust distribution
trust_dist = df_trust.groupBy("trust_signal").count().orderBy("trust_signal").collect()
print("\n📊 Trust Signal Distribution:")
for row in trust_dist:
    pct = (row['count'] / trust_count) * 100
    print(f"   {row.trust_signal}: {row['count']:,} ({pct:.1f}%)")

# Save trust scores
TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"
df_trust.write.mode("overwrite").saveAsTable(TRUST_TABLE)
print(f"\n✅ Trust scores table created: {TRUST_TABLE}")

# COMMAND ----------

# DBTITLE 1,Geographic Aggregation: Care Gap Scores
# ============================================================================
# GEOGRAPHIC AGGREGATION: Calculate Care Gap Scores by State x Capability
# ============================================================================
print("\n" + "="*80)
print("GEOGRAPHIC AGGREGATION: CALCULATING CARE GAP SCORES")
print("="*80)

TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"

# Calculate gap scores by state x capability
df_gaps = spark.table(TRUST_TABLE).groupBy("state", "capability").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_count"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_count"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_count"),
    F.avg("confidence_score").alias("avg_confidence"),
    F.stddev("confidence_score").alias("confidence_stddev")
).withColumn(
    "strong_pct", (F.col("strong_count") / F.col("total_facilities")) * 100
).withColumn(
    "partial_pct", (F.col("partial_count") / F.col("total_facilities")) * 100
).withColumn(
    "weak_pct", (F.col("weak_count") / F.col("total_facilities")) * 100
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
     .when(F.col("avg_confidence") >= 35, "LOW-MEDIUM")
     .otherwise("LOW")
).withColumn(
    "geography_type", F.lit("state")
).withColumn(
    "city", F.lit(None).cast(StringType())
).withColumn(
    "calculation_date", F.current_timestamp()
)

gap_count = df_gaps.count()
print(f"\n✓ Calculated {gap_count} state-level care gaps")

# Save gap scores
GAP_TABLE = "workspace.healthgpt.care_gap_gold"
df_gaps.write.mode("overwrite").saveAsTable(GAP_TABLE)
print(f"\n✅ Care gap table created: {GAP_TABLE}")

# Show top 5 gaps across all capabilities
print("\n⚠️ TOP 5 CARE GAPS (All Capabilities):")
top_gaps = spark.table(GAP_TABLE).orderBy(F.col("gap_score").desc()).limit(5).collect()

for i, row in enumerate(top_gaps, 1):
    print(f"   {i}. {row.state} - {row.capability}: Gap={row.gap_score:.1f}/100, Confidence={row.confidence_level}")

# Show top 5 maternity gaps specifically
print("\n👶 TOP 5 MATERNITY CARE GAPS:")
top_maternity = spark.table(GAP_TABLE).filter(
    F.col("capability") == "maternity"
).orderBy(F.col("gap_score").desc()).limit(5).collect()

for i, row in enumerate(top_maternity, 1):
    print(f"   {i}. {row.state}: Gap={row.gap_score:.1f}/100, Facilities={int(row.total_facilities)}, Confidence={row.confidence_level}")

# COMMAND ----------

# DBTITLE 1,Identify Top Risk Indicators (For UI Screen 1)
# ============================================================================
# RISK INDICATORS: Top 10 Risk Factors (For Overview Screen)
# ============================================================================
print("\n" + "="*80)
print("RISK INDICATORS ANALYSIS")
print("="*80)

GAP_TABLE = "workspace.healthgpt.care_gap_gold"
TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"
SILVER_TABLE = "workspace.healthgpt.facilities_silver"

# Calculate various risk indicators
print("\n🔍 Calculating risk indicators...")

# 1. Low facility density
total_facilities = spark.table(SILVER_TABLE).count()
total_states = spark.table(SILVER_TABLE).select("state").distinct().count()
avg_facilities_per_state = total_facilities / total_states
low_density_states = spark.table(SILVER_TABLE).groupBy("state").count().filter(
    F.col("count") < avg_facilities_per_state * 0.5
).count()
low_density_pct = (low_density_states / total_states) * 100

# 2. Weak evidence prevalence
weak_evidence = spark.table(TRUST_TABLE).filter(F.col("trust_signal") == "weak").count()
total_pairs = spark.table(TRUST_TABLE).count()
weak_evidence_pct = (weak_evidence / total_pairs) * 100

# 3. Missing maternity services
maternity_gaps = spark.table(GAP_TABLE).filter(
    (F.col("capability") == "maternity") & (F.col("gap_score") > 50)
).count()
maternity_gap_pct = (maternity_gaps / total_states) * 100

# 4. Missing ICU capacity
icu_gaps = spark.table(GAP_TABLE).filter(
    (F.col("capability") == "icu") & (F.col("gap_score") > 50)
).count()
icu_gap_pct = (icu_gaps / total_states) * 100

# 5. No emergency services
emergency_gaps = spark.table(GAP_TABLE).filter(
    (F.col("capability") == "emergency") & (F.col("gap_score") > 50)
).count()
emergency_gap_pct = (emergency_gaps / total_states) * 100

# 6. Low confidence scoring
low_confidence = spark.table(GAP_TABLE).filter(
    F.col("confidence_level").isin(["LOW", "LOW-MEDIUM"])
).count()
total_gaps = spark.table(GAP_TABLE).count()
low_confidence_pct = (low_confidence / total_gaps) * 100

# 7. Sparse data quality
sparse_facilities = spark.table(SILVER_TABLE).filter(
    F.col("data_quality") == "SPARSE"
).count()
sparse_pct = (sparse_facilities / total_facilities) * 100

# 8. Missing trauma centers
trauma_gaps = spark.table(GAP_TABLE).filter(
    (F.col("capability") == "trauma") & (F.col("gap_score") > 60)
).count()
trauma_gap_pct = (trauma_gaps / total_states) * 100

# 9. Missing NICU facilities
nicu_gaps = spark.table(GAP_TABLE).filter(
    (F.col("capability") == "nicu") & (F.col("gap_score") > 50)
).count()
nicu_gap_pct = (nicu_gaps / total_states) * 100

# 10. High gap variance
high_variance = spark.table(GAP_TABLE).filter(
    F.col("confidence_stddev") > 20
).count()
high_variance_pct = (high_variance / total_gaps) * 100

# Create risk indicators table
risk_indicators = [
    {"indicator_id": 1, "indicator_name": "Low facility density", "risk_pct": low_density_pct, "priority": "HIGH"},
    {"indicator_id": 2, "indicator_name": "Weak evidence prevalence", "risk_pct": weak_evidence_pct, "priority": "MEDIUM"},
    {"indicator_id": 3, "indicator_name": "Missing maternity services", "risk_pct": maternity_gap_pct, "priority": "HIGH"},
    {"indicator_id": 4, "indicator_name": "Missing ICU capacity", "risk_pct": icu_gap_pct, "priority": "HIGH"},
    {"indicator_id": 5, "indicator_name": "No emergency services", "risk_pct": emergency_gap_pct, "priority": "CRITICAL"},
    {"indicator_id": 6, "indicator_name": "Low confidence scoring", "risk_pct": low_confidence_pct, "priority": "MEDIUM"},
    {"indicator_id": 7, "indicator_name": "Sparse data quality", "risk_pct": sparse_pct, "priority": "LOW"},
    {"indicator_id": 8, "indicator_name": "Missing trauma centers", "risk_pct": trauma_gap_pct, "priority": "HIGH"},
    {"indicator_id": 9, "indicator_name": "Missing NICU facilities", "risk_pct": nicu_gap_pct, "priority": "HIGH"},
    {"indicator_id": 10, "indicator_name": "High gap variance", "risk_pct": high_variance_pct, "priority": "LOW"}
]

df_risk = spark.createDataFrame(risk_indicators).withColumn(
    "analysis_date", F.current_timestamp()
)

RISK_TABLE = "workspace.healthgpt.gap_risk_indicators"
df_risk.write.mode("overwrite").saveAsTable(RISK_TABLE)

print(f"\n✅ Risk indicators table created: {RISK_TABLE}")
print("\n📊 TOP 10 RISK INDICATORS (for Overview UI):")
for indicator in sorted(risk_indicators, key=lambda x: x['risk_pct'], reverse=True):
    print(f"   [{indicator['priority']}] {indicator['indicator_name']}: {indicator['risk_pct']:.1f}%")

print(f"\n📅 Notebook 2 Complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n➡️ Next: Run Notebook 3 (ML Models)")

# COMMAND ----------


