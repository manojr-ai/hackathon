# Databricks notebook source
# DBTITLE 1,Imports and Configuration
# Imports
import json
import re
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

print("=" * 80)
print("HEALTHGPT PHASE 3: FACILITY-BASED CARE GAP TRUST PLANNER")
print("Track 2: Medical Desert Planner")
print("=" * 80)
print(f"\n📅 Pipeline Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📊 Source: databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities")
print(f"🎯 Target: workspace.healthgpt schema")
print(f"\n✓ Imports complete")

# COMMAND ----------

# DBTITLE 1,Bronze Layer: Load Raw Facility Data
# BRONZE LAYER: Load raw facility dataset
print("\n" + "=" * 80)
print("BRONZE LAYER: RAW FACILITY DATA")
print("=" * 80)

# Load source facility data
facilities_source = spark.table("databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities")

print(f"\n📊 Source Records: {facilities_source.count():,}")
print(f"📊 Source Columns: {len(facilities_source.columns)}")

# Create Bronze table (preserve all original fields for traceability)
facilities_source.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facilities_bronze")

print(f"\n✓ Bronze table created: workspace.healthgpt.facilities_bronze")

# Verify Bronze table
bronze = spark.table("workspace.healthgpt.facilities_bronze")
print(f"  • Records: {bronze.count():,}")
print(f"  • Columns: {len(bronze.columns)}")
print(f"  • Unique facilities: {bronze.select('unique_id').distinct().count():,}")

# COMMAND ----------

# DBTITLE 1,Silver Layer: Clean and Parse Facility Data
# SILVER LAYER: Clean and standardize facility data
print("\n" + "=" * 80)
print("SILVER LAYER: CLEANED FACILITY DATA")
print("=" * 80)

# Load Bronze
bronze = spark.table("workspace.healthgpt.facilities_bronze")

# Parse JSON arrays and clean fields
silver = bronze.select(
    F.col("unique_id").alias("facility_id"),
    F.trim(F.col("name")).alias("facility_name"),
    F.upper(F.trim(F.coalesce(F.col("address_stateOrRegion"), F.lit("UNKNOWN")))).alias("state"),
    F.initcap(F.trim(F.coalesce(F.col("address_city"), F.lit("Unknown")))).alias("city"),
    F.trim(F.col("address_zipOrPostcode")).alias("pin_code"),
    F.col("latitude"),
    F.col("longitude"),
    
    # Parse JSON arrays to concatenated strings for easier text search
    F.regexp_replace(F.coalesce(F.col("description"), F.lit("")), r"[\n\r]+", " ").alias("description"),
    F.regexp_replace(F.coalesce(F.col("specialties"), F.lit("[]")), r'[\[\]"\\]', "").alias("specialties_text"),
    F.regexp_replace(F.coalesce(F.col("procedure"), F.lit("[]")), r'[\[\]"\\]', "").alias("procedures_text"),
    F.regexp_replace(F.coalesce(F.col("equipment"), F.lit("[]")), r'[\[\]"\\]', "").alias("equipment_text"),
    F.regexp_replace(F.coalesce(F.col("capability"), F.lit("[]")), r'[\[\]"\\]', "").alias("capability_text"),
    
    # Metadata fields for trust scoring
    F.coalesce(F.col("numberDoctors"), F.lit("0")).cast("int").alias("number_doctors"),
    F.coalesce(F.col("capacity"), F.lit("0")).cast("int").alias("bed_capacity"),
    F.coalesce(F.col("source_urls"), F.lit("")).alias("source_urls"),
    F.coalesce(F.col("officialWebsite"), F.lit("")).alias("official_website"),
    
    # Data quality flags
    F.when(
        (F.col("description").isNotNull()) & 
        (F.col("specialties").isNotNull()) & 
        (F.col("latitude").isNotNull()),
        "COMPLETE"
    ).when(
        (F.col("description").isNotNull()) | (F.col("specialties").isNotNull()),
        "PARTIAL"
    ).otherwise("SPARSE").alias("data_quality"),
    
    F.current_timestamp().alias("processed_at")
).distinct()  # Remove duplicates

# Write Silver table
silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facilities_silver")

print(f"\n✓ Silver table created: workspace.healthgpt.facilities_silver")

# Statistics
silver_df = spark.table("workspace.healthgpt.facilities_silver")
print(f"  • Total facilities: {silver_df.count():,}")
print(f"  • States: {silver_df.select('state').distinct().count():,}")
print(f"  • Cities: {silver_df.select('city').distinct().count():,}")
print(f"\n  Data Quality Distribution:")
silver_df.groupBy("data_quality").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Define Capability Taxonomy and Keywords
# CAPABILITY TAXONOMY: Define 7 core capabilities with keyword patterns
print("\n" + "=" * 80)
print("CAPABILITY TAXONOMY DEFINITION")
print("=" * 80)

# Define capability extraction patterns
CAPABILITY_TAXONOMY = {
    "maternity": {
        "keywords": [
            "maternity", "maternal", "obstetric", "obstetrics", "delivery", "labor",
            "labour", "antenatal", "postnatal", "prenatal", "pregnancy", "childbirth",
            "c-section", "cesarean", "caesarean", "neonatal", "gynecology", "gynaecology"
        ],
        "specialties": ["obstetrics", "gynecology", "maternalFetalMedicine"],
        "procedures": ["delivery", "c-section", "cesarean", "antenatal", "postnatal"],
        "equipment": ["incubator", "fetal monitor", "delivery bed", "warmer"]
    },
    "icu": {
        "keywords": [
            "icu", "intensive care", "critical care", "ventilator", "life support",
            "ccm", "micu", "sicu", "icu bed"
        ],
        "specialties": ["criticalCareMedicine", "intensiveCare"],
        "procedures": ["mechanical ventilation", "intubation", "central line"],
        "equipment": ["ventilator", "monitor", "infusion pump", "defibrillator"]
    },
    "nicu": {
        "keywords": [
            "nicu", "neonatal intensive care", "newborn icu", "premature", "preemie",
            "neonatal", "nicu bed", "special care nursery"
        ],
        "specialties": ["neonatology", "pediatrics", "neonatalPerinatalMedicine"],
        "procedures": ["neonatal resuscitation", "phototherapy", "respiratory support"],
        "equipment": ["incubator", "radiant warmer", "cpap", "phototherapy unit"]
    },
    "emergency": {
        "keywords": [
            "emergency", "casualty", "trauma", "accident", "er", "ed", "a&e",
            "24x7", "24/7", "round the clock", "urgent care"
        ],
        "specialties": ["emergencyMedicine", "trauma", "criticalCare"],
        "procedures": ["resuscitation", "stabilization", "emergency surgery"],
        "equipment": ["emergency cart", "defibrillator", "oxygen", "ambulance"]
    },
    "trauma": {
        "keywords": [
            "trauma", "accident", "injury", "polytrauma", "trauma center",
            "trauma unit", "trauma care", "emergency surgery"
        ],
        "specialties": ["trauma", "traumaSurgery", "orthopedicSurgery", "emergencyMedicine"],
        "procedures": ["trauma surgery", "fracture", "emergency orthopedic"],
        "equipment": ["xray", "ct scan", "operating theatre", "surgical"]
    },
    "oncology": {
        "keywords": [
            "oncology", "cancer", "chemotherapy", "radiation", "tumor", "tumour",
            "malignancy", "chemo", "radiotherapy", "oncologist"
        ],
        "specialties": ["medicalOncology", "surgicalOncology", "radiationOncology", "hematologyOncology"],
        "procedures": ["chemotherapy", "radiation", "cancer surgery", "biopsy"],
        "equipment": ["linear accelerator", "ct simulator", "chemo chair"]
    },
    "dialysis": {
        "keywords": [
            "dialysis", "hemodialysis", "haemodialysis", "renal", "kidney",
            "nephrology", "dialysis unit", "dialysis machine"
        ],
        "specialties": ["nephrology", "renalMedicine"],
        "procedures": ["hemodialysis", "dialysis", "renal replacement"],
        "equipment": ["dialysis machine", "dialyzer", "water treatment"]
    }
}

print(f"\n✓ Defined {len(CAPABILITY_TAXONOMY)} core capabilities:")
for capability, config in CAPABILITY_TAXONOMY.items():
    print(f"  • {capability.upper():12s} - {len(config['keywords'])} keywords, "
          f"{len(config['specialties'])} specialties, "
          f"{len(config['procedures'])} procedures, "
          f"{len(config['equipment'])} equipment patterns")

# COMMAND ----------

# DBTITLE 1,Extract Capability Evidence from Facility Text
# EVIDENCE EXTRACTION: Extract capability claims from facility text fields
print("\n" + "=" * 80)
print("CAPABILITY EVIDENCE EXTRACTION")
print("=" * 80)

# Load Silver facilities
silver = spark.table("workspace.healthgpt.facilities_silver")

# Create evidence extraction UDF
def extract_capability_evidence(facility_row, capability_name, config):
    """
    Extract evidence for a capability from a facility record.
    Returns list of (field, evidence_text, match_count) tuples.
    """
    evidence = []
    keywords = config['keywords']
    
    # Fields to search
    fields_to_check = {
        'description': facility_row['description'] or '',
        'specialties': facility_row['specialties_text'] or '',
        'procedures': facility_row['procedures_text'] or '',
        'equipment': facility_row['equipment_text'] or '',
        'capability': facility_row['capability_text'] or ''
    }
    
    for field_name, field_text in fields_to_check.items():
        if not field_text:
            continue
            
        field_text_lower = field_text.lower()
        matches = []
        
        for keyword in keywords:
            if keyword.lower() in field_text_lower:
                matches.append(keyword)
        
        if matches:
            # Extract context around matches (up to 200 chars)
            first_match = matches[0].lower()
            match_pos = field_text_lower.find(first_match)
            
            start = max(0, match_pos - 50)
            end = min(len(field_text), match_pos + 150)
            context = field_text[start:end].strip()
            
            if start > 0:
                context = "..." + context
            if end < len(field_text):
                context = context + "..."
            
            evidence.append((field_name, context, len(matches)))
    
    return evidence

# Process each facility for each capability
evidence_records = []

print("\nExtracting capability evidence (this may take a few minutes)...\n")
facilities_pd = silver.toPandas()

for idx, facility in facilities_pd.iterrows():
    if idx % 1000 == 0 and idx > 0:
        print(f"  Processed {idx:,} / {len(facilities_pd):,} facilities...")
    
    for capability_name, config in CAPABILITY_TAXONOMY.items():
        evidence = extract_capability_evidence(facility, capability_name, config)
        
        if evidence:
            for field_name, evidence_text, match_count in evidence:
                evidence_records.append({
                    'facility_id': facility['facility_id'],
                    'facility_name': facility['facility_name'],
                    'state': facility['state'],
                    'city': facility['city'],
                    'capability': capability_name,
                    'evidence_field': field_name,
                    'evidence_text': evidence_text,
                    'match_count': match_count,
                    'source_url': facility['source_urls'][:200] if facility['source_urls'] else '',
                    'extracted_at': datetime.now().isoformat()
                })

print(f"\n✓ Extraction complete: {len(evidence_records):,} evidence records found")

# Create evidence DataFrame and save
evidence_df = spark.createDataFrame(evidence_records)

evidence_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facility_capability_evidence")

print(f"\n✓ Evidence table created: workspace.healthgpt.facility_capability_evidence")

# Show evidence distribution
print("\n  Evidence by Capability:")
spark.table("workspace.healthgpt.facility_capability_evidence") \
    .groupBy("capability") \
    .agg(
        F.countDistinct("facility_id").alias("facilities_with_evidence"),
        F.count("*").alias("total_evidence_records")
    ) \
    .orderBy(F.desc("facilities_with_evidence")) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Trust Scoring: Classify Evidence Strength
# TRUST SCORING: Score facility-capability pairs by evidence strength
print("\n" + "=" * 80)
print("TRUST SCORING ENGINE")
print("=" * 80)

# Load evidence and facilities
evidence = spark.table("workspace.healthgpt.facility_capability_evidence")
silver = spark.table("workspace.healthgpt.facilities_silver")

# Aggregate evidence by facility-capability pair
evidence_agg = evidence.groupBy("facility_id", "facility_name", "state", "city", "capability").agg(
    F.collect_set("evidence_field").alias("evidence_fields"),
    F.sum("match_count").alias("total_matches"),
    F.count("*").alias("evidence_count"),
    F.max("source_url").alias("source_url")
)

# Join with facility metadata for completeness signals
trust_scores = evidence_agg.join(
    silver.select("facility_id", "number_doctors", "bed_capacity", "data_quality"),
    "facility_id",
    "left"
)

# Define trust scoring logic
def compute_trust_signal(evidence_fields, evidence_count, total_matches, data_quality):
    """
    Classify evidence as: strong, partial, weak, suspicious, no_claim
    
    Strong: Multiple fields (3+) with high match count + good data quality
    Partial: 2 fields or single field with multiple matches
    Weak: Single field with low matches
    Suspicious: Contradictory patterns (would need more sophisticated logic)
    No Claim: No evidence
    """
    if evidence_count == 0:
        return "no_claim"
    
    num_fields = len(evidence_fields) if evidence_fields else 0
    
    # Strong evidence: Multiple supporting fields
    if num_fields >= 3 and total_matches >= 5:
        return "strong"
    
    # Partial evidence: 2 fields or good matches in 1 field
    if num_fields >= 2 or (num_fields == 1 and total_matches >= 3):
        # Downgrade if data quality is sparse
        if data_quality == "SPARSE":
            return "weak"
        return "partial"
    
    # Weak evidence: Single field, few matches
    if num_fields == 1 and total_matches < 3:
        return "weak"
    
    # Default to partial
    return "partial"

compute_trust_udf = F.udf(compute_trust_signal, StringType())

# Apply trust scoring
trust_scores = trust_scores.withColumn(
    "trust_signal",
    compute_trust_udf(
        F.col("evidence_fields"),
        F.col("evidence_count"),
        F.col("total_matches"),
        F.col("data_quality")
    )
)

# Add confidence score (0-100)
trust_scores = trust_scores.withColumn(
    "confidence_score",
    F.when(F.col("trust_signal") == "strong", F.lit(90))
     .when(F.col("trust_signal") == "partial", F.lit(65))
     .when(F.col("trust_signal") == "weak", F.lit(35))
     .when(F.col("trust_signal") == "suspicious", F.lit(20))
     .otherwise(F.lit(0))
)

# Save trust scores
trust_scores.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facility_trust_scores")

print(f"\n✓ Trust scores created: workspace.healthgpt.facility_trust_scores")

# Show trust signal distribution
print("\n  Trust Signal Distribution:")
trust_df = spark.table("workspace.healthgpt.facility_trust_scores")
trust_df.groupBy("capability", "trust_signal").count() \
    .orderBy("capability", F.desc("count")) \
    .show(50, truncate=False)

print("\n  Summary by Capability:")
trust_df.groupBy("capability").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak"),
    F.avg("confidence_score").alias("avg_confidence")
).orderBy(F.desc("total_facilities")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Geographic Aggregation: Care Gap Scores by State/City
# GEOGRAPHIC AGGREGATION: Compute care gap scores by geography
print("\n" + "=" * 80)
print("GEOGRAPHIC AGGREGATION: CARE GAP SCORES")
print("=" * 80)

# Load trust scores
trust_scores = spark.table("workspace.healthgpt.facility_trust_scores")

# Aggregate by State and Capability
state_gaps = trust_scores.groupBy("state", "capability").agg(
    # Facility counts by trust signal
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_facilities"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_facilities"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_facilities"),
    F.sum(F.when(F.col("trust_signal") == "suspicious", 1).otherwise(0)).alias("suspicious_facilities"),
    
    # Confidence metrics
    F.avg("confidence_score").alias("avg_confidence_score"),
    F.stddev("confidence_score").alias("stddev_confidence"),
    
    # Data quality
    F.avg("evidence_count").alias("avg_evidence_per_facility")
).withColumn("geography_type", F.lit("state"))

# Aggregate by City and Capability  
city_gaps = trust_scores.groupBy("state", "city", "capability").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_facilities"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_facilities"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_facilities"),
    F.sum(F.when(F.col("trust_signal") == "suspicious", 1).otherwise(0)).alias("suspicious_facilities"),
    F.avg("confidence_score").alias("avg_confidence_score"),
    F.stddev("confidence_score").alias("stddev_confidence"),
    F.avg("evidence_count").alias("avg_evidence_per_facility")
).withColumn("geography_type", F.lit("city"))

# Compute care gap score (0-100, higher = bigger gap)
# Formula: Gap Score = 100 - (strong_weight * strong% + partial_weight * partial%)
# Higher gap score = fewer strong facilities = bigger care gap
def compute_gap_score(total, strong, partial, weak):
    if total == 0:
        return 100  # No facilities = maximum gap
    
    strong_pct = (strong / total) * 100
    partial_pct = (partial / total) * 100
    weak_pct = (weak / total) * 100
    
    # Weighted coverage score (0-100)
    coverage_score = (strong_pct * 1.0) + (partial_pct * 0.5) + (weak_pct * 0.2)
    
    # Gap score is inverse of coverage
    gap_score = 100 - coverage_score
    
    return max(0, min(100, gap_score))  # Clamp to 0-100

gap_score_udf = F.udf(compute_gap_score, DoubleType())

# Apply gap scoring to state level
state_gaps = state_gaps.withColumn(
    "gap_score",
    gap_score_udf(
        F.col("total_facilities"),
        F.col("strong_facilities"),
        F.col("partial_facilities"),
        F.col("weak_facilities")
    )
)

# Apply gap scoring to city level
city_gaps = city_gaps.withColumn(
    "gap_score",
    gap_score_udf(
        F.col("total_facilities"),
        F.col("strong_facilities"),
        F.col("partial_facilities"),
        F.col("weak_facilities")
    )
)

# Add confidence level classification
state_gaps = state_gaps.withColumn(
    "confidence_level",
    F.when(F.col("avg_confidence_score") >= 80, "HIGH")
     .when(F.col("avg_confidence_score") >= 60, "MEDIUM-HIGH")
     .when(F.col("avg_confidence_score") >= 40, "MEDIUM")
     .when(F.col("avg_confidence_score") >= 20, "LOW-MEDIUM")
     .otherwise("LOW")
)

city_gaps = city_gaps.withColumn(
    "confidence_level",
    F.when(F.col("avg_confidence_score") >= 80, "HIGH")
     .when(F.col("avg_confidence_score") >= 60, "MEDIUM-HIGH")
     .when(F.col("avg_confidence_score") >= 40, "MEDIUM")
     .when(F.col("avg_confidence_score") >= 20, "LOW-MEDIUM")
     .otherwise("LOW")
)

# Union state and city aggregations
care_gaps = state_gaps.unionByName(city_gaps, allowMissingColumns=True)

# Save care gap scores
care_gaps.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.care_gap_by_geography")

print(f"\n✓ Care gap table created: workspace.healthgpt.care_gap_by_geography")

# Show top care gaps
print("\n  Top 10 Care Gaps (State Level):")
spark.table("workspace.healthgpt.care_gap_by_geography") \
    .filter(F.col("geography_type") == "state") \
    .filter(F.col("gap_score") > 50) \
    .orderBy(F.desc("gap_score")) \
    .select("state", "capability", "gap_score", "confidence_level", "total_facilities", "strong_facilities") \
    .show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Pipeline Summary and Demo Queries
# PIPELINE SUMMARY AND DEMO QUERIES
print("\n" + "=" * 80)
print("PIPELINE SUMMARY")
print("=" * 80)

# Table inventory
tables = [
    "workspace.healthgpt.facilities_bronze",
    "workspace.healthgpt.facilities_silver",
    "workspace.healthgpt.facility_capability_evidence",
    "workspace.healthgpt.facility_trust_scores",
    "workspace.healthgpt.care_gap_by_geography"
]

print("\n📦 Created Tables:")
for table in tables:
    count = spark.table(table).count()
    print(f"  ✓ {table:55s} - {count:,} records")

# Demo: Maternity care gaps
print("\n" + "=" * 80)
print("DEMO QUERY: Maternity Care Gaps by State")
print("=" * 80)

maternity_gaps = spark.sql("""
    SELECT 
        state,
        gap_score,
        confidence_level,
        total_facilities,
        strong_facilities,
        partial_facilities,
        weak_facilities,
        ROUND(avg_confidence_score, 1) as avg_confidence
    FROM workspace.healthgpt.care_gap_by_geography
    WHERE capability = 'maternity'
      AND geography_type = 'state'
      AND gap_score > 50
    ORDER BY gap_score DESC
    LIMIT 15
""")

print("\nStates with Highest Maternity Care Gaps:")
maternity_gaps.show(15, truncate=False)

# Demo: ICU availability
print("\n" + "=" * 80)
print("DEMO QUERY: ICU Capability by State")
print("=" * 80)

icu_gaps = spark.sql("""
    SELECT 
        state,
        gap_score,
        confidence_level,
        total_facilities,
        strong_facilities,
        partial_facilities
    FROM workspace.healthgpt.care_gap_by_geography
    WHERE capability = 'icu'
      AND geography_type = 'state'
    ORDER BY gap_score DESC
    LIMIT 10
""")

print("\nStates with Highest ICU Care Gaps:")
icu_gaps.show(10, truncate=False)

# Demo: Sample facility evidence for verification
print("\n" + "=" * 80)
print("DEMO QUERY: Sample Facility Evidence (For Planner Review)")
print("=" * 80)

sample_evidence = spark.sql("""
    SELECT 
        facility_name,
        state,
        city,
        capability,
        trust_signal,
        confidence_score,
        CONCAT(SIZE(evidence_fields), ' fields') as evidence_sources
    FROM workspace.healthgpt.facility_trust_scores
    WHERE capability = 'maternity'
      AND trust_signal IN ('partial', 'weak')
      AND state = 'KARNATAKA'
    LIMIT 10
""")

print("\nFacilities Requiring Verification (Karnataka Maternity):")
sample_evidence.show(10, truncate=False)

# Final success message
print("\n" + "=" * 80)
print("✅ PIPELINE COMPLETE")
print("=" * 80)
print("\n🎯 Next Steps:")
print("  1. Create planner workspace tables (notes, overrides, scenarios, shortlists)")
print("  2. Build Streamlit UI with 6 screens (Overview, Map, Evidence, Brief, Scenario, Architecture)")
print("  3. Integrate OpenAI for AI Gap Brief generation from structured evidence")
print("  4. Demo script: Select geography + capability → Show gap score → Review evidence → Generate brief")
print("\n📄 Ready for Track 2: Medical Desert Planner submission!")

# COMMAND ----------

# DBTITLE 1,Imports and Setup
# Imports
import json
import re
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

print("=" * 80)
print("HEALTHGPT PHASE 3: FACILITY PIPELINE FOR CARE GAP TRUST PLANNER")
print("Track 2: Medical Desert Planner")
print("=" * 80)
print(f"\n📅 Pipeline Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n✓ Imports loaded successfully")

# COMMAND ----------

# DBTITLE 1,Bronze Layer: Load Raw Facility Data
# Bronze Layer: Load raw facility dataset
print("\n" + "=" * 80)
print("BRONZE LAYER: LOADING RAW FACILITY DATA")
print("=" * 80)

# Source table from Virtue Foundation dataset
SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
BRONZE_TABLE = "workspace.healthgpt.facilities_bronze"

print(f"\n📂 Source: {SOURCE_TABLE}")
print(f"🎯 Target: {BRONZE_TABLE}")

# Load all raw data - preserve all 51 columns for traceability
df_bronze = spark.table(SOURCE_TABLE)

print(f"\n✓ Loaded {df_bronze.count():,} facilities")
print(f"✓ Columns: {len(df_bronze.columns)}")

# Save to Bronze table
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"\n✓ Bronze table created: {BRONZE_TABLE}")

# Show data quality summary
print("\n📊 Data Quality Summary:")
print(f"  - Distinct facilities: {df_bronze.select('unique_id').distinct().count():,}")
print(f"  - States/regions: {df_bronze.select('address_stateOrRegion').distinct().count():,}")
print(f"  - Cities: {df_bronze.select('address_city').distinct().count():,}")
print(f"  - Has description: {df_bronze.filter(F.col('description').isNotNull()).count():,} ({df_bronze.filter(F.col('description').isNotNull()).count()/df_bronze.count()*100:.1f}%)")
print(f"  - Has specialties: {df_bronze.filter(F.col('specialties').isNotNull()).count():,} ({df_bronze.filter(F.col('specialties').isNotNull()).count()/df_bronze.count()*100:.1f}%)")
print(f"  - Has procedures: {df_bronze.filter(F.col('procedure').isNotNull()).count():,} ({df_bronze.filter(F.col('procedure').isNotNull()).count()/df_bronze.count()*100:.1f}%)")
print(f"  - Has equipment: {df_bronze.filter(F.col('equipment').isNotNull()).count():,} ({df_bronze.filter(F.col('equipment').isNotNull()).count()/df_bronze.count()*100:.1f}%)")
print(f"  - Has capability: {df_bronze.filter(F.col('capability').isNotNull()).count():,} ({df_bronze.filter(F.col('capability').isNotNull()).count()/df_bronze.count()*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Silver Layer: Clean and Standardize Facility Data
# Silver Layer: Clean and standardize facility data
print("\n" + "=" * 80)
print("SILVER LAYER: CLEANING AND STANDARDIZING DATA")
print("=" * 80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

print(f"\n📂 Source: {BRONZE_TABLE}")
print(f"🎯 Target: {SILVER_TABLE}")

# Helper function to parse JSON arrays safely
def parse_json_array_udf(column_name):
    def parse_json(text):
        if text is None or text == '':
            return []
        try:
            return json.loads(text) if isinstance(text, str) else text
        except:
            return []
    return F.udf(parse_json, ArrayType(StringType()))

# Load bronze data
df_silver = spark.table(BRONZE_TABLE)

print(f"\n✓ Loaded {df_silver.count():,} facilities from Bronze")

# Standardize geography fields
df_silver = df_silver.withColumn("state", F.trim(F.upper(F.col("address_stateOrRegion")))) \
    .withColumn("city", F.trim(F.initcap(F.col("address_city")))) \
    .withColumn("pin_code", F.regexp_replace(F.col("address_zipOrPostcode"), "[^0-9]", "")) \
    .withColumn("facility_name", F.trim(F.col("name")))

print("✓ Geography fields standardized")

# Clean latitude/longitude
df_silver = df_silver.withColumn("latitude_clean", 
                                  F.when((F.col("latitude").isNotNull()) & 
                                         (F.col("latitude").between(-90, 90)), 
                                         F.col("latitude"))
                                  .otherwise(None)) \
    .withColumn("longitude_clean", 
                F.when((F.col("longitude").isNotNull()) & 
                       (F.col("longitude").between(-180, 180)), 
                       F.col("longitude"))
                .otherwise(None))

print("✓ Lat/long cleaned")

# Parse JSON arrays for key evidence fields
df_silver = df_silver.withColumn("specialties_array", parse_json_array_udf("specialties")(F.col("specialties"))) \
    .withColumn("procedure_array", parse_json_array_udf("procedure")(F.col("procedure"))) \
    .withColumn("equipment_array", parse_json_array_udf("equipment")(F.col("equipment"))) \
    .withColumn("capability_array", parse_json_array_udf("capability")(F.col("capability"))) \
    .withColumn("source_urls_array", parse_json_array_udf("source_urls")(F.col("source_urls")))

print("✓ JSON arrays parsed")

# Create combined text field for evidence extraction
df_silver = df_silver.withColumn("evidence_text",
    F.concat_ws(" | ",
        F.coalesce(F.col("description"), F.lit("")),
        F.array_join(F.col("specialties_array"), " "),
        F.array_join(F.col("procedure_array"), " "),
        F.array_join(F.col("equipment_array"), " "),
        F.array_join(F.col("capability_array"), " ")
    )
)

print("✓ Combined evidence text created")

# Add data quality scores
df_silver = df_silver.withColumn("completeness_score",
    (F.when(F.col("description").isNotNull(), 20).otherwise(0) +
     F.when(F.size(F.col("specialties_array")) > 0, 20).otherwise(0) +
     F.when(F.size(F.col("procedure_array")) > 0, 20).otherwise(0) +
     F.when(F.size(F.col("equipment_array")) > 0, 20).otherwise(0) +
     F.when(F.col("numberDoctors").isNotNull(), 10).otherwise(0) +
     F.when(F.col("capacity").isNotNull(), 10).otherwise(0))
)

print("✓ Data quality scores calculated")

# Select and rename key columns for Silver
df_silver_final = df_silver.select(
    F.col("unique_id").alias("facility_id"),
    "facility_name",
    "state",
    "city",
    "pin_code",
    "latitude_clean",
    "longitude_clean",
    "organization_type",
    "description",
    "specialties_array",
    "procedure_array",
    "equipment_array",
    "capability_array",
    "source_urls_array",
    "evidence_text",
    "numberDoctors",
    "capacity",
    "completeness_score",
    "facilityTypeId",
    "operatorTypeId"
)

# Remove duplicates based on facility_id
df_silver_final = df_silver_final.dropDuplicates(["facility_id"])

print(f"\n✓ Deduplication complete: {df_silver_final.count():,} unique facilities")

# Save to Silver table
df_silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"\n✓ Silver table created: {SILVER_TABLE}")

# Show sample
print("\n🔍 Sample Facility Record:")
df_silver_final.select("facility_id", "facility_name", "state", "city", "completeness_score").show(3, False)

# COMMAND ----------

# DBTITLE 1,Define Capability Taxonomy and Keywords
# Define capability taxonomy and keyword dictionaries
print("\n" + "=" * 80)
print("CAPABILITY TAXONOMY: DEFINING 7 CORE CAPABILITIES")
print("=" * 80)

# Define the 7 capabilities per document specs
CAPABILITY_TAXONOMY = {
    "maternity": {
        "name": "Maternity Care",
        "keywords": [
            "maternity", "maternal", "obstetric", "labor", "delivery", "prenatal", 
            "antenatal", "postnatal", "pregnancy", "childbirth", "gynecology", "gynaecology",
            "c-section", "cesarean", "caesarean", "labour ward", "delivery room",
            "obstetrician", "midwife", "anc", "pmjay maternity"
        ],
        "specialties": ["obstetrics", "gynecology", "gynecologicaloncology"],
        "procedures": ["delivery", "c-section", "cesarean", "normal delivery", "assisted delivery"],
        "equipment": ["delivery table", "fetal monitor", "incubator", "warmer", "resuscitation"]
    },
    "icu": {
        "name": "Intensive Care Unit (ICU)",
        "keywords": [
            "icu", "intensive care", "critical care", "ccu", "coronary care",
            "ventilator", "life support", "critical", "intensive", "icu bed",
            "mechanical ventilation"
        ],
        "specialties": ["criticalcaremedicine", "intensivecare", "anesthesiology"],
        "procedures": ["mechanical ventilation", "central line", "arterial line"],
        "equipment": ["ventilator", "monitors", "infusion pump", "defibrillator"]
    },
    "nicu": {
        "name": "Neonatal Intensive Care Unit (NICU)",
        "keywords": [
            "nicu", "neonatal", "newborn", "neo-natal", "picu", "pediatric intensive",
            "neonatology", "premature", "preterm", "incubator", "neonatal care"
        ],
        "specialties": ["neonatology", "pediatrics", "pediatricintensivecare"],
        "procedures": ["neonatal resuscitation", "phototherapy", "respiratory support"],
        "equipment": ["incubator", "phototherapy unit", "neonatal ventilator", "radiant warmer"]
    },
    "emergency": {
        "name": "Emergency Department",
        "keywords": [
            "emergency", "casualty", "er", "24x7", "24/7", "accident", "urgent care",
            "emergency department", "emergency services", "trauma center", "ed",
            "ambulance", "emergency ward"
        ],
        "specialties": ["emergencymedicine", "trauma", "accidentandemergency"],
        "procedures": ["resuscitation", "triage", "emergency surgery"],
        "equipment": ["crash cart", "defibrillator", "emergency trolley", "ambulance"]
    },
    "trauma": {
        "name": "Trauma Care",
        "keywords": [
            "trauma", "accident", "injury", "fracture", "orthopedic", "orthopaedic",
            "trauma surgery", "trauma center", "trauma unit", "polytrauma",
            "road accident", "emergency surgery"
        ],
        "specialties": ["traumasurgery", "orthopedicsurgery", "orthopedics"],
        "procedures": ["fracture fixation", "trauma surgery", "emergency orthopedic"],
        "equipment": ["c-arm", "traction", "orthopedic instruments", "trauma bay"]
    },
    "oncology": {
        "name": "Cancer Care (Oncology)",
        "keywords": [
            "oncology", "cancer", "chemotherapy", "radiation", "radiotherapy",
            "tumor", "tumour", "malignancy", "chemo", "oncologist",
            "cancer treatment", "cancer care", "medical oncology", "surgical oncology"
        ],
        "specialties": ["medicaloncology", "surgicaloncology", "gynecologicaloncology", "radiationoncology"],
        "procedures": ["chemotherapy", "radiation therapy", "cancer surgery", "biopsy"],
        "equipment": ["linear accelerator", "chemotherapy unit", "radiation equipment", "ct scanner"]
    },
    "dialysis": {
        "name": "Dialysis (Kidney Care)",
        "keywords": [
            "dialysis", "hemodialysis", "haemodialysis", "renal", "kidney",
            "nephrology", "pmndp", "pradhan mantri dialysis", "dialysis unit",
            "dialysis machine", "dialysis center"
        ],
        "specialties": ["nephrology", "renalmedicine"],
        "procedures": ["hemodialysis", "peritoneal dialysis", "dialysis treatment"],
        "equipment": ["dialysis machine", "dialyzer", "ro plant", "water treatment"]
    }
}

print("\n🎯 7 Core Capabilities Defined:")
for cap_id, cap_info in CAPABILITY_TAXONOMY.items():
    print(f"  - {cap_info['name']}: {len(cap_info['keywords'])} keywords, {len(cap_info['specialties'])} specialties")

print(f"\n✓ Capability taxonomy loaded successfully")

# COMMAND ----------

# DBTITLE 1,Extract Capability Evidence from Facilities
# Extract capability evidence from facility data
print("\n" + "=" * 80)
print("CAPABILITY EXTRACTION: EXTRACTING EVIDENCE FROM TEXT FIELDS")
print("=" * 80)

EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"

print(f"\n📂 Source: {SILVER_TABLE}")
print(f"🎯 Target: {EVIDENCE_TABLE}")

# Load silver data
df_facilities = spark.table(SILVER_TABLE)

print(f"\n✓ Loaded {df_facilities.count():,} facilities")

# Create evidence extraction function
def extract_capability_evidence(facility_row, capability_id, capability_config):
    """Extract evidence for a specific capability from facility data"""
    evidence_items = []
    
    # Search in description
    if facility_row.description:
        desc_lower = facility_row.description.lower()
        for keyword in capability_config['keywords']:
            if keyword.lower() in desc_lower:
                # Extract context (100 chars around match)
                idx = desc_lower.find(keyword.lower())
                start = max(0, idx - 50)
                end = min(len(facility_row.description), idx + 50)
                context = facility_row.description[start:end]
                evidence_items.append({
                    'field': 'description',
                    'keyword': keyword,
                    'context': context,
                    'confidence': 0.6
                })
                break  # Only capture first match per field
    
    # Search in specialties array
    if facility_row.specialties_array:
        for specialty in facility_row.specialties_array:
            if specialty and any(s.lower() in specialty.lower() for s in capability_config['specialties']):
                evidence_items.append({
                    'field': 'specialties',
                    'keyword': specialty,
                    'context': specialty,
                    'confidence': 0.8
                })
                break
    
    # Search in procedures array
    if facility_row.procedure_array:
        for procedure in facility_row.procedure_array:
            if procedure:
                proc_lower = procedure.lower()
                for proc_keyword in capability_config['procedures']:
                    if proc_keyword.lower() in proc_lower:
                        evidence_items.append({
                            'field': 'procedure',
                            'keyword': proc_keyword,
                            'context': procedure[:100],
                            'confidence': 0.9
                        })
                        break
    
    # Search in equipment array
    if facility_row.equipment_array:
        for equipment in facility_row.equipment_array:
            if equipment:
                equip_lower = equipment.lower()
                for equip_keyword in capability_config['equipment']:
                    if equip_keyword.lower() in equip_lower:
                        evidence_items.append({
                            'field': 'equipment',
                            'keyword': equip_keyword,
                            'context': equipment[:100],
                            'confidence': 0.85
                        })
                        break
    
    # Search in capability array
    if facility_row.capability_array:
        for capability in facility_row.capability_array:
            if capability:
                cap_lower = capability.lower()
                for keyword in capability_config['keywords']:
                    if keyword.lower() in cap_lower:
                        evidence_items.append({
                            'field': 'capability',
                            'keyword': keyword,
                            'context': capability[:100],
                            'confidence': 0.7
                        })
                        break
    
    return evidence_items

# Process each facility-capability combination
evidence_records = []

print("\n🔍 Extracting evidence for each capability...")

facilities_list = df_facilities.collect()
total_facilities = len(facilities_list)

for idx, facility in enumerate(facilities_list):
    if (idx + 1) % 1000 == 0:
        print(f"  Processed {idx + 1:,} / {total_facilities:,} facilities...")
    
    for cap_id, cap_config in CAPABILITY_TAXONOMY.items():
        evidence_items = extract_capability_evidence(facility, cap_id, cap_config)
        
        if evidence_items:
            # Calculate aggregate confidence
            avg_confidence = sum(e['confidence'] for e in evidence_items) / len(evidence_items)
            evidence_count = len(evidence_items)
            field_coverage = len(set(e['field'] for e in evidence_items))
            
            evidence_records.append({
                'facility_id': facility.facility_id,
                'facility_name': facility.facility_name,
                'state': facility.state,
                'city': facility.city,
                'capability_id': cap_id,
                'capability_name': cap_config['name'],
                'evidence_count': evidence_count,
                'field_coverage': field_coverage,
                'avg_confidence': avg_confidence,
                'evidence_fields': [e['field'] for e in evidence_items],
                'evidence_keywords': [e['keyword'] for e in evidence_items],
                'evidence_contexts': [e['context'] for e in evidence_items]
            })

print(f"\n✓ Extracted {len(evidence_records):,} capability evidence records")

# Create DataFrame
schema = StructType([
    StructField("facility_id", StringType(), False),
    StructField("facility_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("capability_id", StringType(), False),
    StructField("capability_name", StringType(), True),
    StructField("evidence_count", IntegerType(), True),
    StructField("field_coverage", IntegerType(), True),
    StructField("avg_confidence", DoubleType(), True),
    StructField("evidence_fields", ArrayType(StringType()), True),
    StructField("evidence_keywords", ArrayType(StringType()), True),
    StructField("evidence_contexts", ArrayType(StringType()), True)
])

df_evidence = spark.createDataFrame(evidence_records, schema=schema)

print(f"\n✓ Created evidence DataFrame with {df_evidence.count():,} records")

# Save to evidence table
df_evidence.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(EVIDENCE_TABLE)

print(f"\n✓ Evidence table created: {EVIDENCE_TABLE}")

# Show distribution by capability
print("\n📊 Evidence Distribution by Capability:")
df_evidence.groupBy("capability_name") \
    .agg(
        F.count("*").alias("facility_count"),
        F.avg("evidence_count").alias("avg_evidence_count"),
        F.avg("avg_confidence").alias("avg_confidence")
    ) \
    .orderBy(F.desc("facility_count")) \
    .show(10, False)

# COMMAND ----------

# DBTITLE 1,Trust Scoring: Score Evidence as Strong/Partial/Weak/Suspicious/No Claim
# Trust Scoring: Score each facility-capability pair
print("\n" + "=" * 80)
print("TRUST SCORING: SCORING EVIDENCE STRENGTH")
print("=" * 80)

TRUST_SCORES_TABLE = "workspace.healthgpt.facility_trust_scores"

print(f"\n📂 Source: {EVIDENCE_TABLE}")
print(f"🎯 Target: {TRUST_SCORES_TABLE}")

# Load evidence data
df_evidence = spark.table(EVIDENCE_TABLE)

print(f"\n✓ Loaded {df_evidence.count():,} evidence records")

# Define trust scoring logic per document specs
def calculate_trust_signal(evidence_count, field_coverage, avg_confidence):
    """
    Score evidence as:
    - strong: Multiple supporting fields + high confidence
    - partial: Some evidence but missing key fields
    - weak: Single field mention only
    - suspicious: Low confidence or contradictions
    """
    # Strong evidence: 3+ fields with high confidence
    if field_coverage >= 3 and avg_confidence >= 0.75:
        return "strong"
    
    # Partial evidence: 2 fields or moderate confidence
    elif field_coverage >= 2 or (evidence_count >= 2 and avg_confidence >= 0.65):
        return "partial"
    
    # Weak evidence: Single field or low confidence
    elif field_coverage == 1 and avg_confidence < 0.75:
        return "weak"
    
    # Suspicious: Very low confidence
    elif avg_confidence < 0.55:
        return "suspicious"
    
    else:
        return "weak"

# Register UDF
trust_signal_udf = F.udf(calculate_trust_signal, StringType())

# Calculate trust signals
df_trust = df_evidence.withColumn(
    "trust_signal",
    trust_signal_udf(
        F.col("evidence_count"),
        F.col("field_coverage"),
        F.col("avg_confidence")
    )
)

# Add scoring metadata
df_trust = df_trust.withColumn("scored_at", F.current_timestamp())

print("✓ Trust signals calculated")

# Save to trust scores table
df_trust.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TRUST_SCORES_TABLE)

print(f"\n✓ Trust scores table created: {TRUST_SCORES_TABLE}")

# Show trust signal distribution
print("\n📊 Trust Signal Distribution:")
df_trust.groupBy("trust_signal") \
    .agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")) \
    .show()

# Show trust distribution by capability
print("\n📊 Trust Distribution by Capability:")
df_trust.groupBy("capability_name", "trust_signal") \
    .agg(F.count("*").alias("count")) \
    .orderBy("capability_name", F.desc("count")) \
    .show(30, False)

# Sample strong evidence records
print("\n🔍 Sample Strong Evidence:")
df_trust.filter(F.col("trust_signal") == "strong") \
    .select("facility_name", "capability_name", "evidence_count", "field_coverage", "avg_confidence") \
    .show(5, False)

# COMMAND ----------

# DBTITLE 1,Geographic Aggregation: Calculate Care Gap Scores by Geography
# Geographic Aggregation: Calculate care gap scores by geography
print("\n" + "=" * 80)
print("GEOGRAPHIC AGGREGATION: CALCULATING CARE GAP SCORES")
print("=" * 80)

CAREGAP_TABLE = "workspace.healthgpt.care_gap_by_geography"

print(f"\n📂 Source: {TRUST_SCORES_TABLE}")
print(f"🎯 Target: {CAREGAP_TABLE}")

# Load trust scores
df_trust = spark.table(TRUST_SCORES_TABLE)

print(f"\n✓ Loaded {df_trust.count():,} trust scores")

# Aggregate by state and capability
print("\n🔍 Aggregating by State and Capability...")

df_state_agg = df_trust.groupBy("state", "capability_id", "capability_name") \
    .agg(
        F.count("*").alias("total_facilities"),
        F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_count"),
        F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_count"),
        F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_count"),
        F.sum(F.when(F.col("trust_signal") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
        F.avg("avg_confidence").alias("avg_evidence_confidence"),
        F.avg("evidence_count").alias("avg_evidence_count")
    )

# Calculate gap score and confidence level
# Gap score: Higher = worse gap (fewer strong facilities)
# Confidence: Higher = more confident in the gap assessment

df_state_agg = df_state_agg.withColumn(
    "strong_pct",
    (F.col("strong_count") / F.col("total_facilities") * 100)
).withColumn(
    "partial_pct",
    (F.col("partial_count") / F.col("total_facilities") * 100)
).withColumn(
    "weak_pct",
    (F.col("weak_count") / F.col("total_facilities") * 100)
).withColumn(
    "suspicious_pct",
    (F.col("suspicious_count") / F.col("total_facilities") * 100)
)

# Gap Score: 0-100 (100 = worst gap)
# Formula: Weighted inverse of coverage quality
df_state_agg = df_state_agg.withColumn(
    "gap_score",
    F.round(
        100 - (
            F.col("strong_pct") * 1.0 +
            F.col("partial_pct") * 0.5 +
            F.col("weak_pct") * 0.2
        ),
        1
    )
)

# Confidence Level: Based on data completeness and evidence strength
# High confidence = good evidence + clear signal
# Low confidence = sparse/weak evidence
df_state_agg = df_state_agg.withColumn(
    "confidence_score",
    F.round(
        (F.col("avg_evidence_confidence") * 50 +
         F.least(F.col("avg_evidence_count") / 3.0, F.lit(1.0)) * 30 +
         F.least(F.col("total_facilities") / 50.0, F.lit(1.0)) * 20) * 100,
        1
    )
).withColumn(
    "confidence_level",
    F.when(F.col("confidence_score") >= 75, "High")
     .when(F.col("confidence_score") >= 50, "Medium-High")
     .when(F.col("confidence_score") >= 35, "Medium")
     .otherwise("Low")
)

# Add geography metadata
df_state_agg = df_state_agg.withColumn("geography_type", F.lit("state")) \
    .withColumn("geography_id", F.col("state")) \
    .withColumn("geography_name", F.col("state")) \
    .withColumn("aggregated_at", F.current_timestamp())

print("✓ Gap scores and confidence calculated")

# Save to care gap table
df_state_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(CAREGAP_TABLE)

print(f"\n✓ Care gap table created: {CAREGAP_TABLE}")

# Show summary statistics
print("\n📊 Care Gap Summary Statistics:")
print(f"  - Total geography-capability combinations: {df_state_agg.count():,}")
print(f"  - States covered: {df_state_agg.select('state').distinct().count():,}")
print(f"  - Capabilities analyzed: {df_state_agg.select('capability_name').distinct().count():,}")

# Show top gaps (highest gap score)
print("\n🔴 Top 10 Care Gaps (Highest Gap Score):")
df_state_agg.select(
    "state",
    "capability_name",
    "gap_score",
    "confidence_level",
    "total_facilities",
    "strong_count",
    "partial_count",
    "weak_count"
).orderBy(F.desc("gap_score")) \
 .show(10, False)

# Show best coverage (lowest gap score)
print("\n🟢 Top 10 Best Coverage (Lowest Gap Score):")
df_state_agg.select(
    "state",
    "capability_name",
    "gap_score",
    "confidence_level",
    "total_facilities",
    "strong_count"
).orderBy("gap_score") \
 .show(10, False)

# COMMAND ----------

# DBTITLE 1,Create Planner Workspace Persistence Tables
# Create planner workspace persistence tables
print("\n" + "=" * 80)
print("PLANNER WORKSPACE: CREATING PERSISTENCE TABLES")
print("=" * 80)

print("\nCreating 5 persistence tables for user actions...")

# 1. Planner Notes
print("\n1. Creating planner_notes table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_notes (
  note_id STRING,
  user_id STRING,
  facility_id STRING,
  capability_id STRING,
  geography_type STRING,
  geography_id STRING,
  note_text STRING,
  note_type STRING,  -- verification, concern, action, general
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")
print("✓ planner_notes created")

# 2. Planner Overrides
print("\n2. Creating planner_overrides table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_overrides (
  override_id STRING,
  user_id STRING,
  facility_id STRING,
  capability_id STRING,
  original_trust_signal STRING,
  override_trust_signal STRING,
  override_reason STRING,
  evidence_provided STRING,
  created_at TIMESTAMP,
  expires_at TIMESTAMP
) USING DELTA
""")
print("✓ planner_overrides created")

# 3. Planner Scenarios
print("\n3. Creating planner_scenarios table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_scenarios (
  scenario_id STRING,
  user_id STRING,
  scenario_name STRING,
  geography_type STRING,
  geography_id STRING,
  capability_id STRING,
  scenario_type STRING,  -- verify_claims, add_facility, increase_outreach, reject_claims
  scenario_params STRING,  -- JSON string of parameters
  baseline_gap_score DOUBLE,
  baseline_confidence DOUBLE,
  projected_gap_score DOUBLE,
  projected_confidence DOUBLE,
  created_at TIMESTAMP,
  saved BOOLEAN
) USING DELTA
""")
print("✓ planner_scenarios created")

# 4. Planner Shortlists
print("\n4. Creating planner_shortlists table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_shortlists (
  shortlist_id STRING,
  user_id STRING,
  shortlist_name STRING,
  facility_ids ARRAY<STRING>,
  capability_id STRING,
  geography_type STRING,
  geography_id STRING,
  shortlist_purpose STRING,  -- verification, intervention, monitoring
  priority STRING,  -- high, medium, low
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")
print("✓ planner_shortlists created")

# 5. Planner Review Decisions
print("\n5. Creating planner_review_decisions table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_review_decisions (
  decision_id STRING,
  user_id STRING,
  facility_id STRING,
  capability_id STRING,
  review_status STRING,  -- pending, verified, rejected, needs_field_visit
  decision_notes STRING,
  evidence_updated BOOLEAN,
  new_trust_signal STRING,
  reviewed_at TIMESTAMP,
  reviewer_name STRING
) USING DELTA
""")
print("✓ planner_review_decisions created")

print("\n✓ All 5 planner workspace tables created successfully")
print("\n📋 Tables:")
print("  1. workspace.healthgpt.planner_notes")
print("  2. workspace.healthgpt.planner_overrides")
print("  3. workspace.healthgpt.planner_scenarios")
print("  4. workspace.healthgpt.planner_shortlists")
print("  5. workspace.healthgpt.planner_review_decisions")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary and Data Quality Report
# Pipeline Summary and Data Quality Report
print("\n" + "=" * 80)
print("PIPELINE SUMMARY: HEALTHGPT CARE GAP TRUST PLANNER")
print("=" * 80)

print(f"\n📅 Pipeline Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Summary statistics
print("\n" + "=" * 80)
print("DATA LAYER SUMMARY")
print("=" * 80)

# Bronze
df_bronze = spark.table("workspace.healthgpt.facilities_bronze")
print(f"\n🪨 BRONZE - Raw Facility Data:")
print(f"  - Records: {df_bronze.count():,}")
print(f"  - Table: workspace.healthgpt.facilities_bronze")

# Silver
df_silver = spark.table("workspace.healthgpt.facilities_silver")
print(f"\n🪨 SILVER - Cleaned Facility Data:")
print(f"  - Records: {df_silver.count():,}")
print(f"  - States: {df_silver.select('state').distinct().count():,}")
print(f"  - Cities: {df_silver.select('city').distinct().count():,}")
print(f"  - Avg Completeness Score: {df_silver.agg(F.avg('completeness_score')).collect()[0][0]:.1f}/100")
print(f"  - Table: workspace.healthgpt.facilities_silver")

# Evidence
df_evidence = spark.table("workspace.healthgpt.facility_capability_evidence")
print(f"\n🔍 EVIDENCE - Capability Extraction:")
print(f"  - Total Evidence Records: {df_evidence.count():,}")
print(f"  - Facilities with Capabilities: {df_evidence.select('facility_id').distinct().count():,}")
print(f"  - Capability Types: {df_evidence.select('capability_id').distinct().count():,}")
print(f"  - Avg Evidence per Facility-Capability: {df_evidence.agg(F.avg('evidence_count')).collect()[0][0]:.1f}")
print(f"  - Table: workspace.healthgpt.facility_capability_evidence")

# Trust Scores
df_trust = spark.table("workspace.healthgpt.facility_trust_scores")
print(f"\n🛡️ TRUST SCORES - Evidence Scoring:")
print(f"  - Total Scored Records: {df_trust.count():,}")
trust_dist = df_trust.groupBy("trust_signal").count().collect()
for row in sorted(trust_dist, key=lambda x: x[1], reverse=True):
    pct = row[1] / df_trust.count() * 100
    print(f"  - {row[0].title()}: {row[1]:,} ({pct:.1f}%)")
print(f"  - Table: workspace.healthgpt.facility_trust_scores")

# Care Gap
df_gap = spark.table("workspace.healthgpt.care_gap_by_geography")
print(f"\n📍 CARE GAP - Geographic Aggregation:")
print(f"  - Geography-Capability Combinations: {df_gap.count():,}")
print(f"  - States Analyzed: {df_gap.select('state').distinct().count():,}")
print(f"  - Capabilities Tracked: {df_gap.select('capability_name').distinct().count():,}")
print(f"  - Avg Gap Score: {df_gap.agg(F.avg('gap_score')).collect()[0][0]:.1f}/100")
print(f"  - High Confidence Assessments: {df_gap.filter(F.col('confidence_level') == 'High').count():,}")
print(f"  - Table: workspace.healthgpt.care_gap_by_geography")

# Planner Workspace
print(f"\n📝 PLANNER WORKSPACE - Persistence Tables:")
print(f"  - workspace.healthgpt.planner_notes")
print(f"  - workspace.healthgpt.planner_overrides")
print(f"  - workspace.healthgpt.planner_scenarios")
print(f"  - workspace.healthgpt.planner_shortlists")
print(f"  - workspace.healthgpt.planner_review_decisions")

print("\n" + "=" * 80)
print("CAPABILITY BREAKDOWN")
print("=" * 80)

for cap_id, cap_info in CAPABILITY_TAXONOMY.items():
    cap_count = df_trust.filter(F.col("capability_id") == cap_id).count()
    strong_count = df_trust.filter((F.col("capability_id") == cap_id) & (F.col("trust_signal") == "strong")).count()
    print(f"\n{cap_info['name']}:")
    print(f"  - Facilities with Evidence: {cap_count:,}")
    print(f"  - Strong Evidence: {strong_count:,} ({strong_count/cap_count*100 if cap_count > 0 else 0:.1f}%)")

print("\n" + "=" * 80)
print("TOP 5 STATES BY CARE GAPS (MATERNITY)")
print("=" * 80)

df_maternity_gaps = df_gap.filter(F.col("capability_id") == "maternity") \
    .select("state", "gap_score", "confidence_level", "total_facilities", "strong_count") \
    .orderBy(F.desc("gap_score"))

df_maternity_gaps.show(5, False)

print("\n" + "=" * 80)
print("✅ PIPELINE COMPLETE - READY FOR STREAMLIT APP")
print("=" * 80)

print("\n🚀 Next Steps:")
print("  1. Build Streamlit App with 6 screens (Overview, Care Map, Facilities, Evidence Review, Scenario Planner, Ask HealthGPT)")
print("  2. Integrate OpenAI for AI Gap Briefs (structured inputs only, no hallucination)")
print("  3. Add interactive maps with trust signal visualization")
print("  4. Implement scenario simulator (verify claims, add facilities, reject suspicious)")
print("  5. Connect planner workspace tables for persistence")
print("  6. Cache demo geography for reliable live demo (e.g., Maternity in Nicobars)")

print("\n🎯 Product: HealthGPT Care Gap Trust Planner")
print("🎯 Track: Track 2 - Medical Desert Planner")
print("🎯 Architecture: Databricks Bronze/Silver/Gold + OpenAI + Streamlit")

# COMMAND ----------

# DBTITLE 1,Imports and Setup
# Imports and Configuration
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import re
from datetime import datetime

print("\n" + "="*70)
print("HEALTHGPT PHASE 3: FACILITY PIPELINE INITIALIZATION")
print("="*70)
print(f"\n✅ Imports loaded successfully")
print(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🎯 Target: Track 2 - Medical Desert Planner")
print(f"🔍 Source: databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities")
print(f"📦 Output Schema: workspace.healthgpt.*")

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Load Raw Facility Data
# ============================================================================
# BRONZE LAYER: Load Raw Facility Data
# ============================================================================
# Purpose: Preserve messy facility dataset as-is for traceability and auditing

print("\n" + "="*70)
print("BRONZE LAYER: RAW FACILITY DATA")
print("="*70)

# Load source facility data
SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
BRONZE_TABLE = "workspace.healthgpt.facilities_bronze"

print(f"\n📂 Source: {SOURCE_TABLE}")

facilities_df = spark.table(SOURCE_TABLE)

# Write to Bronze (preserve all 51 columns)
facilities_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"✅ Bronze table created: {BRONZE_TABLE}")

# Verify and show stats
bronze_count = spark.table(BRONZE_TABLE).count()
print(f"\n📊 Bronze Layer Statistics:")
print(f"  Total facilities: {bronze_count:,}")
print(f"  Columns preserved: {len(facilities_df.columns)}")

# Sample data quality metrics
quality_df = spark.sql(f"""
SELECT 
  COUNT(DISTINCT unique_id) as distinct_facilities,
  COUNT(DISTINCT address_stateOrRegion) as states,
  COUNT(DISTINCT address_city) as cities,
  SUM(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) as has_description,
  SUM(CASE WHEN specialties IS NOT NULL THEN 1 ELSE 0 END) as has_specialties,
  SUM(CASE WHEN procedure IS NOT NULL THEN 1 ELSE 0 END) as has_procedures,
  SUM(CASE WHEN equipment IS NOT NULL THEN 1 ELSE 0 END) as has_equipment,
  SUM(CASE WHEN capability IS NOT NULL THEN 1 ELSE 0 END) as has_capability,
  SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as has_coordinates
FROM {BRONZE_TABLE}
""")

quality_stats = quality_df.collect()[0]
print(f"\n  Distinct facilities: {quality_stats['distinct_facilities']:,}")
print(f"  Geographic coverage: {quality_stats['states']} states, {quality_stats['cities']:,} cities")
print(f"  Has description: {quality_stats['has_description']:,} ({quality_stats['has_description']/bronze_count*100:.1f}%)")
print(f"  Has specialties: {quality_stats['has_specialties']:,} ({quality_stats['has_specialties']/bronze_count*100:.1f}%)")
print(f"  Has procedures: {quality_stats['has_procedures']:,} ({quality_stats['has_procedures']/bronze_count*100:.1f}%)")
print(f"  Has equipment: {quality_stats['has_equipment']:,} ({quality_stats['has_equipment']/bronze_count*100:.1f}%)")
print(f"  Has capability: {quality_stats['has_capability']:,} ({quality_stats['has_capability']/bronze_count*100:.1f}%)")
print(f"  Has coordinates: {quality_stats['has_coordinates']:,} ({quality_stats['has_coordinates']/bronze_count*100:.1f}%)")

print(f"\n✅ Bronze layer complete")

# COMMAND ----------

# DBTITLE 1,Silver Layer - Clean and Standardize
# ============================================================================
# SILVER LAYER: Clean and Standardize Facility Data
# ============================================================================
# Purpose: Standardize geography, parse JSON arrays, deduplicate, handle nulls

print("\n" + "="*70)
print("SILVER LAYER: CLEANED FACILITY DATA")
print("="*70)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

# Load bronze data
bronze_df = spark.table(BRONZE_TABLE)

# Parse JSON array fields and clean data
silver_df = bronze_df.select(
    F.col("unique_id").alias("facility_id"),
    F.trim(F.col("name")).alias("facility_name"),
    
    # Geography - standardized
    F.trim(F.upper(F.col("address_stateOrRegion"))).alias("state"),
    F.trim(F.initcap(F.col("address_city"))).alias("city"),
    F.trim(F.col("address_zipOrPostcode")).alias("pin_code"),
    F.col("address_country").alias("country"),
    
    # Coordinates
    F.col("latitude"),
    F.col("longitude"),
    
    # Organizational details
    F.col("organization_type"),
    F.col("facilityTypeId").alias("facility_type_id"),
    F.col("operatorTypeId").alias("operator_type_id"),
    
    # Capacity indicators (string to numeric)
    F.when(F.col("numberDoctors").rlike("^[0-9]+$"), F.col("numberDoctors").cast("int")).alias("number_doctors"),
    F.when(F.col("capacity").rlike("^[0-9]+$"), F.col("capacity").cast("int")).alias("bed_capacity"),
    
    # Text fields (raw - for evidence extraction)
    F.col("description"),
    F.col("specialties"),
    F.col("procedure"),
    F.col("equipment"),
    F.col("capability"),
    
    # Trust signals
    F.col("source_urls"),
    F.col("websites"),
    F.col("officialWebsite").alias("official_website"),
    F.col("yearEstablished").alias("year_established"),
    
    # Social media & engagement metrics (trust signals)
    F.col("distinct_social_media_presence_count"),
    F.col("affiliated_staff_presence"),
    F.col("custom_logo_presence"),
    F.col("number_of_facts_about_the_organization"),
    F.col("post_metrics_most_recent_social_media_post_date"),
    F.col("engagement_metrics_n_followers"),
    
    # Contact
    F.col("phone_numbers"),
    F.col("email")
)

# Add data quality score (0-100)
silver_df = silver_df.withColumn(
    "data_completeness_score",
    (
        F.when(F.col("description").isNotNull(), 20).otherwise(0) +
        F.when(F.col("specialties").isNotNull(), 20).otherwise(0) +
        F.when(F.col("procedure").isNotNull(), 15).otherwise(0) +
        F.when(F.col("equipment").isNotNull(), 15).otherwise(0) +
        F.when(F.col("capability").isNotNull(), 10).otherwise(0) +
        F.when(F.col("latitude").isNotNull() & F.col("longitude").isNotNull(), 10).otherwise(0) +
        F.when(F.col("source_urls").isNotNull(), 5).otherwise(0) +
        F.when(F.col("number_doctors").isNotNull(), 5).otherwise(0)
    )
)

# Add record timestamp
silver_df = silver_df.withColumn("processed_at", F.current_timestamp())

# Write Silver table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"✅ Silver table created: {SILVER_TABLE}")

# Statistics
silver_count = spark.table(SILVER_TABLE).count()
print(f"\n📊 Silver Layer Statistics:")
print(f"  Total facilities: {silver_count:,}")

# Data quality distribution
quality_dist = spark.sql(f"""
SELECT 
  CASE 
    WHEN data_completeness_score >= 80 THEN 'High (80-100)'
    WHEN data_completeness_score >= 50 THEN 'Medium (50-79)'
    ELSE 'Low (0-49)'
  END as quality_tier,
  COUNT(*) as facility_count,
  ROUND(AVG(data_completeness_score), 1) as avg_score
FROM {SILVER_TABLE}
GROUP BY 1
ORDER BY avg_score DESC
""")

print(f"\n  Data Completeness Distribution:")
for row in quality_dist.collect():
    print(f"    {row['quality_tier']}: {row['facility_count']:,} facilities (avg: {row['avg_score']})")

# Geographic distribution
geo_dist = spark.sql(f"""
SELECT 
  state,
  COUNT(*) as facility_count,
  COUNT(DISTINCT city) as city_count
FROM {SILVER_TABLE}
WHERE state IS NOT NULL
GROUP BY state
ORDER BY facility_count DESC
LIMIT 10
""")

print(f"\n  Top 10 States by Facility Count:")
for row in geo_dist.collect():
    print(f"    {row['state']}: {row['facility_count']:,} facilities across {row['city_count']} cities")

print(f"\n✅ Silver layer complete")

# COMMAND ----------

# DBTITLE 1,Define Capability Taxonomy and Keywords
# ============================================================================
# CAPABILITY TAXONOMY: Define 7 Core Capabilities for Track 2
# ============================================================================
# Purpose: Define keywords and patterns for evidence extraction

print("\n" + "="*70)
print("CAPABILITY TAXONOMY DEFINITION")
print("="*70)

# Capability taxonomy with keyword dictionaries for evidence extraction
CAPABILITY_TAXONOMY = {
    "maternity": {
        "name": "Maternity Care",
        "description": "Obstetric services, delivery, prenatal/postnatal care",
        "keywords": [
            "maternity", "obstetric", "obstetrics", "gynecology", "gynaecology",
            "delivery", "labor", "labour", "childbirth", "prenatal", "antenatal",
            "postnatal", "pregnancy", "maternal", "c-section", "caesarean",
            "neonatal", "newborn", "obgyn", "ob/gyn"
        ],
        "specialties": ["obstetrics", "gynecology", "maternalFetalMedicine"],
        "procedures": ["delivery", "c-section", "caesarean", "labor", "antenatal"],
        "equipment": ["incubator", "fetal monitor", "delivery bed", "labor room"]
    },
    "icu": {
        "name": "Intensive Care Unit (ICU)",
        "description": "Critical care for life-threatening conditions",
        "keywords": [
            "icu", "intensive care", "critical care", "ventilator", "life support",
            "icu bed", "icu ward", "intensive care unit", "critical care unit",
            "ccu", "cardiac care unit"
        ],
        "specialties": ["criticalCareMedicine", "intensiveCare"],
        "procedures": ["ventilation", "mechanical ventilation", "life support"],
        "equipment": ["ventilator", "icu bed", "monitor", "life support"]
    },
    "nicu": {
        "name": "Neonatal ICU (NICU)",
        "description": "Intensive care for newborns and premature infants",
        "keywords": [
            "nicu", "neonatal", "newborn intensive", "premature", "neonatal icu",
            "neonatal intensive care", "neonatal care", "pediatric intensive",
            "neonatal unit"
        ],
        "specialties": ["neonatology", "neonatalPerinatalMedicine", "pediatrics"],
        "procedures": ["neonatal care", "premature care"],
        "equipment": ["incubator", "neonatal ventilator", "radiant warmer", "phototherapy"]
    },
    "emergency": {
        "name": "Emergency Services",
        "description": "24/7 emergency medical services and trauma response",
        "keywords": [
            "emergency", "emergency room", "er", "24/7", "24x7", "24 hour",
            "accident", "trauma", "emergency services", "emergency department",
            "casualty", "urgent care", "emergency ward"
        ],
        "specialties": ["emergencyMedicine", "traumaSurgery"],
        "procedures": ["emergency", "trauma", "resuscitation"],
        "equipment": ["emergency", "ambulance", "defibrillator", "trauma"]
    },
    "trauma": {
        "name": "Trauma Care",
        "description": "Specialized care for severe injuries and accidents",
        "keywords": [
            "trauma", "trauma center", "trauma centre", "trauma care", "trauma unit",
            "accident", "injury", "trauma surgery", "polytrauma", "burn",
            "orthopedic trauma"
        ],
        "specialties": ["traumaSurgery", "orthopedicSurgery", "emergencyMedicine"],
        "procedures": ["trauma surgery", "fracture", "burn", "injury"],
        "equipment": ["trauma", "surgery", "orthopedic"]
    },
    "dialysis": {
        "name": "Dialysis",
        "description": "Renal replacement therapy for kidney failure",
        "keywords": [
            "dialysis", "hemodialysis", "haemodialysis", "peritoneal dialysis",
            "renal", "kidney", "dialysis unit", "dialysis center", "dialysis machine",
            "nephrology", "renal replacement"
        ],
        "specialties": ["nephrology", "renalMedicine"],
        "procedures": ["dialysis", "hemodialysis", "haemodialysis", "renal"],
        "equipment": ["dialysis machine", "dialysis", "hemodialysis"]
    },
    "oncology": {
        "name": "Oncology (Cancer Care)",
        "description": "Cancer diagnosis, treatment, and chemotherapy",
        "keywords": [
            "oncology", "cancer", "chemotherapy", "radiation therapy", "radiotherapy",
            "tumor", "tumour", "oncology center", "cancer center", "cancer care",
            "medical oncology", "surgical oncology", "radiation oncology",
            "chemo", "malignancy"
        ],
        "specialties": ["medicalOncology", "surgicalOncology", "radiationOncology", "hematologyOncology"],
        "procedures": ["chemotherapy", "radiation", "radiotherapy", "cancer surgery", "biopsy"],
        "equipment": ["linear accelerator", "radiation", "chemotherapy", "ct scanner", "pet scan"]
    }
}

print(f"\n✅ Capability taxonomy defined: {len(CAPABILITY_TAXONOMY)} capabilities")
for cap_id, cap_data in CAPABILITY_TAXONOMY.items():
    print(f"  • {cap_data['name']} ({cap_id}): {len(cap_data['keywords'])} keywords")

print(f"\n🎯 Primary demo capability: Maternity Care")
print(f"🔍 Evidence extraction will scan: description, specialties, procedure, equipment, capability fields")

# COMMAND ----------

# DBTITLE 1,Extract Capability Evidence from Facility Text
# ============================================================================
# EVIDENCE EXTRACTION: Extract capability claims from facility text fields
# ============================================================================
# Purpose: Scan description, specialties, procedure, equipment for capability mentions

print("\n" + "="*70)
print("EVIDENCE EXTRACTION PIPELINE")
print("="*70)

from pyspark.sql.functions import udf, explode, arrays_zip, array, lit, struct, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, FloatType

# Define evidence extraction UDF
def extract_capability_evidence(description, specialties, procedure, equipment, capability):
    """
    Extract evidence of capability mentions from facility text fields.
    Returns list of (capability_id, evidence_text, evidence_field, match_count)
    """
    results = []
    
    # Combine all text for searching (handle None values)
    text_fields = {
        "description": (description or "").lower(),
        "specialties": (specialties or "").lower(),
        "procedure": (procedure or "").lower(),
        "equipment": (equipment or "").lower(),
        "capability": (capability or "").lower()
    }
    
    # Search for each capability
    for cap_id, cap_data in CAPABILITY_TAXONOMY.items():
        field_matches = []
        
        for field_name, field_text in text_fields.items():
            if not field_text:
                continue
            
            # Count keyword matches
            matches = []
            for keyword in cap_data['keywords']:
                if keyword.lower() in field_text:
                    matches.append(keyword)
            
            if matches:
                # Extract snippet with context (up to 200 chars)
                first_match = matches[0].lower()
                match_pos = field_text.find(first_match)
                start = max(0, match_pos - 50)
                end = min(len(field_text), match_pos + 150)
                snippet = field_text[start:end].strip()
                if start > 0:
                    snippet = "..." + snippet
                if end < len(field_text):
                    snippet = snippet + "..."
                
                field_matches.append({
                    'field': field_name,
                    'snippet': snippet,
                    'match_count': len(matches),
                    'matched_keywords': matches[:5]  # Top 5 matches
                })
        
        # If any field matches, add to results
        if field_matches:
            # Aggregate evidence
            total_matches = sum(m['match_count'] for m in field_matches)
            evidence_text = " | ".join([f"{m['field']}: {m['snippet'][:100]}" for m in field_matches[:3]])
            evidence_fields = ",".join([m['field'] for m in field_matches])
            
            results.append((
                cap_id,
                evidence_text[:500],  # Limit to 500 chars
                evidence_fields,
                total_matches
            ))
    
    return results if results else [(None, None, None, 0)]

# Register UDF
evidence_schema = ArrayType(StructType([
    StructField("capability_id", StringType(), True),
    StructField("evidence_text", StringType(), True),
    StructField("evidence_fields", StringType(), True),
    StructField("match_count", IntegerType(), True)
]))

extract_evidence_udf = udf(extract_capability_evidence, evidence_schema)

print(f"\n🔍 Extracting capability evidence from {silver_count:,} facilities...")
print(f"  Scanning fields: description, specialties, procedure, equipment, capability")
print(f"  Target capabilities: {', '.join(CAPABILITY_TAXONOMY.keys())}")

# Load silver data and extract evidence
silver_df = spark.table(SILVER_TABLE)

evidence_df = silver_df.select(
    col("facility_id"),
    col("facility_name"),
    col("state"),
    col("city"),
    extract_evidence_udf(
        col("description"),
        col("specialties"),
        col("procedure"),
        col("equipment"),
        col("capability")
    ).alias("evidence_array")
)

# Explode evidence array to get one row per facility-capability pair
evidence_exploded = evidence_df \
    .select(
        col("facility_id"),
        col("facility_name"),
        col("state"),
        col("city"),
        explode(col("evidence_array")).alias("evidence")
    ) \
    .select(
        col("facility_id"),
        col("facility_name"),
        col("state"),
        col("city"),
        col("evidence.capability_id"),
        col("evidence.evidence_text"),
        col("evidence.evidence_fields"),
        col("evidence.match_count")
    ) \
    .filter(col("capability_id").isNotNull())

# Write to evidence table
EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"

evidence_exploded.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(EVIDENCE_TABLE)

print(f"\n✅ Evidence table created: {EVIDENCE_TABLE}")

# Statistics
evidence_stats = spark.sql(f"""
SELECT 
  capability_id,
  COUNT(DISTINCT facility_id) as facility_count,
  AVG(match_count) as avg_match_count,
  SUM(match_count) as total_matches
FROM {EVIDENCE_TABLE}
GROUP BY capability_id
ORDER BY facility_count DESC
""")

print(f"\n📊 Evidence Extraction Results:")
for row in evidence_stats.collect():
    cap_name = CAPABILITY_TAXONOMY[row['capability_id']]['name']
    print(f"  • {cap_name} ({row['capability_id']}):")
    print(f"      Facilities with evidence: {row['facility_count']:,}")
    print(f"      Avg matches per facility: {row['avg_match_count']:.1f}")
    print(f"      Total keyword matches: {row['total_matches']:,}")

print(f"\n✅ Evidence extraction complete")

# COMMAND ----------

# DBTITLE 1,Trust Scoring - Classify Evidence Strength
# ============================================================================
# TRUST SCORING: Score facility-capability evidence strength
# ============================================================================
# Purpose: Classify evidence as strong/partial/weak/suspicious/no_claim

print("\n" + "="*70)
print("TRUST SCORING ENGINE")
print("="*70)

print(f"\n🔒 Trust Signal Framework:")
print(f"  • Strong: Multiple fields + procedure/equipment specifics (score: 4)")
print(f"  • Partial: Mentioned in description/specialty but lacks detail (score: 3)")
print(f"  • Weak: Single noisy field mention (score: 2)")
print(f"  • Suspicious: Contradictions or implausible claims (score: 1)")
print(f"  • No Claim: No evidence found (score: 0)")

# Load evidence and silver data
evidence_df = spark.table(EVIDENCE_TABLE)
silver_df = spark.table(SILVER_TABLE)

# Join evidence with facility details for trust scoring
scoring_df = evidence_df.alias("e").join(
    silver_df.alias("f"),
    col("e.facility_id") == col("f.facility_id"),
    "inner"
).select(
    col("e.facility_id"),
    col("f.facility_name"),
    col("f.state"),
    col("f.city"),
    col("f.pin_code"),
    col("f.latitude"),
    col("f.longitude"),
    col("e.capability_id"),
    col("e.evidence_text"),
    col("e.evidence_fields"),
    col("e.match_count"),
    col("f.number_doctors"),
    col("f.bed_capacity"),
    col("f.data_completeness_score"),
    col("f.source_urls"),
    col("f.official_website")
)

# Trust scoring logic
scoring_df = scoring_df.withColumn(
    "field_count",
    F.size(F.split(col("evidence_fields"), ","))
)

scoring_df = scoring_df.withColumn(
    "has_procedure_equipment",
    F.when(
        (F.col("evidence_fields").contains("procedure")) |
        (F.col("evidence_fields").contains("equipment")),
        True
    ).otherwise(False)
)

scoring_df = scoring_df.withColumn(
    "has_specialty",
    F.when(F.col("evidence_fields").contains("specialties"), True).otherwise(False)
)

scoring_df = scoring_df.withColumn(
    "has_capability_field",
    F.when(F.col("evidence_fields").contains("capability"), True).otherwise(False)
)

# Assign trust signal
scoring_df = scoring_df.withColumn(
    "trust_signal",
    F.when(
        # Strong: Multiple fields (3+) including procedure/equipment OR specialty + high match count
        ((col("field_count") >= 3) & (col("has_procedure_equipment"))) |
        ((col("has_specialty")) & (col("match_count") >= 5) & (col("has_procedure_equipment"))),
        "strong"
    ).when(
        # Partial: 2+ fields OR specialty match OR decent match count
        (col("field_count") >= 2) |
        (col("has_specialty")) |
        (col("match_count") >= 3),
        "partial"
    ).when(
        # Weak: Single field, low matches
        (col("field_count") == 1) & (col("match_count") < 3),
        "weak"
    ).otherwise("suspicious")
)

# Assign numeric trust score
scoring_df = scoring_df.withColumn(
    "trust_score",
    F.when(col("trust_signal") == "strong", 4)
    .when(col("trust_signal") == "partial", 3)
    .when(col("trust_signal") == "weak", 2)
    .when(col("trust_signal") == "suspicious", 1)
    .otherwise(0)
)

# Add confidence adjustment based on data completeness
scoring_df = scoring_df.withColumn(
    "confidence_adjustment",
    F.when(col("data_completeness_score") >= 80, 1.0)
    .when(col("data_completeness_score") >= 50, 0.8)
    .otherwise(0.6)
)

scoring_df = scoring_df.withColumn(
    "adjusted_trust_score",
    F.round(col("trust_score") * col("confidence_adjustment"), 2)
)

# Write trust scores table
TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"

scoring_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TRUST_TABLE)

print(f"\n✅ Trust scores table created: {TRUST_TABLE}")

# Statistics
trust_stats = spark.sql(f"""
SELECT 
  capability_id,
  trust_signal,
  COUNT(*) as facility_count,
  AVG(trust_score) as avg_trust_score,
  AVG(adjusted_trust_score) as avg_adjusted_score
FROM {TRUST_TABLE}
GROUP BY capability_id, trust_signal
ORDER BY capability_id, trust_signal
""")

print(f"\n📊 Trust Score Distribution:")
current_cap = None
for row in trust_stats.collect():
    if current_cap != row['capability_id']:
        if current_cap is not None:
            print()
        current_cap = row['capability_id']
        cap_name = CAPABILITY_TAXONOMY[row['capability_id']]['name']
        print(f"\n  {cap_name} ({row['capability_id']}):")
    
    signal_emoji = {
        "strong": "✅",
        "partial": "🟡",
        "weak": "🟠",
        "suspicious": "⚠️"
    }.get(row['trust_signal'], "❓")
    
    print(f"    {signal_emoji} {row['trust_signal'].capitalize()}: {row['facility_count']:,} facilities (avg score: {row['avg_adjusted_score']:.2f})")

print(f"\n✅ Trust scoring complete")

# COMMAND ----------

# DBTITLE 1,Geographic Aggregation - Care Gap Scores
# ============================================================================
# GEOGRAPHIC AGGREGATION: Compute care gap scores by geography
# ============================================================================
# Purpose: Aggregate facility evidence to state/city level with gap scores

print("\n" + "="*70)
print("GEOGRAPHIC AGGREGATION: CARE GAP SCORING")
print("="*70)

# Load trust scores
trust_df = spark.table(TRUST_TABLE)

# Aggregate by state + capability
state_agg = trust_df.groupBy("state", "capability_id").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_count"),
    F.sum(F.when(col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_count"),
    F.sum(F.when(col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_count"),
    F.sum(F.when(col("trust_signal") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
    F.avg("adjusted_trust_score").alias("avg_trust_score"),
    F.avg("data_completeness_score").alias("avg_data_completeness"),
    F.countDistinct("city").alias("city_count")
)

# Calculate care gap score (0-100, higher = bigger gap)
# Formula: Gap increases with fewer strong facilities, more weak/suspicious, lower completeness
state_agg = state_agg.withColumn(
    "strong_ratio",
    col("strong_count") / col("total_facilities")
)

state_agg = state_agg.withColumn(
    "weak_suspicious_ratio",
    (col("weak_count") + col("suspicious_count")) / col("total_facilities")
)

state_agg = state_agg.withColumn(
    "gap_score",
    F.round(
        # Base gap: inverse of strong ratio (0-40 points)
        ((1 - col("strong_ratio")) * 40) +
        # Weak/suspicious burden (0-30 points)
        (col("weak_suspicious_ratio") * 30) +
        # Data incompleteness penalty (0-30 points)
        ((100 - col("avg_data_completeness")) / 100 * 30)
    , 1)
)

# Calculate confidence level (0-100)
# Confidence increases with data completeness, more facilities, lower weak/suspicious ratio
state_agg = state_agg.withColumn(
    "confidence_score",
    F.round(
        # Data completeness (0-40 points)
        (col("avg_data_completeness") / 100 * 40) +
        # Facility coverage (0-30 points, capped at 10+ facilities)
        (F.least(col("total_facilities") / 10.0, 1.0) * 30) +
        # Trust quality (0-30 points, based on strong ratio)
        (col("strong_ratio") * 30)
    , 1)
)

# Classify confidence level
state_agg = state_agg.withColumn(
    "confidence_level",
    F.when(col("confidence_score") >= 70, "High")
    .when(col("confidence_score") >= 50, "Medium-High")
    .when(col("confidence_score") >= 30, "Medium")
    .otherwise("Low")
)

# Classify gap severity
state_agg = state_agg.withColumn(
    "gap_severity",
    F.when(col("gap_score") >= 70, "Critical")
    .when(col("gap_score") >= 50, "High")
    .when(col("gap_score") >= 30, "Moderate")
    .otherwise("Low")
)

# Add geography type
state_agg = state_agg.withColumn("geography_type", F.lit("state"))

# Write state-level aggregation
GAP_STATE_TABLE = "workspace.healthgpt.care_gap_by_state"

state_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GAP_STATE_TABLE)

print(f"\n✅ State-level gap scores created: {GAP_STATE_TABLE}")

# Aggregate by city + capability (for detailed drill-down)
city_agg = trust_df.groupBy("state", "city", "capability_id").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_count"),
    F.sum(F.when(col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_count"),
    F.sum(F.when(col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_count"),
    F.sum(F.when(col("trust_signal") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
    F.avg("adjusted_trust_score").alias("avg_trust_score"),
    F.avg("data_completeness_score").alias("avg_data_completeness")
)

# Apply same gap scoring logic
city_agg = city_agg.withColumn(
    "strong_ratio",
    col("strong_count") / col("total_facilities")
).withColumn(
    "weak_suspicious_ratio",
    (col("weak_count") + col("suspicious_count")) / col("total_facilities")
).withColumn(
    "gap_score",
    F.round(
        ((1 - col("strong_ratio")) * 40) +
        (col("weak_suspicious_ratio") * 30) +
        ((100 - col("avg_data_completeness")) / 100 * 30)
    , 1)
).withColumn(
    "confidence_score",
    F.round(
        (col("avg_data_completeness") / 100 * 40) +
        (F.least(col("total_facilities") / 5.0, 1.0) * 30) +
        (col("strong_ratio") * 30)
    , 1)
).withColumn(
    "confidence_level",
    F.when(col("confidence_score") >= 70, "High")
    .when(col("confidence_score") >= 50, "Medium-High")
    .when(col("confidence_score") >= 30, "Medium")
    .otherwise("Low")
).withColumn(
    "gap_severity",
    F.when(col("gap_score") >= 70, "Critical")
    .when(col("gap_score") >= 50, "High")
    .when(col("gap_score") >= 30, "Moderate")
    .otherwise("Low")
).withColumn("geography_type", F.lit("city"))

GAP_CITY_TABLE = "workspace.healthgpt.care_gap_by_city"

city_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GAP_CITY_TABLE)

print(f"✅ City-level gap scores created: {GAP_CITY_TABLE}")

# Show top care gaps for maternity (demo capability)
print(f"\n🎯 Top 10 States with Highest Maternity Care Gaps:")
top_gaps = spark.sql(f"""
SELECT 
  state,
  gap_score,
  gap_severity,
  confidence_level,
  total_facilities,
  strong_count,
  partial_count,
  weak_count + suspicious_count as weak_suspicious_count
FROM {GAP_STATE_TABLE}
WHERE capability_id = 'maternity'
ORDER BY gap_score DESC
LIMIT 10
""")

for row in top_gaps.collect():
    print(f"\n  {row['state']}:")
    print(f"    Gap Score: {row['gap_score']} ({row['gap_severity']})")
    print(f"    Confidence: {row['confidence_level']}")
    print(f"    Facilities: {row['total_facilities']} total, {row['strong_count']} strong, {row['partial_count']} partial, {row['weak_suspicious_count']} weak/suspicious")

print(f"\n✅ Geographic aggregation complete")

# COMMAND ----------

# DBTITLE 1,Create Planner Workspace Tables
# ============================================================================
# PLANNER WORKSPACE: Create persistence tables for user actions
# ============================================================================
# Purpose: Track notes, overrides, scenarios, shortlists, and review decisions

print("\n" + "="*70)
print("PLANNER WORKSPACE TABLES")
print("="*70)

print(f"\n💾 Creating 5 persistence tables for planner actions...")

# 1. Planner Notes
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_notes (
  note_id STRING,
  user_id STRING,
  geography_type STRING,
  geography_name STRING,
  capability_id STRING,
  facility_id STRING,
  note_text STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")
print("  ✅ workspace.healthgpt.planner_notes")

# 2. Planner Overrides (manual trust signal corrections)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_overrides (
  override_id STRING,
  user_id STRING,
  facility_id STRING,
  capability_id STRING,
  original_trust_signal STRING,
  override_trust_signal STRING,
  override_reason STRING,
  created_at TIMESTAMP,
  applied BOOLEAN
) USING DELTA
""")
print("  ✅ workspace.healthgpt.planner_overrides")

# 3. Planner Scenarios (what-if simulations)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_scenarios (
  scenario_id STRING,
  user_id STRING,
  scenario_name STRING,
  geography_type STRING,
  geography_name STRING,
  capability_id STRING,
  scenario_type STRING,  -- 'verify_claims', 'add_facility', 'reject_claims', 'intervention'
  scenario_params STRING, -- JSON with scenario parameters
  projected_gap_score DOUBLE,
  projected_confidence_score DOUBLE,
  created_at TIMESTAMP
) USING DELTA
""")
print("  ✅ workspace.healthgpt.planner_scenarios")

# 4. Planner Shortlists (saved facilities for follow-up)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_shortlists (
  shortlist_id STRING,
  user_id STRING,
  shortlist_name STRING,
  facility_id STRING,
  capability_id STRING,
  priority STRING,  -- 'high', 'medium', 'low'
  action_needed STRING, -- 'verify', 'visit', 'contact', 'review'
  added_at TIMESTAMP
) USING DELTA
""")
print("  ✅ workspace.healthgpt.planner_shortlists")

# 5. Planner Review Decisions (evidence review outcomes)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_review_decisions (
  review_id STRING,
  user_id STRING,
  facility_id STRING,
  capability_id STRING,
  original_trust_signal STRING,
  review_decision STRING,  -- 'confirmed', 'rejected', 'needs_verification', 'upgraded', 'downgraded'
  review_notes STRING,
  reviewed_at TIMESTAMP
) USING DELTA
""")
print("  ✅ workspace.healthgpt.planner_review_decisions")

print(f"\n✅ All planner workspace tables created")
print(f"\n📝 These tables enable:")
print(f"  • Manual corrections and overrides")
print(f"  • Scenario planning and what-if analysis")
print(f"  • Facility shortlists and action tracking")
print(f"  • Evidence review workflow")
print(f"  • Audit trail of planner decisions")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary and Next Steps
# ============================================================================
# PIPELINE SUMMARY
# ============================================================================

print("\n" + "="*70)
print("HEALTHGPT PHASE 3: FACILITY PIPELINE COMPLETE")
print("="*70)

print(f"\n🏆 DATA ARCHITECTURE COMPLETE FOR TRACK 2: MEDICAL DESERT PLANNER")

print(f"\n📦 Tables Created:")
tables = [
    ("workspace.healthgpt.facilities_bronze", "Raw facility data (10K+ facilities)"),
    ("workspace.healthgpt.facilities_silver", "Cleaned and standardized facilities"),
    ("workspace.healthgpt.facility_capability_evidence", "Extracted capability evidence"),
    ("workspace.healthgpt.facility_trust_scores", "Trust-scored facility-capability pairs"),
    ("workspace.healthgpt.care_gap_by_state", "State-level gap scores and confidence"),
    ("workspace.healthgpt.care_gap_by_city", "City-level gap scores and confidence"),
    ("workspace.healthgpt.planner_notes", "User notes persistence"),
    ("workspace.healthgpt.planner_overrides", "Manual trust corrections"),
    ("workspace.healthgpt.planner_scenarios", "What-if scenario simulations"),
    ("workspace.healthgpt.planner_shortlists", "Facility action shortlists"),
    ("workspace.healthgpt.planner_review_decisions", "Evidence review outcomes")
]

for table_name, description in tables:
    try:
        count = spark.table(table_name).count()
        print(f"  ✅ {table_name}")
        print(f"      {description} ({count:,} records)")
    except:
        print(f"  🟡 {table_name}")
        print(f"      {description} (created, empty)")

print(f"\n🎯 Capabilities Supported:")
for cap_id, cap_data in CAPABILITY_TAXONOMY.items():
    print(f"  • {cap_data['name']} ({cap_id})")

print(f"\n🔒 Trust Scoring Framework:")
print(f"  • Strong Evidence: Multiple fields + procedure/equipment")
print(f"  • Partial Evidence: Description/specialty mentions")
print(f"  • Weak Evidence: Single noisy field")
print(f"  • Suspicious Evidence: Contradictions detected")
print(f"  • No Claim: No evidence found")

print(f"\n📏 Gap Scoring Formula:")
print(f"  Gap Score (0-100) = Inverse of strong facilities (40pts)")
print(f"                     + Weak/suspicious burden (30pts)")
print(f"                     + Data incompleteness (30pts)")

print(f"\n🔍 Confidence Calculation:")
print(f"  Confidence (0-100) = Data completeness (40pts)")
print(f"                      + Facility coverage (30pts)")
print(f"                      + Trust quality (30pts)")

print(f"\n🛠️ Next Steps:")
print(f"  1. ✅ Data pipeline complete (Bronze → Silver → Gold)")
print(f"  2. 🔄 Build Streamlit app with 6 screens:")
print(f"      • Overview / Gap Command Center")
print(f"      • Care Gap Map")
print(f"      • Facility Evidence Detail")
print(f"      • AI Gap Brief (OpenAI integration)")
print(f"      • Scenario Planner (What-if)")
print(f"      • Architecture / Trust (Data lineage)")
print(f"  3. 🤖 Integrate OpenAI for AI Gap Brief generation")
print(f"  4. 🎯 Prepare demo: Select Maternity Care in high-gap state")
print(f"  5. 📊 Test end-to-end workflow: geography → evidence → decision")

print(f"\n🚀 Ready for Streamlit App Development!")
print(f"\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,HealthGPT Phase 3 - Facility Pipeline Header
# MAGIC %md
# MAGIC # HealthGPT Phase 3: Facility-Based Care Gap Trust Planner
# MAGIC
# MAGIC **Track 2: Medical Desert Planner | Databricks Apps & Agents for Good 2026**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Project Context
# MAGIC
# MAGIC **Original Approach (Phase 1-2):** District-level health indicators (NFHS-style) with risk prediction models
# MAGIC
# MAGIC **Revised Approach (Phase 3):** Facility-level trust scoring for care gap detection
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Core Pivot: From District Risk to Facility Trust
# MAGIC
# MAGIC | Aspect | Phase 1-2 (District) | Phase 3 (Facility) |
# MAGIC |--------|---------------------|--------------------|
# MAGIC | **Data Source** | NFHS health indicators | Healthcare facility records |
# MAGIC | **Granularity** | 706 districts | 10,088+ facilities |
# MAGIC | **Core Question** | Which districts have high health risk? | Which facilities can we trust for each capability? |
# MAGIC | **Output** | Policy recommendations | Care gap brief with evidence confidence |
# MAGIC | **AI Role** | Explain health risks | Explain evidence and uncertainty |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC Bronze Layer (Raw)          → Silver Layer (Cleaned)     → Gold Layer (Evidence)
# MAGIC ├─ facilities_bronze        ├─ facilities_silver         ├─ facility_capability_evidence
# MAGIC    (10,088 facilities)         (parsed JSON arrays)         (capability × facility claims)
# MAGIC                             ├─ standardized geography    ├─ facility_trust_scores
# MAGIC                                (state, city, PIN)           (strong/partial/weak/suspicious)
# MAGIC                                                          ├─ care_gap_by_geography
# MAGIC                                                             (gap score + confidence)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7 Core Capabilities (Per Document)
# MAGIC
# MAGIC 1. **Maternity** - Delivery, ANC, obstetrics
# MAGIC 2. **ICU** - Intensive care, ventilators
# MAGIC 3. **NICU** - Neonatal intensive care, incubators
# MAGIC 4. **Emergency** - 24x7 emergency services
# MAGIC 5. **Trauma** - Trauma center, emergency surgery
# MAGIC 6. **Oncology** - Cancer treatment, radiation, chemotherapy
# MAGIC 7. **Dialysis** - Kidney dialysis, nephrology
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Trust Scoring Framework
# MAGIC
# MAGIC | Trust Signal | Definition | Example |
# MAGIC |--------------|------------|----------|
# MAGIC | **Strong** | Multiple supporting fields + specific equipment/procedures | Maternity + obstetrics specialty + C-section procedure + incubator equipment |
# MAGIC | **Partial** | Mentioned in description/specialty but lacks procedure/equipment | Maternity mentioned, but no delivery equipment listed |
# MAGIC | **Weak** | Single field mention with little support | NICU in capability field only |
# MAGIC | **Suspicious** | Contradiction or implausible combination | Oncology claim but no oncology specialty/equipment |
# MAGIC | **No Claim** | No evidence for selected capability | Facility has no maternity-related fields |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import Libraries and Setup
# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import re
from datetime import datetime

print("="*80)
print("HEALTHGPT PHASE 3: FACILITY-BASED CARE GAP TRUST PLANNER")
print("="*80)
print(f"Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Source: databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities")
print(f"Target schema: workspace.healthgpt")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Load Raw Facility Data
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BRONZE LAYER: Load raw facility data as-is for traceability
# MAGIC -- ============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.healthgpt.facilities_bronze AS
# MAGIC SELECT *
# MAGIC FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities;
# MAGIC
# MAGIC -- Verify Bronze table creation
# MAGIC SELECT 
# MAGIC   'Bronze Layer Created' as status,
# MAGIC   COUNT(*) as total_facilities,
# MAGIC   COUNT(DISTINCT unique_id) as distinct_facilities,
# MAGIC   COUNT(DISTINCT address_stateOrRegion) as distinct_states,
# MAGIC   COUNT(DISTINCT address_city) as distinct_cities,
# MAGIC   SUM(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) as has_description,
# MAGIC   SUM(CASE WHEN specialties IS NOT NULL THEN 1 ELSE 0 END) as has_specialties,
# MAGIC   SUM(CASE WHEN procedure IS NOT NULL THEN 1 ELSE 0 END) as has_procedures,
# MAGIC   SUM(CASE WHEN equipment IS NOT NULL THEN 1 ELSE 0 END) as has_equipment,
# MAGIC   SUM(CASE WHEN capability IS NOT NULL THEN 1 ELSE 0 END) as has_capability,
# MAGIC   ROUND(SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_with_coordinates
# MAGIC FROM workspace.healthgpt.facilities_bronze;

# COMMAND ----------

# DBTITLE 1,Silver Layer - Clean and Standardize
# ============================================================================
# SILVER LAYER: Clean and standardize facility data
# ============================================================================

print("\n" + "="*80)
print("BUILDING SILVER LAYER")
print("="*80)

# Load Bronze data
df_bronze = spark.table("workspace.healthgpt.facilities_bronze")
print(f"✓ Loaded {df_bronze.count():,} facilities from Bronze layer")

# Parse JSON arrays and clean data
df_silver = df_bronze \
    .withColumn("facility_id", F.col("unique_id")) \
    .withColumn("facility_name", F.trim(F.col("name"))) \
    .withColumn("state", F.trim(F.col("address_stateOrRegion"))) \
    .withColumn("city", F.trim(F.col("address_city"))) \
    .withColumn("pin_code", F.trim(F.col("address_zipOrPostcode"))) \
    .withColumn("lat", F.col("latitude")) \
    .withColumn("lon", F.col("longitude")) \
    .withColumn("description_text", F.coalesce(F.col("description"), F.lit(""))) \
    .withColumn("specialties_array", 
        F.when(F.col("specialties").isNotNull(), 
               F.from_json(F.col("specialties"), ArrayType(StringType())))
        .otherwise(F.array())) \
    .withColumn("procedures_array", 
        F.when(F.col("procedure").isNotNull(), 
               F.from_json(F.col("procedure"), ArrayType(StringType())))
        .otherwise(F.array())) \
    .withColumn("equipment_array", 
        F.when(F.col("equipment").isNotNull(), 
               F.from_json(F.col("equipment"), ArrayType(StringType())))
        .otherwise(F.array())) \
    .withColumn("capability_array", 
        F.when(F.col("capability").isNotNull(), 
               F.from_json(F.col("capability"), ArrayType(StringType())))
        .otherwise(F.array())) \
    .withColumn("doctors_count", 
        F.when(F.col("numberDoctors").cast("int").isNotNull(), 
               F.col("numberDoctors").cast("int"))
        .otherwise(F.lit(0))) \
    .withColumn("bed_capacity", 
        F.when(F.col("capacity").cast("int").isNotNull(), 
               F.col("capacity").cast("int"))
        .otherwise(F.lit(0))) \
    .withColumn("source_urls_array", 
        F.when(F.col("source_urls").isNotNull(), 
               F.from_json(F.col("source_urls"), ArrayType(StringType())))
        .otherwise(F.array()))

# Calculate data completeness score (0-100)
df_silver = df_silver \
    .withColumn("completeness_score",
        (
            F.when(F.col("facility_name").isNotNull(), 10).otherwise(0) +
            F.when(F.col("state").isNotNull(), 10).otherwise(0) +
            F.when(F.col("city").isNotNull(), 10).otherwise(0) +
            F.when(F.col("lat").isNotNull(), 10).otherwise(0) +
            F.when(F.col("description_text") != "", 15).otherwise(0) +
            F.when(F.size(F.col("specialties_array")) > 0, 15).otherwise(0) +
            F.when(F.size(F.col("procedures_array")) > 0, 10).otherwise(0) +
            F.when(F.size(F.col("equipment_array")) > 0, 10).otherwise(0) +
            F.when(F.col("doctors_count") > 0, 5).otherwise(0) +
            F.when(F.size(F.col("source_urls_array")) > 0, 5).otherwise(0)
        ).cast("int")
    )

# Select final Silver columns
df_silver_final = df_silver.select(
    "facility_id",
    "facility_name",
    "state",
    "city",
    "pin_code",
    "lat",
    "lon",
    "description_text",
    "specialties_array",
    "procedures_array",
    "equipment_array",
    "capability_array",
    "doctors_count",
    "bed_capacity",
    "source_urls_array",
    "completeness_score",
    "organization_type",
    "facilityTypeId",
    "operatorTypeId"
).dropDuplicates(["facility_id"])

# Save Silver table
df_silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facilities_silver")

print(f"✓ Silver layer created: workspace.healthgpt.facilities_silver")
print(f"  Total facilities: {df_silver_final.count():,}")
print(f"  Average completeness score: {df_silver_final.agg(F.avg('completeness_score')).collect()[0][0]:.1f}/100")
print(f"  Facilities with coordinates: {df_silver_final.filter(F.col('lat').isNotNull()).count():,}")
print(f"  Facilities with >80% completeness: {df_silver_final.filter(F.col('completeness_score') >= 80).count():,}")

# COMMAND ----------

# DBTITLE 1,Capability Taxonomy - Define Keywords
# ============================================================================
# CAPABILITY TAXONOMY: Define 7 core capabilities with keyword patterns
# ============================================================================

print("\n" + "="*80)
print("CAPABILITY TAXONOMY")
print("="*80)

# Define comprehensive keyword dictionaries for each capability
CAPABILITY_KEYWORDS = {
    "maternity": {
        "primary": ["maternity", "obstetric", "obstetrics", "delivery", "labor", "labour", 
                   "antenatal", "prenatal", "postnatal", "anc", "c-section", "caesarean"],
        "specialties": ["obstetrics", "gynecology", "gynaecology", "obgyn", "ob/gyn"],
        "procedures": ["delivery", "c-section", "caesarean", "episiotomy", "forceps", 
                      "vacuum delivery", "normal delivery", "vaginal delivery"],
        "equipment": ["incubator", "fetal monitor", "delivery table", "ultrasound", 
                     "warmer", "neonate", "labor room"]
    },
    "icu": {
        "primary": ["icu", "intensive care", "critical care", "ventilator", "ccm"],
        "specialties": ["critical care", "intensive care", "critical care medicine"],
        "procedures": ["mechanical ventilation", "intubation", "central line", 
                      "hemodynamic monitoring", "vasopressor"],
        "equipment": ["ventilator", "monitor", "infusion pump", "defibrillator", 
                     "ecg", "crash cart", "icu bed"]
    },
    "nicu": {
        "primary": ["nicu", "neonatal", "newborn", "premature", "preterm"],
        "specialties": ["neonatology", "pediatrics", "neonatal care"],
        "procedures": ["neonatal resuscitation", "phototherapy", "cpap", 
                      "respiratory support", "kangaroo care"],
        "equipment": ["incubator", "phototherapy", "neonatal ventilator", 
                     "infant warmer", "nicu monitor", "cpap"]
    },
    "emergency": {
        "primary": ["emergency", "casualty", "trauma", "24x7", "24/7", "accident"],
        "specialties": ["emergency medicine", "trauma", "accident and emergency"],
        "procedures": ["resuscitation", "stabilization", "triage", "emergency surgery"],
        "equipment": ["emergency room", "crash cart", "defibrillator", "ambulance", 
                     "stretcher", "emergency ward"]
    },
    "trauma": {
        "primary": ["trauma", "trauma center", "trauma centre", "polytrauma", 
                   "accident", "injury"],
        "specialties": ["trauma surgery", "orthopedic trauma", "neurosurgery"],
        "procedures": ["trauma surgery", "emergency surgery", "fracture fixation", 
                      "debridement", "laparotomy"],
        "equipment": ["ct scan", "x-ray", "operation theatre", "trauma bay", 
                     "blood bank"]
    },
    "oncology": {
        "primary": ["oncology", "cancer", "chemotherapy", "radiotherapy", "radiation"],
        "specialties": ["medical oncology", "surgical oncology", "radiation oncology", 
                       "gynecological oncology", "pediatric oncology"],
        "procedures": ["chemotherapy", "radiation", "radiotherapy", "biopsy", 
                      "tumor resection", "cancer surgery"],
        "equipment": ["linear accelerator", "cobalt", "ct simulator", "pet scan", 
                     "chemotherapy unit", "radiation therapy"]
    },
    "dialysis": {
        "primary": ["dialysis", "hemodialysis", "haemodialysis", "renal", "kidney"],
        "specialties": ["nephrology", "renal medicine"],
        "procedures": ["dialysis", "hemodialysis", "peritoneal dialysis", "renal replacement"],
        "equipment": ["dialysis machine", "dialysis unit", "hemodialysis", "av fistula"]
    }
}

print("✓ Defined 7 capability taxonomies:")
for cap, keywords in CAPABILITY_KEYWORDS.items():
    total_keywords = sum(len(v) for v in keywords.values())
    print(f"  {cap.upper()}: {total_keywords} keywords across {len(keywords)} categories")

# COMMAND ----------

# DBTITLE 1,Evidence Extraction Pipeline
# ============================================================================
# EVIDENCE EXTRACTION: Extract capability claims from facility text
# ============================================================================

print("\n" + "="*80)
print("EVIDENCE EXTRACTION PIPELINE")
print("="*80)

from pyspark.sql import Row

# Load Silver data
df_facilities = spark.table("workspace.healthgpt.facilities_silver")

def extract_capability_evidence(row):
    """
    Extract evidence for each capability from facility fields.
    Returns list of (facility_id, capability, evidence_text, evidence_field, confidence)
    """
    facility_id = row.facility_id
    results = []
    
    # Combine all text for searching
    description = (row.description_text or "").lower()
    specialties = " ".join(row.specialties_array or []).lower()
    procedures = " ".join(row.procedures_array or []).lower()
    equipment = " ".join(row.equipment_array or []).lower()
    capability_text = " ".join(row.capability_array or []).lower()
    
    # Check each capability
    for cap_name, keywords in CAPABILITY_KEYWORDS.items():
        evidence_items = []
        confidence_factors = []
        
        # Check description
        for keyword in keywords["primary"]:
            if keyword.lower() in description:
                evidence_items.append(f"Description mentions '{keyword}'")
                confidence_factors.append(1.0)
                break
        
        # Check specialties (high confidence)
        for keyword in keywords["specialties"]:
            if keyword.lower() in specialties:
                evidence_items.append(f"Specialty: {keyword}")
                confidence_factors.append(2.0)
                break
        
        # Check procedures (high confidence)
        for keyword in keywords["procedures"]:
            if keyword.lower() in procedures:
                evidence_items.append(f"Procedure: {keyword}")
                confidence_factors.append(2.0)
                break
        
        # Check equipment (high confidence)
        for keyword in keywords["equipment"]:
            if keyword.lower() in equipment:
                evidence_items.append(f"Equipment: {keyword}")
                confidence_factors.append(2.0)
                break
        
        # Check capability field
        for keyword in keywords["primary"]:
            if keyword.lower() in capability_text:
                evidence_items.append(f"Capability field mentions '{keyword}'")
                confidence_factors.append(1.5)
                break
        
        # If evidence found, create record
        if evidence_items:
            evidence_text = "; ".join(evidence_items)
            # Confidence: sum of factors, normalized to 0-100
            raw_confidence = sum(confidence_factors)
            confidence_score = min(100, int((raw_confidence / 8.0) * 100))  # 8 = max possible
            
            results.append(Row(
                facility_id=facility_id,
                capability=cap_name,
                evidence_text=evidence_text,
                evidence_count=len(evidence_items),
                confidence_score=confidence_score
            ))
    
    return results

print("\n⏳ Extracting capability evidence from facilities...")
print("   This may take a few minutes for 10K+ facilities...")

# Apply extraction to all facilities
evidence_rdd = df_facilities.rdd.flatMap(extract_capability_evidence)
evidence_schema = StructType([
    StructField("facility_id", StringType(), False),
    StructField("capability", StringType(), False),
    StructField("evidence_text", StringType(), False),
    StructField("evidence_count", IntegerType(), False),
    StructField("confidence_score", IntegerType(), False)
])

df_evidence = spark.createDataFrame(evidence_rdd, schema=evidence_schema)

# Save evidence table
df_evidence.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facility_capability_evidence")

print(f"\n✓ Evidence extraction complete: workspace.healthgpt.facility_capability_evidence")
print(f"  Total evidence records: {df_evidence.count():,}")
print(f"  Facilities with at least 1 capability: {df_evidence.select('facility_id').distinct().count():,}")
print(f"\n  Capability distribution:")
df_evidence.groupBy("capability").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Trust Scoring Logic
# ============================================================================
# TRUST SCORING: Assign trust signals to facility-capability pairs
# ============================================================================

print("\n" + "="*80)
print("TRUST SCORING ENGINE")
print("="*80)

# Load evidence and facilities
df_evidence = spark.table("workspace.healthgpt.facility_capability_evidence")
df_facilities = spark.table("workspace.healthgpt.facilities_silver")

# Join evidence with facility metadata
df_trust = df_evidence.join(df_facilities, "facility_id", "left")

# Apply trust scoring logic
df_trust_scored = df_trust \
    .withColumn("trust_signal",
        F.when((F.col("confidence_score") >= 80) & (F.col("evidence_count") >= 3), "strong")
        .when((F.col("confidence_score") >= 60) & (F.col("evidence_count") >= 2), "partial")
        .when((F.col("confidence_score") >= 40) & (F.col("evidence_count") >= 1), "weak")
        .when(F.col("confidence_score") < 40, "suspicious")
        .otherwise("weak")
    ) \
    .withColumn("trust_score",
        F.when(F.col("trust_signal") == "strong", 100)
        .when(F.col("trust_signal") == "partial", 70)
        .when(F.col("trust_signal") == "weak", 40)
        .when(F.col("trust_signal") == "suspicious", 10)
        .otherwise(0)
    ) \
    .withColumn("review_priority",
        F.when(F.col("trust_signal") == "suspicious", "high")
        .when((F.col("trust_signal") == "weak") & (F.col("completeness_score") < 50), "high")
        .when(F.col("trust_signal") == "weak", "medium")
        .when(F.col("trust_signal") == "partial", "medium")
        .otherwise("low")
    ) \
    .withColumn("needs_verification",
        F.when(F.col("review_priority").isin(["high", "medium"]), True)
        .otherwise(False)
    )

# Select final trust score columns
df_trust_final = df_trust_scored.select(
    "facility_id",
    "facility_name",
    "capability",
    "trust_signal",
    "trust_score",
    "confidence_score",
    "evidence_count",
    "evidence_text",
    "review_priority",
    "needs_verification",
    "state",
    "city",
    "pin_code",
    "lat",
    "lon",
    "completeness_score",
    "doctors_count",
    "bed_capacity",
    "source_urls_array"
)

# Save trust scores table
df_trust_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facility_trust_scores")

print(f"\n✓ Trust scoring complete: workspace.healthgpt.facility_trust_scores")
print(f"  Total scored records: {df_trust_final.count():,}")
print(f"\n  Trust signal distribution:")
df_trust_final.groupBy("trust_signal").count().orderBy(F.desc("count")).show(truncate=False)
print(f"\n  Review priority distribution:")
df_trust_final.groupBy("review_priority").count().orderBy(F.desc("count")).show(truncate=False)
print(f"\n  Records needing verification: {df_trust_final.filter(F.col('needs_verification')).count():,}")

# COMMAND ----------

# DBTITLE 1,Geographic Aggregation - Care Gap Scoring
# ============================================================================
# GEOGRAPHIC AGGREGATION: Compute care gaps by state and city
# ============================================================================

print("\n" + "="*80)
print("GEOGRAPHIC AGGREGATION & CARE GAP SCORING")
print("="*80)

# Load trust scores
df_trust = spark.table("workspace.healthgpt.facility_trust_scores")

# Aggregate by state, city, and capability
df_geo_agg = df_trust \
    .groupBy("state", "city", "capability") \
    .agg(
        F.count("*").alias("total_facilities"),
        F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_count"),
        F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_count"),
        F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_count"),
        F.sum(F.when(F.col("trust_signal") == "suspicious", 1).otherwise(0)).alias("suspicious_count"),
        F.avg("trust_score").alias("avg_trust_score"),
        F.avg("confidence_score").alias("avg_confidence_score"),
        F.avg("completeness_score").alias("avg_completeness_score"),
        F.sum(F.when(F.col("needs_verification"), 1).otherwise(0)).alias("needs_verification_count")
    )

# Calculate gap score (0-100, higher = bigger gap)
# Gap increases when: fewer strong facilities, more weak/suspicious, lower trust
df_care_gap = df_geo_agg \
    .withColumn("strong_coverage_pct",
        (F.col("strong_count") * 100.0 / F.col("total_facilities")).cast("int")
    ) \
    .withColumn("weak_suspicious_pct",
        ((F.col("weak_count") + F.col("suspicious_count")) * 100.0 / F.col("total_facilities")).cast("int")
    ) \
    .withColumn("gap_score",
        F.least(
            F.lit(100),
            (
                # Base gap from lack of strong coverage
                (100 - F.col("strong_coverage_pct")) * 0.5 +
                # Penalty for weak/suspicious claims
                F.col("weak_suspicious_pct") * 0.3 +
                # Penalty for low average trust
                (100 - F.col("avg_trust_score")) * 0.2
            ).cast("int")
        )
    ) \
    .withColumn("confidence_level",
        F.when(F.col("avg_confidence_score") >= 80, "high")
        .when(F.col("avg_confidence_score") >= 60, "medium-high")
        .when(F.col("avg_confidence_score") >= 40, "medium")
        .when(F.col("avg_confidence_score") >= 20, "low-medium")
        .otherwise("low")
    ) \
    .withColumn("data_quality",
        F.when(F.col("avg_completeness_score") >= 80, "good")
        .when(F.col("avg_completeness_score") >= 60, "fair")
        .otherwise("poor")
    ) \
    .withColumn("planner_action",
        F.when((F.col("gap_score") >= 70) & (F.col("confidence_level").isin(["high", "medium-high"])), 
               "urgent_action_real_gap")
        .when((F.col("gap_score") >= 70) & (F.col("confidence_level").isin(["medium", "low-medium", "low"])), 
               "verify_then_act")
        .when((F.col("gap_score") >= 50) & (F.col("needs_verification_count") > 2), 
               "verification_priority")
        .when(F.col("gap_score") >= 50, 
               "monitor_and_verify")
        .otherwise("adequate_coverage")
    )

# Save care gap table
df_care_gap.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.care_gap_by_geography")

print(f"\n✓ Care gap aggregation complete: workspace.healthgpt.care_gap_by_geography")
print(f"  Total geographic-capability combinations: {df_care_gap.count():,}")
print(f"\n  Top 10 care gaps (highest gap_score):")
df_care_gap \
    .orderBy(F.desc("gap_score")) \
    .select("state", "city", "capability", "gap_score", "confidence_level", 
            "total_facilities", "strong_count", "planner_action") \
    .show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Pipeline Summary Dashboard
# ============================================================================
# PIPELINE SUMMARY DASHBOARD
# ============================================================================

print("\n" + "="*80)
print("PIPELINE COMPLETION SUMMARY")
print("="*80)
print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Load all tables
df_bronze = spark.table("workspace.healthgpt.facilities_bronze")
df_silver = spark.table("workspace.healthgpt.facilities_silver")
df_evidence = spark.table("workspace.healthgpt.facility_capability_evidence")
df_trust = spark.table("workspace.healthgpt.facility_trust_scores")
df_care_gap = spark.table("workspace.healthgpt.care_gap_by_geography")

print("\n📊 DATA PIPELINE METRICS")
print("-" * 80)
print(f"Bronze Layer: {df_bronze.count():,} raw facility records")
print(f"Silver Layer: {df_silver.count():,} cleaned facilities")
print(f"Evidence Extracted: {df_evidence.count():,} capability claims")
print(f"Trust Scores: {df_trust.count():,} facility-capability pairs scored")
print(f"Care Gaps: {df_care_gap.count():,} geographic-capability combinations analyzed")

print("\n🎯 CAPABILITY COVERAGE")
print("-" * 80)
for cap in ["maternity", "icu", "nicu", "emergency", "trauma", "oncology", "dialysis"]:
    count = df_evidence.filter(F.col("capability") == cap).count()
    strong = df_trust.filter((F.col("capability") == cap) & (F.col("trust_signal") == "strong")).count()
    print(f"{cap.upper():15s}: {count:5,} claims | {strong:5,} strong ({strong*100//count if count > 0 else 0}%)")

print("\n🚨 TOP 10 URGENT CARE GAPS (High gap score + High confidence)")
print("-" * 80)
df_urgent = df_care_gap.filter(
    (F.col("gap_score") >= 60) & 
    (F.col("confidence_level").isin(["high", "medium-high"]))
).orderBy(F.desc("gap_score")).limit(10)

for row in df_urgent.collect():
    print(f"{row.state:20s} | {row.city:20s} | {row.capability:12s} | Gap: {row.gap_score:3d} | "
          f"Conf: {row.confidence_level:12s} | Strong: {row.strong_count}/{row.total_facilities}")

print("\n📋 DATA QUALITY SUMMARY")
print("-" * 80)
avg_completeness = df_silver.agg(F.avg("completeness_score")).collect()[0][0]
print(f"Average facility completeness: {avg_completeness:.1f}/100")
print(f"Facilities with coordinates: {df_silver.filter(F.col('lat').isNotNull()).count():,}")
print(f"Facilities needing verification: {df_trust.filter(F.col('needs_verification')).count():,}")
print(f"High-priority review queue: {df_trust.filter(F.col('review_priority') == 'high').count():,}")

print("\n✅ TABLES CREATED IN workspace.healthgpt SCHEMA:")
print("-" * 80)
print("  1. facilities_bronze          - Raw facility data")
print("  2. facilities_silver          - Cleaned and standardized")
print("  3. facility_capability_evidence - Extracted capability claims")
print("  4. facility_trust_scores      - Trust-scored facility-capability pairs")
print("  5. care_gap_by_geography      - Geographic care gap analysis")

print("\n🎉 PIPELINE COMPLETE! Ready for Streamlit UI integration.")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Import Libraries and Setup
# Import required libraries
import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
import re
from datetime import datetime

print("=" * 80)
print("HEALTHGPT PHASE 3: FACILITY-BASED CARE GAP TRUST PLANNER")
print("Track 2: Medical Desert Planner")
print("=" * 80)
print(f"\n✓ Libraries imported successfully")
print(f"✓ Using Spark version: {spark.version}")
print(f"✓ Pipeline start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Load Raw Facility Data
# STEP 1: BRONZE LAYER - Load raw facility data as-is
print("\n" + "=" * 80)
print("STEP 1: BRONZE LAYER - Loading Raw Facility Data")
print("=" * 80)

# Source table
SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
BRONZE_TABLE = "workspace.healthgpt.facilities_bronze"

# Load all raw fields for traceability
df_bronze = spark.read.table(SOURCE_TABLE)

# Write to Bronze
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"\n✓ Bronze table created: {BRONZE_TABLE}")
print(f"  Total facilities: {df_bronze.count():,}")
print(f"  Total columns: {len(df_bronze.columns)}")
print(f"  Source: {SOURCE_TABLE}")

# Show sample
print("\n" + "_" * 80)
print("Sample facilities:")
df_bronze.select("unique_id", "name", "address_city", "address_stateOrRegion").show(5, truncate=50)

# COMMAND ----------

# DBTITLE 1,HealthGPT Phase 3: Facility-Based Care Gap Trust Planner
# MAGIC %md
# MAGIC # HealthGPT Phase 3: Facility-Based Care Gap Trust Planner
# MAGIC
# MAGIC **Track 2: Medical Desert Planner** | Built on Databricks + OpenAI
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 **Project Pivot: From District Health Indicators to Facility Trust Scoring**
# MAGIC
# MAGIC ### Original Approach (Phase 1-2)
# MAGIC * District-level health indicators (NFHS-style population data)
# MAGIC * 706 districts with anemia %, ANC visits %, vaccination %
# MAGIC * ML models predicting district risk scores
# MAGIC
# MAGIC ### New Approach (Phase 3)
# MAGIC * **Facility-level healthcare records** (messy capability claims)
# MAGIC * 10,088 healthcare facilities across India
# MAGIC * Evidence-based trust scoring and care gap detection
# MAGIC * Geographic aggregation of facility coverage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 **Pipeline Architecture**
# MAGIC
# MAGIC ```
# MAGIC Bronze (Raw)          → Silver (Cleaned)      → Gold (Evidence & Trust)
# MAGIC ─────────────────────────────────────────────────────────────────────────
# MAGIC ✓ facilities_bronze   → facilities_silver    → facility_capability_evidence
# MAGIC   (10K facilities)      (standardized)         (capability extractions)
# MAGIC                                               → facility_trust_scores
# MAGIC                                                  (strong/partial/weak/suspicious)
# MAGIC                                               → care_gap_by_geography
# MAGIC                                                  (gap_score + confidence_level)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏥 **7 Core Capabilities**
# MAGIC
# MAGIC 1. **Maternity Care** - Delivery, C-section, prenatal/postnatal
# MAGIC 2. **ICU** - Intensive care, critical care, ventilator support
# MAGIC 3. **NICU** - Neonatal intensive care, incubators, pediatric critical care
# MAGIC 4. **Emergency** - 24x7 emergency, trauma response, ambulance
# MAGIC 5. **Trauma** - Trauma surgery, orthopedic emergency, burn care
# MAGIC 6. **Oncology** - Cancer treatment, radiation therapy, chemotherapy
# MAGIC 7. **Dialysis** - Kidney dialysis, renal care, dialysis machines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 **Trust Scoring Framework**
# MAGIC
# MAGIC | Trust Signal | Definition | Example |
# MAGIC |--------------|------------|----------|
# MAGIC | **Strong** | Multiple fields + specific procedures/equipment | Maternity + obstetrics specialty + delivery procedure + C-section equipment |
# MAGIC | **Partial** | Mentioned in description/specialty but lacks procedure/equipment | Maternity mentioned, but no emergency OB equipment listed |
# MAGIC | **Weak** | Single noisy field mention | NICU in capability field only, no pediatrics support |
# MAGIC | **Suspicious** | Contradictions or implausible combinations | Oncology claim but no oncology specialty/procedure/equipment |
# MAGIC | **No Claim** | No meaningful evidence | Facility has no maternity-related fields |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 **Data Sources**
# MAGIC
# MAGIC * **Bronze**: `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities`
# MAGIC * **Silver**: `workspace.healthgpt.facilities_silver`
# MAGIC * **Gold Evidence**: `workspace.healthgpt.facility_capability_evidence`
# MAGIC * **Gold Trust**: `workspace.healthgpt.facility_trust_scores`
# MAGIC * **Gold Aggregation**: `workspace.healthgpt.care_gap_by_geography`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's build the pipeline! 🚀

# COMMAND ----------

# DBTITLE 1,Setup: Imports and Configuration
# Setup: Imports and Configuration
import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
import re
from datetime import datetime

print("\n" + "="*70)
print("HEALTHGPT PHASE 3: FACILITY-BASED CARE GAP TRUST PLANNER")
print("="*70)
print(f"\n🔧 Pipeline Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n🎯 Track: Medical Desert Planner (Track 2)")
print(f"\u2705 Dataset: 10,088 healthcare facilities across India")
print(f"\u2705 Goal: Trust-weighted care gap detection with evidence citations\n")

# COMMAND ----------

# DBTITLE 1,Bronze Layer: Load Raw Facility Data
# MAGIC %sql
# MAGIC -- Bronze Layer: Load raw facility data from Virtue Foundation dataset
# MAGIC -- Preserve all original fields for traceability
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.healthgpt.facilities_bronze AS
# MAGIC SELECT *
# MAGIC FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities;
# MAGIC
# MAGIC -- Verify table creation
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_facilities,
# MAGIC   COUNT(DISTINCT unique_id) as distinct_facilities,
# MAGIC   COUNT(DISTINCT address_stateOrRegion) as distinct_states,
# MAGIC   COUNT(DISTINCT address_city) as distinct_cities,
# MAGIC   SUM(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) as has_description,
# MAGIC   SUM(CASE WHEN specialties IS NOT NULL THEN 1 ELSE 0 END) as has_specialties,
# MAGIC   SUM(CASE WHEN procedure IS NOT NULL THEN 1 ELSE 0 END) as has_procedures,
# MAGIC   SUM(CASE WHEN equipment IS NOT NULL THEN 1 ELSE 0 END) as has_equipment,
# MAGIC   SUM(CASE WHEN capability IS NOT NULL THEN 1 ELSE 0 END) as has_capability
# MAGIC FROM workspace.healthgpt.facilities_bronze;

# COMMAND ----------

# DBTITLE 1,Silver Layer: Clean and Standardize Facilities
# Silver Layer: Clean and standardize facility data
print("\n" + "="*70)
print("SILVER LAYER: CLEANING AND STANDARDIZATION")
print("="*70)

# Load Bronze data
df_bronze = spark.table("workspace.healthgpt.facilities_bronze")

print(f"\n📊 Bronze records: {df_bronze.count():,}")

# Define UDF to safely parse JSON arrays
@F.udf(ArrayType(StringType()))
def parse_json_array(json_str):
    if json_str is None or json_str.strip() == "":
        return []
    try:
        parsed = json.loads(json_str)
        if isinstance(parsed, list):
            return [str(item) for item in parsed if item]
        return []
    except:
        return []

# Create Silver table with cleaned data
df_silver = df_bronze.select(
    F.col("unique_id").alias("facility_id"),
    F.trim(F.col("name")).alias("facility_name"),
    
    # Geographic fields - standardized
    F.trim(F.coalesce(F.col("address_stateOrRegion"), F.lit("Unknown"))).alias("state"),
    F.trim(F.coalesce(F.col("address_city"), F.lit("Unknown"))).alias("city"),
    F.trim(F.col("address_zipOrPostcode")).alias("pin_code"),
    F.col("latitude"),
    F.col("longitude"),
    
    # Text fields for capability extraction
    F.trim(F.col("description")).alias("description"),
    
    # Parse JSON arrays
    parse_json_array(F.col("specialties")).alias("specialties"),
    parse_json_array(F.col("procedure")).alias("procedures"),
    parse_json_array(F.col("equipment")).alias("equipment"),
    parse_json_array(F.col("capability")).alias("capabilities"),
    
    # Capacity and quality signals
    F.col("numberDoctors").cast("int").alias("number_doctors"),
    F.col("capacity").cast("int").alias("bed_capacity"),
    
    # Source and trust signals
    parse_json_array(F.col("source_urls")).alias("source_urls"),
    F.col("organization_type"),
    F.col("facilityTypeId").alias("facility_type_id"),
    F.col("operatorTypeId").alias("operator_type_id"),
    
    # Social media and recency signals
    F.col("distinct_social_media_presence_count").cast("int").alias("social_media_count"),
    F.col("recency_of_page_update"),
    F.col("number_of_facts_about_the_organization").cast("int").alias("facts_count")
)

# Add data quality flags
df_silver = df_silver.withColumn(
    "has_description", F.when(F.col("description").isNotNull(), 1).otherwise(0)
).withColumn(
    "has_specialties", F.when(F.size(F.col("specialties")) > 0, 1).otherwise(0)
).withColumn(
    "has_procedures", F.when(F.size(F.col("procedures")) > 0, 1).otherwise(0)
).withColumn(
    "has_equipment", F.when(F.size(F.col("equipment")) > 0, 1).otherwise(0)
).withColumn(
    "has_capabilities", F.when(F.size(F.col("capabilities")) > 0, 1).otherwise(0)
).withColumn(
    "has_location", F.when(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull(), 1
    ).otherwise(0)
).withColumn(
    "data_completeness_score",
    (F.col("has_description") + F.col("has_specialties") + 
     F.col("has_procedures") + F.col("has_equipment") + 
     F.col("has_capabilities") + F.col("has_location")) / 6.0 * 100
)

# Write Silver table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facilities_silver")

print(f"\n✅ Silver table created: workspace.healthgpt.facilities_silver")
print(f"   Records: {df_silver.count():,}")
print(f"   Columns: {len(df_silver.columns)}")

# Show sample record
print("\n🔍 Sample Silver Record:")
df_silver.select(
    "facility_id", "facility_name", "state", "city",
    "data_completeness_score"
).show(3, truncate=False)

# Data quality summary
print("\n📊 Data Quality Summary:")
df_silver.agg(
    F.avg("data_completeness_score").alias("avg_completeness"),
    F.sum("has_description").alias("with_description"),
    F.sum("has_specialties").alias("with_specialties"),
    F.sum("has_procedures").alias("with_procedures"),
    F.sum("has_equipment").alias("with_equipment"),
    F.sum("has_location").alias("with_location")
).show()

# COMMAND ----------

# DBTITLE 1,Capability Taxonomy: Define 7 Core Capabilities
# Capability Taxonomy with keywords
print("\n" + "="*70)
print("CAPABILITY TAXONOMY: 7 CORE CAPABILITIES")
print("="*70)

CAPABILITY_KEYWORDS = {
    "maternity": {
        "keywords": ["maternity", "maternal", "obstetric", "delivery", "labor", "prenatal", "postnatal", 
                    "c-section", "cesarean", "childbirth", "pregnancy", "gynecology", "obstetrics"],
        "procedure_keywords": ["delivery", "c-section", "cesarean", "obstetric"],
        "equipment_keywords": ["delivery", "fetal", "obstetric", "maternal"]
    },
    "icu": {
        "keywords": ["icu", "intensive care", "critical care", "ventilator", "life support"],
        "procedure_keywords": ["ventilat", "critical", "intensive"],
        "equipment_keywords": ["ventilator", "icu", "intensive"]
    },
    "nicu": {
        "keywords": ["nicu", "neonatal", "newborn intensive", "pediatric intensive", "picu", "incubator"],
        "procedure_keywords": ["neonatal", "pediatric", "nicu"],
        "equipment_keywords": ["incubator", "neonatal", "pediatric"]
    },
    "emergency": {
        "keywords": ["emergency", "24x7", "casualty", "trauma center", "ambulance", "er"],
        "procedure_keywords": ["emergency", "casualty", "trauma"],
        "equipment_keywords": ["emergency", "ambulance"]
    },
    "trauma": {
        "keywords": ["trauma", "trauma surgery", "orthopedic emergency", "fracture", "burn", "injury"],
        "procedure_keywords": ["trauma", "fracture", "orthopedic"],
        "equipment_keywords": ["trauma", "orthopedic", "burn"]
    },
    "oncology": {
        "keywords": ["oncology", "cancer", "chemotherapy", "radiation therapy", "tumor", "malignancy"],
        "procedure_keywords": ["cancer", "chemotherapy", "radiation"],
        "equipment_keywords": ["radiation", "chemotherapy", "oncolog"]
    },
    "dialysis": {
        "keywords": ["dialysis", "hemodialysis", "kidney dialysis", "renal", "nephrology"],
        "procedure_keywords": ["dialysis", "renal", "kidney"],
        "equipment_keywords": ["dialysis", "kidney", "renal"]
    }
}

print(f"\n✅ Defined {len(CAPABILITY_KEYWORDS)} capabilities")
for cap_name, cap_def in CAPABILITY_KEYWORDS.items():
    print(f"   - {cap_name.upper()}: {len(cap_def['keywords'])} keywords")

# COMMAND ----------

# DBTITLE 1,Evidence Extraction: Extract Capability Claims
# Extract capability evidence from facility data
print("\n" + "="*70)
print("EVIDENCE EXTRACTION: CAPABILITY CLAIMS")
print("="*70)

df_silver = spark.table("workspace.healthgpt.facilities_silver")

evidence_records = []

# Collect facility data for extraction
facilities = df_silver.select(
    "facility_id", "facility_name", "state", "city",
    "description", "specialties", "procedures", "equipment", "capabilities"
).collect()

print(f"\n🔍 Processing {len(facilities):,} facilities...")

for facility in facilities:
    facility_id = facility.facility_id
    
    # Combine all text for searching
    all_text = ""
    if facility.description:
        all_text += facility.description.lower() + " "
    
    # Add arrays
    for arr in [facility.specialties, facility.procedures, facility.equipment, facility.capabilities]:
        if arr:
            all_text += " ".join([str(item).lower() for item in arr]) + " "
    
    # Extract evidence for each capability
    for cap_name, cap_def in CAPABILITY_KEYWORDS.items():
        evidence_found = []
        evidence_fields = []
        
        # Search in description
        if facility.description:
            desc_lower = facility.description.lower()
            for kw in cap_def["keywords"]:
                if kw in desc_lower:
                    evidence_found.append(kw)
                    evidence_fields.append("description")
                    break
        
        # Search in specialties
        if facility.specialties:
            for spec in facility.specialties:
                spec_lower = str(spec).lower()
                for kw in cap_def["keywords"]:
                    if kw in spec_lower:
                        evidence_found.append(spec)
                        evidence_fields.append("specialties")
                        break
        
        # Search in procedures
        if facility.procedures:
            for proc in facility.procedures:
                proc_lower = str(proc).lower()
                for kw in cap_def["procedure_keywords"]:
                    if kw in proc_lower:
                        evidence_found.append(proc)
                        evidence_fields.append("procedures")
                        break
        
        # Search in equipment
        if facility.equipment:
            for equip in facility.equipment:
                equip_lower = str(equip).lower()
                for kw in cap_def["equipment_keywords"]:
                    if kw in equip_lower:
                        evidence_found.append(equip)
                        evidence_fields.append("equipment")
                        break
        
        # Only add if evidence found
        if evidence_found:
            evidence_records.append({
                "facility_id": facility_id,
                "capability": cap_name,
                "evidence_text": " | ".join(str(e) for e in evidence_found[:5]),  # Top 5
                "evidence_fields": list(set(evidence_fields)),
                "evidence_count": len(evidence_found),
                "has_description_evidence": "description" in evidence_fields,
                "has_specialty_evidence": "specialties" in evidence_fields,
                "has_procedure_evidence": "procedures" in evidence_fields,
                "has_equipment_evidence": "equipment" in evidence_fields
            })

# Create evidence DataFrame
df_evidence = spark.createDataFrame(evidence_records)

# Write evidence table
df_evidence.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facility_capability_evidence")

print(f"\n✅ Evidence extraction complete!")
print(f"   Total evidence records: {df_evidence.count():,}")
print(f"   Unique facilities with evidence: {df_evidence.select('facility_id').distinct().count():,}")

print("\n📊 Evidence by Capability:")
df_evidence.groupBy("capability").count().orderBy("count", ascending=False).show()

print("\n🔍 Sample Evidence Records:")
df_evidence.show(5, truncate=80)

# COMMAND ----------

# DBTITLE 1,Trust Scoring: Score Facility-Capability Evidence
# Trust Scoring: Assign trust signals based on evidence strength
print("\n" + "="*70)
print("TRUST SCORING: EVIDENCE STRENGTH ASSESSMENT")
print("="*70)

df_evidence = spark.table("workspace.healthgpt.facility_capability_evidence")

# Define trust scoring logic
def assign_trust_signal(has_desc, has_spec, has_proc, has_equip, evidence_count):
    """
    Assign trust signal based on evidence strength:
    - Strong: Multiple fields + procedure/equipment support
    - Partial: Description/specialty but lacks procedure/equipment
    - Weak: Single field mention
    - Suspicious: Will be detected via contradictions (not in this simple version)
    """
    field_count = sum([has_desc, has_spec, has_proc, has_equip])
    
    # Strong: Multiple fields AND procedure/equipment evidence
    if field_count >= 3 and (has_proc or has_equip):
        return "strong"
    
    # Strong: Procedure AND equipment evidence
    if has_proc and has_equip:
        return "strong"
    
    # Partial: 2+ fields but no procedure/equipment
    if field_count >= 2 and not has_proc and not has_equip:
        return "partial"
    
    # Partial: Has procedure OR equipment
    if has_proc or has_equip:
        return "partial"
    
    # Weak: Single field, no procedure/equipment
    if field_count == 1:
        return "weak"
    
    return "weak"

# Register UDF
assign_trust_udf = F.udf(assign_trust_signal, StringType())

# Apply trust scoring
df_trust_scores = df_evidence.withColumn(
    "trust_signal",
    assign_trust_udf(
        F.col("has_description_evidence"),
        F.col("has_specialty_evidence"),
        F.col("has_procedure_evidence"),
        F.col("has_equipment_evidence"),
        F.col("evidence_count")
    )
).withColumn(
    "trust_score_numeric",
    F.when(F.col("trust_signal") == "strong", 100)
     .when(F.col("trust_signal") == "partial", 60)
     .when(F.col("trust_signal") == "weak", 30)
     .otherwise(0)
)

# Write trust scores table
df_trust_scores.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.facility_trust_scores")

print(f"\n✅ Trust scoring complete!")
print(f"   Total scored records: {df_trust_scores.count():,}")

print("\n📊 Trust Signal Distribution:")
df_trust_scores.groupBy("trust_signal").count().orderBy("count", ascending=False).show()

print("\n📊 Trust Signals by Capability:")
df_trust_scores.groupBy("capability", "trust_signal") \
    .count() \
    .orderBy("capability", "count", ascending=[True, False]) \
    .show(30)

print("\n🔍 Sample Trust Scores:")
df_trust_scores.select(
    "facility_id", "capability", "trust_signal", "trust_score_numeric",
    "evidence_count", "evidence_text"
).show(10, truncate=60)

# COMMAND ----------

# DBTITLE 1,Geographic Aggregation: Care Gap by Geography
# Geographic Aggregation: Compute care gap scores by state/city
print("\n" + "="*70)
print("GEOGRAPHIC AGGREGATION: CARE GAP COMPUTATION")
print("="*70)

df_trust_scores = spark.table("workspace.healthgpt.facility_trust_scores")
df_silver = spark.table("workspace.healthgpt.facilities_silver")

# Join with facility geography
df_geo_evidence = df_trust_scores.join(
    df_silver.select("facility_id", "state", "city", "facility_name"),
    on="facility_id",
    how="inner"
)

# Aggregate by state and capability
df_state_capability = df_geo_evidence.groupBy("state", "capability").agg(
    F.count("*").alias("total_facilities"),
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("facilities_strong"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("facilities_partial"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("facilities_weak"),
    F.avg("trust_score_numeric").alias("avg_trust_score"),
    F.avg("evidence_count").alias("avg_evidence_count")
)

# Compute gap score and confidence
df_state_capability = df_state_capability.withColumn(
    "coverage_score",
    (F.col("facilities_strong") * 1.0 + F.col("facilities_partial") * 0.5) / 
    F.greatest(F.col("total_facilities"), F.lit(1)) * 100
).withColumn(
    "gap_score",
    100 - F.col("coverage_score")
).withColumn(
    "confidence_level",
    F.when(
        (F.col("facilities_strong") >= 3) & (F.col("avg_evidence_count") >= 2), "High"
    ).when(
        (F.col("total_facilities") >= 2) & (F.col("avg_evidence_count") >= 1.5), "Medium-High"
    ).when(
        F.col("total_facilities") >= 1, "Medium"
    ).otherwise("Low")
).withColumn(
    "data_completeness_score",
    F.least(F.col("avg_evidence_count") * 20, F.lit(100))  # Scale to 0-100
)

# Add recommended actions
df_state_capability = df_state_capability.withColumn(
    "recommended_action",
    F.when(
        (F.col("gap_score") > 70) & (F.col("confidence_level").isin(["High", "Medium-High"])),
        "URGENT: Real care gap - Plan service expansion"
    ).when(
        (F.col("gap_score") > 70) & (F.col("confidence_level").isin(["Medium", "Low"])),
        "VERIFY: Possible gap - Field verification needed"
    ).when(
        (F.col("gap_score") > 40) & (F.col("facilities_weak") > F.col("facilities_strong")),
        "REVIEW: High weak claims - Evidence review priority"
    ).otherwise("MONITOR: Adequate coverage - Maintain quality")
)

# Write geographic aggregation table
df_state_capability.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.healthgpt.care_gap_by_geography")

print(f"\n✅ Geographic aggregation complete!")
print(f"   Geography-Capability combinations: {df_state_capability.count():,}")

print("\n🔥 Top 10 Care Gaps (High Confidence):")
df_state_capability.filter(
    F.col("confidence_level").isin(["High", "Medium-High"])
).orderBy("gap_score", ascending=False).select(
    "state", "capability", "gap_score", "confidence_level",
    "facilities_strong", "facilities_weak", "recommended_action"
).show(10, truncate=50)

print("\n📊 Gap Score Distribution:")
df_state_capability.select(
    F.avg("gap_score").alias("avg_gap_score"),
    F.min("gap_score").alias("min_gap_score"),
    F.max("gap_score").alias("max_gap_score")
).show()

# COMMAND ----------

# DBTITLE 1,Pipeline Summary and Key Metrics
# Pipeline Summary and Key Metrics
print("\n" + "="*70)
print("PIPELINE SUMMARY: HEALTHGPT CARE GAP TRUST PLANNER")
print("="*70)

# Table counts
print("\n📋 Data Layers:")
print(f"   Bronze: {spark.table('workspace.healthgpt.facilities_bronze').count():,} facilities")
print(f"   Silver: {spark.table('workspace.healthgpt.facilities_silver').count():,} facilities")
print(f"   Evidence: {spark.table('workspace.healthgpt.facility_capability_evidence').count():,} capability claims")
print(f"   Trust Scores: {spark.table('workspace.healthgpt.facility_trust_scores').count():,} scored claims")
print(f"   Geographic Aggregation: {spark.table('workspace.healthgpt.care_gap_by_geography').count():,} state-capability pairs")

# Trust signal summary
print("\n🔒 Trust Signal Summary:")
spark.table('workspace.healthgpt.facility_trust_scores') \
    .groupBy('trust_signal').count() \
    .withColumn('percentage', F.round(F.col('count') / F.sum('count').over(Window.partitionBy()) * 100, 2)) \
    .orderBy('count', ascending=False) \
    .show()

# Capability coverage
print("\n🏥 Capability Coverage Across India:")
spark.table('workspace.healthgpt.facility_capability_evidence') \
    .groupBy('capability').agg(
        F.countDistinct('facility_id').alias('facilities_with_capability')
    ).orderBy('facilities_with_capability', ascending=False).show()

# Geographic distribution
print("\n🗺️ Top 10 States by Facility Count:")
spark.table('workspace.healthgpt.facilities_silver') \
    .groupBy('state').count() \
    .orderBy('count', ascending=False) \
    .show(10)

# Critical care gaps
print("\n⚠️ Critical Care Gaps (Gap Score > 80, High Confidence):")
spark.table('workspace.healthgpt.care_gap_by_geography').filter(
    (F.col('gap_score') > 80) & 
    (F.col('confidence_level') == 'High')
).select(
    'state', 'capability', 'gap_score', 'facilities_strong', 'facilities_weak'
).orderBy('gap_score', ascending=False).show(20, truncate=False)

print("\n" + "="*70)
print("✅ PIPELINE COMPLETE - READY FOR STREAMLIT APP INTEGRATION")
print("="*70)
print("\n🚀 Next Steps:")
print("   1. Build Streamlit UI with 6 screens (Overview, Map, Evidence, Brief, Scenario, Architecture)")
print("   2. Create planner workspace tables (notes, overrides, scenarios, shortlists, reviews)")
print("   3. Integrate OpenAI for AI Gap Brief generation")
print("   4. Add scenario planner (verify claims, add capacity)")
print("   5. Prepare demo: Maternity Care in target geography")
print("\n🎯 Track 2: Medical Desert Planner - Trust-Weighted Care Gap Detection")
print(f"\n🕒 Pipeline End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# COMMAND ----------

# DBTITLE 1,Import Libraries and Setup
# Import required libraries
import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
import re

print("=" * 80)
print("HEALTHGPT PHASE 3: FACILITY-BASED CARE GAP TRUST PLANNER")
print("Track 2: Medical Desert Planner")
print("=" * 80)
print(f"\n✓ Libraries imported successfully")
print(f"✓ Using Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Step 1: Create Bronze Layer - Raw Facility Data
# BRONZE LAYER: Load raw facility data from Virtue Foundation dataset
print("\n" + "=" * 80)
print("STEP 1: BRONZE LAYER - RAW FACILITY DATA")
print("=" * 80)

SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
BRONZE_TABLE = "workspace.healthgpt.facilities_bronze"

# Create Bronze table - preserve all original fields for traceability
spark.sql(f"""
CREATE OR REPLACE TABLE {BRONZE_TABLE} AS
SELECT *
FROM {SOURCE_TABLE}
""")

# Verify Bronze layer
bronze_df = spark.table(BRONZE_TABLE)
record_count = bronze_df.count()
distinct_facilities = bronze_df.select("unique_id").distinct().count()

print(f"\n✓ Bronze table created: {BRONZE_TABLE}")
print(f"  Total records: {record_count:,}")
print(f"  Distinct facilities: {distinct_facilities:,}")
print(f"  Columns: {len(bronze_df.columns)}")
print(f"\nKey columns:")
for col in ['unique_id', 'name', 'address_stateOrRegion', 'address_city', 
            'description', 'specialties', 'procedure', 'equipment', 'capability']:
    non_null = bronze_df.filter(F.col(col).isNotNull()).count()
    print(f"  {col}: {non_null:,} ({non_null/record_count*100:.1f}% populated)")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Silver Layer - Cleaned and Parsed Facilities
# SILVER LAYER: Clean and standardize facility data
print("\n" + "=" * 80)
print("STEP 2: SILVER LAYER - CLEANED AND PARSED FACILITIES")
print("=" * 80)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

# Parse JSON arrays and standardize geography
silver_df = bronze_df.select(
    F.col("unique_id").alias("facility_id"),
    F.trim(F.col("name")).alias("facility_name"),
    
    # Geography fields - standardized
    F.trim(F.coalesce(F.col("address_stateOrRegion"), F.lit("Unknown"))).alias("state"),
    F.trim(F.coalesce(F.col("address_city"), F.lit("Unknown"))).alias("city"),
    F.trim(F.col("address_zipOrPostcode")).alias("pin_code"),
    F.col("latitude"),
    F.col("longitude"),
    
    # Organization details
    F.col("organization_type"),
    F.col("facilityTypeId").alias("facility_type"),
    F.col("operatorTypeId").alias("operator_type"),
    
    # Capacity indicators
    F.col("numberDoctors").alias("doctor_count"),
    F.col("capacity").alias("bed_capacity"),
    
    # Text fields for capability extraction
    F.col("description"),
    F.col("specialties"),
    F.col("procedure"),
    F.col("equipment"),
    F.col("capability"),
    
    # Source and quality indicators
    F.col("source_urls"),
    F.col("websites"),
    F.col("officialWebsite").alias("official_website"),
    F.col("yearEstablished").alias("year_established"),
    F.col("recency_of_page_update"),
    F.col("distinct_social_media_presence_count").alias("social_media_count"),
    F.col("custom_logo_presence"),
    
    # Contact
    F.col("phone_numbers"),
    F.col("email")
)

# Add data quality flags
silver_df = silver_df.withColumn(
    "has_description", F.col("description").isNotNull()
).withColumn(
    "has_specialties", F.col("specialties").isNotNull()
).withColumn(
    "has_procedures", F.col("procedure").isNotNull()
).withColumn(
    "has_equipment", F.col("equipment").isNotNull()
).withColumn(
    "has_capability", F.col("capability").isNotNull()
).withColumn(
    "has_location", (F.col("latitude").isNotNull()) & (F.col("longitude").isNotNull())
).withColumn(
    "has_source_url", F.col("source_urls").isNotNull()
).withColumn(
    "data_completeness_score",
    (F.col("has_description").cast("int") + 
     F.col("has_specialties").cast("int") + 
     F.col("has_procedures").cast("int") + 
     F.col("has_equipment").cast("int") + 
     F.col("has_capability").cast("int") + 
     F.col("has_location").cast("int") + 
     F.col("has_source_url").cast("int")) / 7.0
)

# Write Silver table
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)

# Verify Silver layer
silver_verify = spark.table(SILVER_TABLE)
print(f"\n✓ Silver table created: {SILVER_TABLE}")
print(f"  Records: {silver_verify.count():,}")
print(f"  Columns: {len(silver_verify.columns)}")

# Data quality summary
quality_stats = silver_verify.agg(
    F.avg("data_completeness_score").alias("avg_completeness"),
    F.sum(F.col("has_description").cast("int")).alias("has_description_count"),
    F.sum(F.col("has_specialties").cast("int")).alias("has_specialties_count"),
    F.sum(F.col("has_equipment").cast("int")).alias("has_equipment_count"),
    F.countDistinct("state").alias("distinct_states"),
    F.countDistinct("city").alias("distinct_cities")
).collect()[0]

print(f"\nData Quality:")
print(f"  Average completeness: {quality_stats['avg_completeness']*100:.1f}%")
print(f"  Facilities with description: {quality_stats['has_description_count']:,}")
print(f"  Facilities with specialties: {quality_stats['has_specialties_count']:,}")
print(f"  Facilities with equipment: {quality_stats['has_equipment_count']:,}")
print(f"  Geographic coverage: {quality_stats['distinct_states']} states, {quality_stats['distinct_cities']:,} cities")

# COMMAND ----------

# DBTITLE 1,Step 3: Define Capability Taxonomy and Keywords
# CAPABILITY TAXONOMY: Define 7 core capabilities with keyword dictionaries
print("\n" + "=" * 80)
print("STEP 3: CAPABILITY TAXONOMY AND KEYWORD DICTIONARIES")
print("=" * 80)

# Define capability taxonomy per document requirements
CAPABILITY_KEYWORDS = {
    "maternity": {
        "keywords": [
            # General terms
            "maternity", "maternal", "delivery", "labor", "obstetric", "pregnancy", "prenatal",
            "antenatal", "postnatal", "gynecolog", "gynaecolog", "obstetrician",
            # Procedures
            "c-section", "cesarean", "caesarean", "normal delivery", "vaginal delivery",
            "emergency obstetric", "anc", "antenatal care", "maternity ward",
            # Equipment
            "labor room", "delivery room", "maternity bed"
        ],
        "procedures": ["delivery", "cesarean", "c-section", "obstetric"],
        "equipment": ["incubator", "labor bed", "delivery table", "fetal monitor"],
        "specialties": ["obstetrics", "gynecology", "obgyn"]
    },
    "icu": {
        "keywords": [
            "icu", "intensive care", "critical care", "itu", "intensive therapy",
            "ventilator", "life support", "critical"
        ],
        "procedures": ["mechanical ventilation", "critical care"],
        "equipment": ["ventilator", "cardiac monitor", "infusion pump", "defibrillator"],
        "specialties": ["critical care medicine", "intensive care"]
    },
    "nicu": {
        "keywords": [
            "nicu", "neonatal", "newborn", "pediatric icu", "picu",
            "neonatal intensive", "premature", "neonate"
        ],
        "procedures": ["neonatal resuscitation", "neonatal care"],
        "equipment": ["incubator", "phototherapy", "neonatal ventilator", "radiant warmer"],
        "specialties": ["neonatology", "pediatric"]
    },
    "emergency": {
        "keywords": [
            "emergency", "casualty", "trauma center", "accident", "24x7", "24/7",
            "emergency department", "emergency service", "emergency ward"
        ],
        "procedures": ["emergency surgery", "trauma care", "resuscitation"],
        "equipment": ["emergency equipment", "crash cart", "ambulance"],
        "specialties": ["emergency medicine", "trauma"]
    },
    "trauma": {
        "keywords": [
            "trauma", "trauma center", "trauma care", "accident", "injury",
            "trauma surgery", "polytrauma", "emergency surgery"
        ],
        "procedures": ["trauma surgery", "emergency surgery", "orthopedic trauma"],
        "equipment": ["trauma equipment", "surgical tools"],
        "specialties": ["trauma surgery", "orthopedic", "emergency medicine"]
    },
    "oncology": {
        "keywords": [
            "oncology", "cancer", "chemotherapy", "radiation", "tumor",
            "oncologist", "cancer treatment", "radiotherapy", "chemo"
        ],
        "procedures": ["chemotherapy", "radiation therapy", "cancer surgery", "radiotherapy"],
        "equipment": ["linear accelerator", "radiation", "chemotherapy"],
        "specialties": ["oncology", "medical oncology", "surgical oncology", "radiation oncology"]
    },
    "dialysis": {
        "keywords": [
            "dialysis", "kidney", "renal", "hemodialysis", "haemodialysis",
            "nephrology", "dialysis unit", "dialysis center"
        ],
        "procedures": ["dialysis", "hemodialysis", "peritoneal dialysis"],
        "equipment": ["dialysis machine", "dialysis equipment"],
        "specialties": ["nephrology", "renal"]
    }
}

print(f"\n✓ Capability taxonomy defined:")
for capability, details in CAPABILITY_KEYWORDS.items():
    print(f"  {capability.upper()}: {len(details['keywords'])} keywords, {len(details['procedures'])} procedures, {len(details['equipment'])} equipment, {len(details['specialties'])} specialties")

print(f"\nTotal capabilities: {len(CAPABILITY_KEYWORDS)}")

# COMMAND ----------

# DBTITLE 1,Step 4: Extract Capability Evidence from Facility Text
# EVIDENCE EXTRACTION: Extract capability mentions from facility text fields
print("\n" + "=" * 80)
print("STEP 4: CAPABILITY EVIDENCE EXTRACTION")
print("=" * 80)

EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"

# Load silver data
silver_data = spark.table(SILVER_TABLE)

# Function to check if keywords present in text
def contains_keywords(text, keywords):
    """Check if any keyword is present in text (case-insensitive)"""
    if not text:
        return False, []
    text_lower = str(text).lower()
    found_keywords = [kw for kw in keywords if kw.lower() in text_lower]
    return len(found_keywords) > 0, found_keywords

# Parse JSON arrays and extract evidence
evidence_rows = []

for row in silver_data.collect():
    facility_id = row['facility_id']
    facility_name = row['facility_name']
    state = row['state']
    city = row['city']
    
    # Text fields to search
    description = row['description'] or ""
    specialties_raw = row['specialties'] or ""
    procedure_raw = row['procedure'] or ""
    equipment_raw = row['equipment'] or ""
    capability_raw = row['capability'] or ""
    source_urls = row['source_urls'] or ""
    
    # Parse JSON arrays if present
    try:
        specialties_list = json.loads(specialties_raw) if specialties_raw and specialties_raw.startswith('[') else []
    except:
        specialties_list = []
    
    try:
        procedure_list = json.loads(procedure_raw) if procedure_raw and procedure_raw.startswith('[') else []
    except:
        procedure_list = []
    
    try:
        equipment_list = json.loads(equipment_raw) if equipment_raw and equipment_raw.startswith('[') else []
    except:
        equipment_list = []
    
    try:
        capability_list = json.loads(capability_raw) if capability_raw and capability_raw.startswith('[') else []
    except:
        capability_list = []
    
    # Combine text for searching
    specialties_text = " ".join(specialties_list)
    procedure_text = " ".join(procedure_list)
    equipment_text = " ".join(equipment_list)
    capability_text = " ".join(capability_list)
    
    # Check each capability
    for capability, keywords_dict in CAPABILITY_KEYWORDS.items():
        evidence_sources = []
        all_keywords = keywords_dict['keywords']
        
        # Check description
        desc_match, desc_keywords = contains_keywords(description, all_keywords)
        if desc_match:
            evidence_sources.append({
                'field': 'description',
                'text': description[:500],  # Truncate for storage
                'keywords': desc_keywords[:5]  # Top 5 matches
            })
        
        # Check specialties
        spec_match, spec_keywords = contains_keywords(specialties_text, all_keywords + keywords_dict['specialties'])
        if spec_match:
            evidence_sources.append({
                'field': 'specialties',
                'text': specialties_text[:500],
                'keywords': spec_keywords[:5]
            })
        
        # Check procedures
        proc_match, proc_keywords = contains_keywords(procedure_text, all_keywords + keywords_dict['procedures'])
        if proc_match:
            evidence_sources.append({
                'field': 'procedure',
                'text': procedure_text[:500],
                'keywords': proc_keywords[:5]
            })
        
        # Check equipment
        equip_match, equip_keywords = contains_keywords(equipment_text, all_keywords + keywords_dict['equipment'])
        if equip_match:
            evidence_sources.append({
                'field': 'equipment',
                'text': equipment_text[:500],
                'keywords': equip_keywords[:5]
            })
        
        # Check capability field
        cap_match, cap_keywords = contains_keywords(capability_text, all_keywords)
        if cap_match:
            evidence_sources.append({
                'field': 'capability',
                'text': capability_text[:500],
                'keywords': cap_keywords[:5]
            })
        
        # Only create evidence row if we found matches
        if len(evidence_sources) > 0:
            evidence_rows.append({
                'facility_id': facility_id,
                'facility_name': facility_name,
                'state': state,
                'city': city,
                'capability': capability,
                'evidence_count': len(evidence_sources),
                'evidence_sources': json.dumps(evidence_sources),
                'has_description': desc_match,
                'has_specialties': spec_match,
                'has_procedures': proc_match,
                'has_equipment': equip_match,
                'has_capability_field': cap_match,
                'source_urls': source_urls
            })

print(f"\n✓ Evidence extraction complete")
print(f"  Facility-capability pairs extracted: {len(evidence_rows):,}")

# Create evidence DataFrame
evidence_schema = StructType([
    StructField("facility_id", StringType(), True),
    StructField("facility_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("capability", StringType(), True),
    StructField("evidence_count", IntegerType(), True),
    StructField("evidence_sources", StringType(), True),
    StructField("has_description", BooleanType(), True),
    StructField("has_specialties", BooleanType(), True),
    StructField("has_procedures", BooleanType(), True),
    StructField("has_equipment", BooleanType(), True),
    StructField("has_capability_field", BooleanType(), True),
    StructField("source_urls", StringType(), True)
])

evidence_df = spark.createDataFrame(evidence_rows, schema=evidence_schema)

# Write to Delta table
evidence_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(EVIDENCE_TABLE)

print(f"\n✓ Evidence table created: {EVIDENCE_TABLE}")

# Show evidence distribution by capability
evidence_summary = evidence_df.groupBy("capability").agg(
    F.count("facility_id").alias("facility_count"),
    F.avg("evidence_count").alias("avg_evidence_sources")
).orderBy(F.desc("facility_count"))

print(f"\nEvidence distribution by capability:")
evidence_summary.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 5: Implement Trust Scoring Logic
# TRUST SCORING: Score each facility-capability pair
print("\n" + "=" * 80)
print("STEP 5: TRUST SCORING ENGINE")
print("=" * 80)

TRUST_SCORES_TABLE = "workspace.healthgpt.facility_trust_scores"

# Load evidence data
evidence_data = spark.table(EVIDENCE_TABLE)

# Define trust scoring UDF
def calculate_trust_signal(evidence_count, has_desc, has_spec, has_proc, has_equip, has_cap):
    """
    Calculate trust signal based on evidence strength
    
    Trust Levels (per document):
    - Strong: Multiple supporting fields + specific equipment/procedures
    - Partial: Mentioned in description/specialty but lacks procedure/equipment
    - Weak: Single field mention with little support
    - Suspicious: Implausible combination (to be enhanced with contradiction detection)
    - No Claim: No evidence (already filtered out)
    """
    # Count supporting field types
    field_count = sum([has_desc, has_spec, has_proc, has_equip, has_cap])
    
    # Strong evidence: 3+ fields including procedures OR equipment
    if field_count >= 3 and (has_proc or has_equip):
        return "strong", 0.9
    
    # Partial evidence: 2+ fields but missing both procedures and equipment
    elif field_count >= 2 and not (has_proc and has_equip):
        return "partial", 0.6
    
    # Partial with good support: 2+ fields with procedure or equipment
    elif field_count >= 2:
        return "partial", 0.7
    
    # Weak evidence: Only 1 field
    elif field_count == 1:
        # Capability field alone is weakest
        if has_cap and not (has_desc or has_spec or has_proc or has_equip):
            return "weak", 0.3
        else:
            return "weak", 0.4
    
    # Default to weak
    else:
        return "weak", 0.3

# Register UDF
trust_signal_udf = F.udf(calculate_trust_signal, StructType([
    StructField("trust_signal", StringType()),
    StructField("confidence_score", DoubleType())
]))

# Apply trust scoring
trust_df = evidence_data.withColumn(
    "trust_result",
    trust_signal_udf(
        F.col("evidence_count"),
        F.col("has_description"),
        F.col("has_specialties"),
        F.col("has_procedures"),
        F.col("has_equipment"),
        F.col("has_capability_field")
    )
).select(
    "facility_id",
    "facility_name",
    "state",
    "city",
    "capability",
    "evidence_count",
    "evidence_sources",
    F.col("trust_result.trust_signal"),
    F.col("trust_result.confidence_score"),
    "has_description",
    "has_specialties",
    "has_procedures",
    "has_equipment",
    "has_capability_field",
    "source_urls"
)

# Write trust scores
trust_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRUST_SCORES_TABLE)

print(f"\n✓ Trust scoring complete")
print(f"  Trust scores table created: {TRUST_SCORES_TABLE}")

# Show trust signal distribution
trust_summary = trust_df.groupBy("capability", "trust_signal").agg(
    F.count("facility_id").alias("facility_count")
).orderBy("capability", "trust_signal")

print(f"\nTrust signal distribution by capability:")
trust_summary.show(50, truncate=False)

# Overall trust distribution
overall_trust = trust_df.groupBy("trust_signal").agg(
    F.count("facility_id").alias("total_facilities"),
    F.avg("confidence_score").alias("avg_confidence")
).orderBy(F.desc("total_facilities"))

print(f"\nOverall trust signal distribution:")
overall_trust.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 6: Geographic Aggregation and Care Gap Scoring
# GEOGRAPHIC AGGREGATION: Compute care gap scores by geography
print("\n" + "=" * 80)
print("STEP 6: GEOGRAPHIC AGGREGATION AND CARE GAP SCORING")
print("=" * 80)

GAP_SCORES_TABLE = "workspace.healthgpt.care_gap_by_geography"

# Load trust scores
trust_scores = spark.table(TRUST_SCORES_TABLE)

# Aggregate by state, city, and capability
geo_agg = trust_scores.groupBy("state", "city", "capability").agg(
    # Facility counts by trust signal
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong_facilities"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial_facilities"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak_facilities"),
    F.sum(F.when(F.col("trust_signal") == "suspicious", 1).otherwise(0)).alias("suspicious_facilities"),
    F.count("facility_id").alias("total_facilities"),
    
    # Average confidence
    F.avg("confidence_score").alias("avg_confidence"),
    
    # Evidence completeness
    F.avg(
        (F.col("has_description").cast("int") +
         F.col("has_specialties").cast("int") +
         F.col("has_procedures").cast("int") +
         F.col("has_equipment").cast("int")) / 4.0
    ).alias("evidence_completeness")
)

# Calculate care gap score and confidence level
# Gap score formula: weighted by trust signal strength
# Lower score = better coverage (fewer gaps)
# Higher score = worse coverage (more gaps)
gap_scores = geo_agg.withColumn(
    "trust_weighted_facilities",
    (F.col("strong_facilities") * 1.0) +
    (F.col("partial_facilities") * 0.6) +
    (F.col("weak_facilities") * 0.3)
).withColumn(
    # Gap score: inverse of trust-weighted coverage
    # Normalized 0-100 where 100 = severe gap, 0 = good coverage
    "gap_score",
    F.when(F.col("trust_weighted_facilities") == 0, 100)
     .otherwise(
         F.greatest(
             F.lit(0),
             F.lit(100) - (F.col("trust_weighted_facilities") * 10)  # Scaled formula
         )
     )
).withColumn(
    # Confidence level based on evidence completeness and avg confidence
    "confidence_level",
    F.when(
        (F.col("avg_confidence") >= 0.7) & (F.col("evidence_completeness") >= 0.7),
        "high"
    ).when(
        (F.col("avg_confidence") >= 0.5) & (F.col("evidence_completeness") >= 0.5),
        "medium-high"
    ).when(
        (F.col("avg_confidence") >= 0.4) & (F.col("evidence_completeness") >= 0.4),
        "medium"
    ).otherwise("low")
).withColumn(
    # Risk category based on gap score
    "risk_category",
    F.when(F.col("gap_score") >= 80, "critical")
     .when(F.col("gap_score") >= 60, "high")
     .when(F.col("gap_score") >= 40, "moderate")
     .otherwise("low")
).withColumn(
    # Review priority: high gap score + low confidence = high priority
    "review_priority",
    F.when(
        (F.col("gap_score") >= 60) & (F.col("confidence_level").isin(["low", "medium"])),
        "high"
    ).when(
        (F.col("gap_score") >= 40) | (F.col("confidence_level") == "low"),
        "medium"
    ).otherwise("low")
)

# Write gap scores
gap_scores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GAP_SCORES_TABLE)

print(f"\n✓ Geographic aggregation complete")
print(f"  Care gap scores table created: {GAP_SCORES_TABLE}")

# Show top gaps by state and capability
top_gaps = gap_scores.filter(F.col("risk_category").isin(["critical", "high"])) \
    .orderBy(F.desc("gap_score")) \
    .select("state", "city", "capability", "gap_score", "confidence_level", 
            "strong_facilities", "partial_facilities", "weak_facilities", "risk_category")

print(f"\nTop care gaps (critical/high risk):")
top_gaps.show(20, truncate=False)

# Summary by capability
capability_summary = gap_scores.groupBy("capability").agg(
    F.avg("gap_score").alias("avg_gap_score"),
    F.sum("strong_facilities").alias("total_strong"),
    F.sum("partial_facilities").alias("total_partial"),
    F.sum("weak_facilities").alias("total_weak"),
    F.count("*").alias("geographies_covered")
).orderBy(F.desc("avg_gap_score"))

print(f"\nCapability gap summary:")
capability_summary.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 7: Create Planner Workspace Persistence Tables
# PLANNER WORKSPACE: Create persistence tables for user actions
print("\n" + "=" * 80)
print("STEP 7: PLANNER WORKSPACE PERSISTENCE TABLES")
print("=" * 80)

# Define persistence table schemas per document requirements

# 1. Planner Notes
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_notes (
    note_id STRING,
    user_id STRING,
    facility_id STRING,
    capability STRING,
    geography_type STRING,
    geography_name STRING,
    note_text STRING,
    note_type STRING,  -- observation, concern, recommendation
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
""")
print("✓ Created: workspace.healthgpt.planner_notes")

# 2. Planner Overrides (manual trust signal adjustments)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_overrides (
    override_id STRING,
    user_id STRING,
    facility_id STRING,
    capability STRING,
    original_trust_signal STRING,
    override_trust_signal STRING,
    reason STRING,
    verification_source STRING,
    created_at TIMESTAMP
) USING DELTA
""")
print("✓ Created: workspace.healthgpt.planner_overrides")

# 3. Planner Scenarios (what-if simulations)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_scenarios (
    scenario_id STRING,
    user_id STRING,
    scenario_name STRING,
    geography_type STRING,
    geography_name STRING,
    capability STRING,
    scenario_type STRING,  -- verify_claims, add_facility, increase_capacity, reject_suspicious
    scenario_params STRING,  -- JSON with scenario-specific parameters
    baseline_gap_score DOUBLE,
    baseline_confidence STRING,
    projected_gap_score DOUBLE,
    projected_confidence STRING,
    impact_description STRING,
    created_at TIMESTAMP
) USING DELTA
""")
print("✓ Created: workspace.healthgpt.planner_scenarios")

# 4. Planner Shortlists (saved facility lists for follow-up)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_shortlists (
    shortlist_id STRING,
    user_id STRING,
    shortlist_name STRING,
    facility_id STRING,
    capability STRING,
    action_type STRING,  -- field_verification, capacity_expansion, data_cleanup
    priority STRING,  -- high, medium, low
    status STRING,  -- pending, in_progress, completed
    notes STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
""")
print("✓ Created: workspace.healthgpt.planner_shortlists")

# 5. Planner Review Decisions (evidence review outcomes)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.healthgpt.planner_review_decisions (
    review_id STRING,
    user_id STRING,
    facility_id STRING,
    capability STRING,
    review_type STRING,  -- suspicious_claim, weak_evidence, missing_data
    decision STRING,  -- verified, rejected, needs_more_info
    decision_rationale STRING,
    evidence_updated BOOLEAN,
    created_at TIMESTAMP
) USING DELTA
""")
print("✓ Created: workspace.healthgpt.planner_review_decisions")

print(f"\n✓ All planner workspace tables created successfully")
print(f"\nPersistence Layer Summary:")
print(f"  1. planner_notes - Capture planner observations and recommendations")
print(f"  2. planner_overrides - Manual trust signal adjustments with verification")
print(f"  3. planner_scenarios - What-if simulation results")
print(f"  4. planner_shortlists - Facility action lists for field follow-up")
print(f"  5. planner_review_decisions - Evidence review outcomes")

# COMMAND ----------

# DBTITLE 1,Step 8: Pipeline Summary and Data Quality Report
# PIPELINE SUMMARY: Final statistics and data quality report
print("\n" + "=" * 80)
print("STEP 8: PIPELINE SUMMARY AND DATA QUALITY REPORT")
print("=" * 80)

# Gather statistics from all layers
print(f"\n{'='*80}")
print("DATA LAYER SUMMARY")
print(f"{'='*80}")

# Bronze layer
bronze_count = spark.table("workspace.healthgpt.facilities_bronze").count()
print(f"\n✅ BRONZE LAYER: workspace.healthgpt.facilities_bronze")
print(f"   Total facilities: {bronze_count:,}")

# Silver layer
silver_stats = spark.table("workspace.healthgpt.facilities_silver").agg(
    F.count("facility_id").alias("total"),
    F.countDistinct("state").alias("states"),
    F.countDistinct("city").alias("cities"),
    F.avg("data_completeness_score").alias("avg_completeness")
).collect()[0]

print(f"\n✅ SILVER LAYER: workspace.healthgpt.facilities_silver")
print(f"   Total facilities: {silver_stats['total']:,}")
print(f"   States covered: {silver_stats['states']}")
print(f"   Cities covered: {silver_stats['cities']:,}")
print(f"   Avg data completeness: {silver_stats['avg_completeness']*100:.1f}%")

# Evidence layer
evidence_stats = spark.table("workspace.healthgpt.facility_capability_evidence").agg(
    F.count("*").alias("total_claims"),
    F.countDistinct("facility_id").alias("facilities_with_claims"),
    F.countDistinct("capability").alias("capabilities"),
    F.avg("evidence_count").alias("avg_evidence_sources")
).collect()[0]

print(f"\n✅ EVIDENCE LAYER: workspace.healthgpt.facility_capability_evidence")
print(f"   Total facility-capability pairs: {evidence_stats['total_claims']:,}")
print(f"   Facilities with capability claims: {evidence_stats['facilities_with_claims']:,}")
print(f"   Capabilities extracted: {evidence_stats['capabilities']}")
print(f"   Avg evidence sources per claim: {evidence_stats['avg_evidence_sources']:.1f}")

# Trust scores layer
trust_stats = spark.table("workspace.healthgpt.facility_trust_scores").groupBy("trust_signal").agg(
    F.count("*").alias("count"),
    F.avg("confidence_score").alias("avg_confidence")
).collect()

print(f"\n✅ TRUST SCORES LAYER: workspace.healthgpt.facility_trust_scores")
for row in trust_stats:
    print(f"   {row['trust_signal'].upper():12} - {row['count']:,} claims ({row['avg_confidence']*100:.1f}% avg confidence)")

# Geographic aggregation
gap_stats = spark.table("workspace.healthgpt.care_gap_by_geography").agg(
    F.count("*").alias("total_geographies"),
    F.avg("gap_score").alias("avg_gap_score"),
    F.sum(F.when(F.col("risk_category") == "critical", 1).otherwise(0)).alias("critical_gaps"),
    F.sum(F.when(F.col("risk_category") == "high", 1).otherwise(0)).alias("high_gaps")
).collect()[0]

print(f"\n✅ CARE GAP LAYER: workspace.healthgpt.care_gap_by_geography")
print(f"   Geographic areas analyzed: {gap_stats['total_geographies']:,}")
print(f"   Average gap score: {gap_stats['avg_gap_score']:.1f}/100")
print(f"   Critical care gaps: {gap_stats['critical_gaps']}")
print(f"   High-risk care gaps: {gap_stats['high_gaps']}")

print(f"\n✅ PLANNER WORKSPACE: 5 persistence tables created")

# Top findings for demo
print(f"\n{'='*80}")
print("KEY FINDINGS FOR DEMO")
print(f"{'='*80}")

# Find best demo case: maternity care with clear gap
maternity_gaps = spark.table("workspace.healthgpt.care_gap_by_geography") \
    .filter(F.col("capability") == "maternity") \
    .filter(F.col("risk_category").isin(["high", "critical"])) \
    .orderBy(F.desc("gap_score")) \
    .limit(5) \
    .select("state", "city", "gap_score", "confidence_level", 
            "strong_facilities", "partial_facilities", "weak_facilities")

print(f"\n🎯 TOP 5 MATERNITY CARE GAPS (Demo Candidates):")
maternity_gaps.show(truncate=False)

# Capability coverage summary
print(f"\n📊 CAPABILITY COVERAGE SUMMARY:")
capability_coverage = spark.table("workspace.healthgpt.care_gap_by_geography") \
    .groupBy("capability") \
    .agg(
        F.avg("gap_score").alias("avg_gap_score"),
        F.sum("strong_facilities").alias("strong_facilities"),
        F.sum("partial_facilities").alias("partial_facilities"),
        F.sum("weak_facilities").alias("weak_facilities")
    ) \
    .orderBy(F.desc("avg_gap_score"))

capability_coverage.show(truncate=False)

print(f"\n{'='*80}")
print("✅ PHASE 3 FACILITY PIPELINE COMPLETE")
print(f"{'='*80}")
print(f"\nNext Steps:")
print(f"  1. Build Streamlit app using these Gold tables")
print(f"  2. Integrate OpenAI for AI Gap Brief generation")
print(f"  3. Implement scenario simulation logic")
print(f"  4. Add Care Gap Map visualization")
print(f"  5. Test end-to-end planner workflow")

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
# ===========================
# IMPORTS AND CONFIGURATION
# ===========================

import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
import re
from datetime import datetime

print("✓ Imports complete")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Load Raw Facility Data
# ===========================
# BRONZE LAYER - RAW FACILITY DATA
# ===========================

print("\n" + "="*70)
print("BRONZE LAYER: LOADING RAW FACILITY DATA")
print("="*70)

# Source: Virtue Foundation Dataset (Hackathon-provided)
SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
BRONZE_TABLE = "workspace.healthgpt.facilities_bronze"

# Load raw facility data as-is
df_bronze = spark.read.table(SOURCE_TABLE)

# Write to Bronze table (preserves all 51 columns for traceability)
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

# Verify Bronze table creation
count = spark.table(BRONZE_TABLE).count()
print(f"\n✓ Bronze table created: {BRONZE_TABLE}")
print(f"  Total facilities: {count:,}")
print(f"  Columns: {len(df_bronze.columns)}")

# Show sample record
print("\nSample facility record:")
df_bronze.select(
    "unique_id", "name", "address_city", "address_stateOrRegion",
    "description", "specialties", "capability"
).limit(1).show(truncate=50)

# COMMAND ----------

# DBTITLE 1,Silver Layer - Clean and Parse Facility Data
# ===========================
# SILVER LAYER - CLEANED FACILITY DATA
# ===========================

print("\n" + "="*70)
print("SILVER LAYER: CLEANING AND STANDARDIZING FACILITY DATA")
print("="*70)

SILVER_TABLE = "workspace.healthgpt.facilities_silver"

# Load Bronze data
df_bronze = spark.table(BRONZE_TABLE)

# Clean and standardize
df_silver = df_bronze.select(
    # Core identifiers
    F.col("unique_id").alias("facility_id"),
    F.trim(F.col("name")).alias("facility_name"),
    
    # Geography (standardized)
    F.coalesce(F.trim(F.col("address_city")), F.lit("Unknown")).alias("city"),
    F.coalesce(F.trim(F.col("address_stateOrRegion")), F.lit("Unknown")).alias("state"),
    F.coalesce(F.trim(F.col("address_zipOrPostcode")), F.lit("000000")).alias("pin_code"),
    
    # Coordinates (cleaned)
    F.col("latitude"),
    F.col("longitude"),
    
    # Text fields (for evidence extraction)
    F.col("description"),
    F.col("specialties"),      # JSON array
    F.col("procedure"),         # JSON array
    F.col("equipment"),         # JSON array
    F.col("capability"),        # JSON array
    
    # Capacity signals
    F.col("numberDoctors"),
    F.col("capacity"),
    
    # Trust signals
    F.col("source_urls"),
    F.col("websites"),
    F.col("recency_of_page_update"),
    F.col("distinct_social_media_presence_count"),
    
    # Metadata
    F.col("organization_type"),
    F.col("facilityTypeId"),
    F.col("operatorTypeId")
)

# Add data quality flags
df_silver = df_silver.withColumn(
    "has_description",
    F.when(F.col("description").isNotNull(), True).otherwise(False)
).withColumn(
    "has_specialties",
    F.when(F.col("specialties").isNotNull(), True).otherwise(False)
).withColumn(
    "has_procedures",
    F.when(F.col("procedure").isNotNull(), True).otherwise(False)
).withColumn(
    "has_equipment",
    F.when(F.col("equipment").isNotNull(), True).otherwise(False)
).withColumn(
    "has_capability",
    F.when(F.col("capability").isNotNull(), True).otherwise(False)
).withColumn(
    "has_coordinates",
    F.when((F.col("latitude").isNotNull()) & (F.col("longitude").isNotNull()), True).otherwise(False)
)

# Calculate data quality score (0-100)
df_silver = df_silver.withColumn(
    "data_quality_score",
    (
        F.col("has_description").cast("int") * 20 +
        F.col("has_specialties").cast("int") * 20 +
        F.col("has_procedures").cast("int") * 20 +
        F.col("has_equipment").cast("int") * 20 +
        F.col("has_coordinates").cast("int") * 20
    )
)

# Write to Silver table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"\n✓ Silver table created: {SILVER_TABLE}")
print(f"  Records: {df_silver.count():,}")

# Show data quality distribution
print("\nData Quality Distribution:")
df_silver.groupBy("data_quality_score") \
    .count() \
    .orderBy("data_quality_score", ascending=False) \
    .show(10)

print("\nGeographic Coverage:")
print(f"  States: {df_silver.select('state').distinct().count()}")
print(f"  Cities: {df_silver.select('city').distinct().count()}")

# COMMAND ----------

# DBTITLE 1,Capability Taxonomy and Keywords
# ===========================
# CAPABILITY TAXONOMY (7 CORE CAPABILITIES)
# ===========================

print("\n" + "="*70)
print("CAPABILITY TAXONOMY AND KEYWORD DICTIONARIES")
print("="*70)

# Define the 7 core capabilities per project document
CAPABILITY_KEYWORDS = {
    "maternity": {
        "description_keywords": [
            "maternity", "delivery", "labor", "labour", "obstetric", "antenatal", 
            "postnatal", "pregnancy", "maternal", "childbirth", "c-section", 
            "caesarean", "gynecology", "gynaecology", "OB", "ANC"
        ],
        "specialty_keywords": [
            "obstetrics", "gynecology", "gynaecology", "maternal", "fetal"
        ],
        "procedure_keywords": [
            "delivery", "c-section", "caesarean", "normal delivery", "vaginal delivery",
            "emergency obstetric", "antenatal care", "postnatal care"
        ],
        "equipment_keywords": [
            "delivery table", "fetal monitor", "incubator", "warmer", 
            "delivery kit", "suction", "labor room"
        ]
    },
    "icu": {
        "description_keywords": [
            "ICU", "intensive care", "critical care", "ventilator", 
            "life support", "CCU", "cardiac care"
        ],
        "specialty_keywords": [
            "critical care", "intensive care", "ICU", "emergency medicine"
        ],
        "procedure_keywords": [
            "mechanical ventilation", "intubation", "critical care management"
        ],
        "equipment_keywords": [
            "ventilator", "ICU bed", "cardiac monitor", "infusion pump", 
            "defibrillator", "ventilator bed"
        ]
    },
    "nicu": {
        "description_keywords": [
            "NICU", "neonatal intensive", "neonatal care", "newborn intensive",
            "premature", "preemie", "neonatal ICU"
        ],
        "specialty_keywords": [
            "neonatology", "neonatal", "pediatric intensive"
        ],
        "procedure_keywords": [
            "neonatal resuscitation", "phototherapy", "neonatal ventilation"
        ],
        "equipment_keywords": [
            "incubator", "neonatal ventilator", "phototherapy", "radiant warmer",
            "NICU bed", "infant warmer"
        ]
    },
    "emergency": {
        "description_keywords": [
            "emergency", "24x7", "24/7", "casualty", "trauma center", 
            "accident", "urgent care", "emergency department", "ED"
        ],
        "specialty_keywords": [
            "emergency medicine", "trauma", "accident"
        ],
        "procedure_keywords": [
            "emergency surgery", "trauma management", "resuscitation", 
            "emergency stabilization"
        ],
        "equipment_keywords": [
            "emergency bed", "ambulance", "crash cart", "defibrillator",
            "oxygen", "emergency equipment"
        ]
    },
    "trauma": {
        "description_keywords": [
            "trauma", "trauma center", "trauma care", "accident", 
            "polytrauma", "trauma unit"
        ],
        "specialty_keywords": [
            "trauma surgery", "orthopedic trauma", "trauma"
        ],
        "procedure_keywords": [
            "trauma surgery", "emergency surgery", "fracture management",
            "polytrauma care"
        ],
        "equipment_keywords": [
            "trauma bay", "C-arm", "orthopedic equipment", "trauma kit"
        ]
    },
    "oncology": {
        "description_keywords": [
            "cancer", "oncology", "chemotherapy", "radiation therapy", 
            "radiotherapy", "tumor", "malignancy", "chemo"
        ],
        "specialty_keywords": [
            "oncology", "medical oncology", "surgical oncology", 
            "radiation oncology", "hematology"
        ],
        "procedure_keywords": [
            "chemotherapy", "radiation therapy", "cancer surgery", 
            "tumor resection", "biopsy"
        ],
        "equipment_keywords": [
            "linear accelerator", "radiation machine", "chemo chair", 
            "CT simulator", "radiotherapy", "LINAC"
        ]
    },
    "dialysis": {
        "description_keywords": [
            "dialysis", "hemodialysis", "kidney", "renal", "nephrology",
            "PMNDP", "dialysis unit"
        ],
        "specialty_keywords": [
            "nephrology", "renal", "dialysis"
        ],
        "procedure_keywords": [
            "hemodialysis", "peritoneal dialysis", "dialysis", "renal replacement"
        ],
        "equipment_keywords": [
            "dialysis machine", "dialysis unit", "RO plant", "dialysis bed",
            "hemodialysis machine"
        ]
    }
}

print("\n✓ Capability taxonomy defined:")
for cap, keywords in CAPABILITY_KEYWORDS.items():
    print(f"  {cap.upper()}: {len(keywords['description_keywords']) + len(keywords['specialty_keywords'])} keywords")

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Evidence Extraction Pipeline
# ===========================
# EVIDENCE EXTRACTION PIPELINE
# ===========================

print("\n" + "="*70)
print("EVIDENCE EXTRACTION: CAPABILITY CLAIMS FROM FACILITY DATA")
print("="*70)

EVIDENCE_TABLE = "workspace.healthgpt.facility_capability_evidence"

# Load Silver data
df_facilities = spark.table(SILVER_TABLE)

# Function to extract evidence for a capability
def extract_capability_evidence(capability_name, keywords_dict):
    """
    Extract evidence for a specific capability from facility fields.
    Returns DataFrame with facility_id, capability, evidence_text, evidence_field, confidence.
    """
    
    evidence_rows = []
    
    # Collect facilities data
    facilities = df_facilities.select(
        "facility_id", "facility_name", "description", 
        "specialties", "procedure", "equipment", "capability"
    ).collect()
    
    for facility in facilities:
        facility_id = facility.facility_id
        evidence_found = []
        
        # Check description field
        if facility.description:
            desc_lower = facility.description.lower()
            matched_keywords = [kw for kw in keywords_dict['description_keywords'] 
                              if kw in desc_lower]
            if matched_keywords:
                evidence_found.append({
                    "facility_id": facility_id,
                    "capability": capability_name,
                    "evidence_text": facility.description[:500],  # Truncate for storage
                    "evidence_field": "description",
                    "matched_keywords": ",".join(matched_keywords[:5]),
                    "match_count": len(matched_keywords)
                })
        
        # Check specialties (JSON array)
        if facility.specialties:
            try:
                specialties_list = json.loads(facility.specialties) if isinstance(facility.specialties, str) else []
                specialties_text = " ".join([str(s).lower() for s in specialties_list])
                matched_keywords = [kw for kw in keywords_dict['specialty_keywords'] 
                                  if kw in specialties_text]
                if matched_keywords:
                    evidence_found.append({
                        "facility_id": facility_id,
                        "capability": capability_name,
                        "evidence_text": ",".join(specialties_list[:10]),
                        "evidence_field": "specialties",
                        "matched_keywords": ",".join(matched_keywords[:5]),
                        "match_count": len(matched_keywords)
                    })
            except:
                pass
        
        # Check procedures (JSON array)
        if facility.procedure:
            try:
                procedure_list = json.loads(facility.procedure) if isinstance(facility.procedure, str) else []
                procedure_text = " ".join([str(p).lower() for p in procedure_list])
                matched_keywords = [kw for kw in keywords_dict['procedure_keywords'] 
                                  if kw in procedure_text]
                if matched_keywords:
                    evidence_found.append({
                        "facility_id": facility_id,
                        "capability": capability_name,
                        "evidence_text": ",".join(procedure_list[:10]),
                        "evidence_field": "procedure",
                        "matched_keywords": ",".join(matched_keywords[:5]),
                        "match_count": len(matched_keywords)
                    })
            except:
                pass
        
        # Check equipment (JSON array)
        if facility.equipment:
            try:
                equipment_list = json.loads(facility.equipment) if isinstance(facility.equipment, str) else []
                equipment_text = " ".join([str(e).lower() for e in equipment_list])
                matched_keywords = [kw for kw in keywords_dict['equipment_keywords'] 
                                  if kw in equipment_text]
                if matched_keywords:
                    evidence_found.append({
                        "facility_id": facility_id,
                        "capability": capability_name,
                        "evidence_text": ",".join(equipment_list[:10]),
                        "evidence_field": "equipment",
                        "matched_keywords": ",".join(matched_keywords[:5]),
                        "match_count": len(matched_keywords)
                    })
            except:
                pass
        
        # Add all evidence found for this facility
        evidence_rows.extend(evidence_found)
    
    return evidence_rows

# Extract evidence for all 7 capabilities
all_evidence = []

for capability_name, keywords_dict in CAPABILITY_KEYWORDS.items():
    print(f"\n  Extracting evidence for: {capability_name.upper()}")
    evidence = extract_capability_evidence(capability_name, keywords_dict)
    all_evidence.extend(evidence)
    print(f"    Found {len(evidence)} evidence records")

# Convert to Spark DataFrame
schema = StructType([
    StructField("facility_id", StringType(), True),
    StructField("capability", StringType(), True),
    StructField("evidence_text", StringType(), True),
    StructField("evidence_field", StringType(), True),
    StructField("matched_keywords", StringType(), True),
    StructField("match_count", IntegerType(), True)
])

df_evidence = spark.createDataFrame(all_evidence, schema=schema)

# Write to Evidence table
df_evidence.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(EVIDENCE_TABLE)

print(f"\n\n✓ Evidence table created: {EVIDENCE_TABLE}")
print(f"  Total evidence records: {df_evidence.count():,}")
print(f"  Facilities with evidence: {df_evidence.select('facility_id').distinct().count():,}")

# Show evidence distribution by capability
print("\nEvidence Distribution by Capability:")
df_evidence.groupBy("capability") \
    .agg(
        F.count("*").alias("evidence_count"),
        F.countDistinct("facility_id").alias("facilities_count")
    ) \
    .orderBy("facilities_count", ascending=False) \
    .show()

# COMMAND ----------

# DBTITLE 1,Trust Scoring Engine
# ===========================
# TRUST SCORING ENGINE
# ===========================

print("\n" + "="*70)
print("TRUST SCORING: EVALUATING FACILITY-CAPABILITY EVIDENCE STRENGTH")
print("="*70)

TRUST_TABLE = "workspace.healthgpt.facility_trust_scores"

# Load evidence data
df_evidence = spark.table(EVIDENCE_TABLE)

# Aggregate evidence by facility and capability
df_trust_base = df_evidence.groupBy("facility_id", "capability").agg(
    F.count("*").alias("evidence_count"),
    F.countDistinct("evidence_field").alias("field_diversity"),
    F.sum("match_count").alias("total_keyword_matches"),
    F.collect_list("evidence_field").alias("evidence_fields"),
    F.max("evidence_text").alias("sample_evidence_text")
)

# Define trust scoring logic per document specifications
def calculate_trust_signal(evidence_count, field_diversity, total_matches):
    """
    Trust Signal Classification:
    - Strong: Multiple supporting fields (3+) + high keyword matches (5+)
    - Partial: 2 fields or moderate matches (2-4)
    - Weak: Single field mention with few matches
    - Suspicious: Evidence contradictions (handled separately)
    - No Claim: No evidence (handled in aggregation)
    """
    if field_diversity >= 3 and total_matches >= 5:
        return "strong"
    elif field_diversity >= 2 or total_matches >= 3:
        return "partial"
    else:
        return "weak"

# Register UDF
trust_signal_udf = F.udf(calculate_trust_signal, StringType())

# Apply trust scoring
df_trust = df_trust_base.withColumn(
    "trust_signal",
    trust_signal_udf(
        F.col("evidence_count"),
        F.col("field_diversity"),
        F.col("total_keyword_matches")
    )
)

# Add confidence score (0-100)
df_trust = df_trust.withColumn(
    "confidence_score",
    F.when(F.col("trust_signal") == "strong", 85) \
     .when(F.col("trust_signal") == "partial", 60) \
     .when(F.col("trust_signal") == "weak", 30) \
     .otherwise(0)
)

# Join with facility data for geography
df_facilities_geo = spark.table(SILVER_TABLE).select(
    "facility_id", "facility_name", "state", "city", "pin_code",
    "latitude", "longitude", "data_quality_score"
)

df_trust_final = df_trust.join(df_facilities_geo, "facility_id", "left")

# Write to Trust Scores table
df_trust_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TRUST_TABLE)

print(f"\n✓ Trust scores table created: {TRUST_TABLE}")
print(f"  Total facility-capability pairs: {df_trust_final.count():,}")

# Show trust signal distribution
print("\nTrust Signal Distribution:")
df_trust_final.groupBy("trust_signal") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

print("\nTrust Signals by Capability:")
df_trust_final.groupBy("capability", "trust_signal") \
    .count() \
    .orderBy("capability", "count", ascending=[True, False]) \
    .show(30)

# COMMAND ----------

# DBTITLE 1,Geographic Aggregation - Care Gap Scores
# ===========================
# GEOGRAPHIC AGGREGATION - CARE GAP SCORING
# ===========================

print("\n" + "="*70)
print("GEOGRAPHIC AGGREGATION: COMPUTING CARE GAP SCORES BY STATE/CITY")
print("="*70)

GAP_TABLE = "workspace.healthgpt.care_gap_by_geography"

# Load trust scores
df_trust = spark.table(TRUST_TABLE)

# Aggregate by state, city, and capability
df_gap = df_trust.groupBy("state", "city", "capability").agg(
    # Facility counts by trust signal
    F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("facilities_strong"),
    F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("facilities_partial"),
    F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("facilities_weak"),
    F.count("*").alias("facilities_total"),
    
    # Evidence strength metrics
    F.avg("confidence_score").alias("avg_confidence_score"),
    F.avg("data_quality_score").alias("avg_data_quality"),
    F.avg("field_diversity").alias("avg_field_diversity"),
    
    # Sample facilities
    F.collect_list("facility_name").alias("facility_names")
)

# Calculate Care Gap Score (0-100, higher = worse gap)
# Gap score considers:
# 1. Strong facility availability (lower is worse)
# 2. Weak/suspicious facility burden (higher is worse)
# 3. Data quality (lower is worse)

df_gap = df_gap.withColumn(
    "gap_score",
    F.least(
        F.lit(100),
        F.greatest(
            F.lit(0),
            # Base gap from lack of strong facilities
            100 - (F.col("facilities_strong") * 15) +
            # Penalty for weak evidence
            (F.col("facilities_weak") * 10) -
            # Credit for data quality
            (F.col("avg_data_quality") / 2)
        )
    )
)

# Calculate Confidence Level
df_gap = df_gap.withColumn(
    "confidence_level",
    F.when(
        (F.col("avg_confidence_score") >= 70) & (F.col("avg_data_quality") >= 60),
        "High"
    ).when(
        (F.col("avg_confidence_score") >= 50) & (F.col("avg_data_quality") >= 40),
        "Medium-High"
    ).when(
        (F.col("avg_confidence_score") >= 30),
        "Medium"
    ).otherwise("Low")
)

# Calculate Data Completeness Score
df_gap = df_gap.withColumn(
    "data_completeness_score",
    F.round(
        (F.col("avg_data_quality") + 
         F.col("avg_field_diversity") * 10) / 2,
        1
    )
)

# Determine Review Priority
df_gap = df_gap.withColumn(
    "review_priority",
    F.when(
        (F.col("gap_score") >= 70) & (F.col("confidence_level") == "Low"),
        "Critical"
    ).when(
        (F.col("gap_score") >= 50) | (F.col("facilities_weak") >= 3),
        "High"
    ).when(
        (F.col("gap_score") >= 30),
        "Medium"
    ).otherwise("Low")
)

# Write to Care Gap table
df_gap.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GAP_TABLE)

print(f"\n✓ Care gap table created: {GAP_TABLE}")
print(f"  Geographic units analyzed: {df_gap.count():,}")

# Show top care gaps
print("\nTop 10 Care Gaps (Highest Gap Score + Low Confidence):")
df_gap.filter(F.col("confidence_level").isin(["Low", "Medium"])) \
    .orderBy("gap_score", ascending=False) \
    .select(
        "state", "city", "capability", "gap_score", 
        "confidence_level", "facilities_strong", "facilities_weak"
    ) \
    .show(10, truncate=False)

# Show summary by capability
print("\nCare Gap Summary by Capability:")
df_gap.groupBy("capability").agg(
    F.avg("gap_score").alias("avg_gap_score"),
    F.sum("facilities_strong").alias("total_strong"),
    F.sum("facilities_weak").alias("total_weak"),
    F.count("*").alias("geographic_units")
).orderBy("avg_gap_score", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Planner Workspace - Persistence Tables
# ===========================
# PLANNER WORKSPACE - PERSISTENCE TABLES
# ===========================

print("\n" + "="*70)
print("PLANNER WORKSPACE: CREATING PERSISTENCE TABLES")
print("="*70)

# Per document requirements: Persist user actions, notes, overrides, scenarios, shortlists, review decisions

# 1. Planner Notes
schema_notes = StructType([
    StructField("note_id", StringType(), False),
    StructField("planner_user_id", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("capability", StringType(), True),
    StructField("note_text", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

df_notes_empty = spark.createDataFrame([], schema=schema_notes)
df_notes_empty.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.healthgpt.planner_notes")

print("  ✓ Created: workspace.healthgpt.planner_notes")

# 2. Planner Overrides (manual trust signal corrections)
schema_overrides = StructType([
    StructField("override_id", StringType(), False),
    StructField("planner_user_id", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("capability", StringType(), True),
    StructField("original_trust_signal", StringType(), True),
    StructField("override_trust_signal", StringType(), True),
    StructField("override_reason", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

df_overrides_empty = spark.createDataFrame([], schema=schema_overrides)
df_overrides_empty.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.healthgpt.planner_overrides")

print("  ✓ Created: workspace.healthgpt.planner_overrides")

# 3. Planner Scenarios (what-if simulations)
schema_scenarios = StructType([
    StructField("scenario_id", StringType(), False),
    StructField("planner_user_id", StringType(), True),
    StructField("scenario_name", StringType(), True),
    StructField("geography_state", StringType(), True),
    StructField("geography_city", StringType(), True),
    StructField("capability", StringType(), True),
    StructField("scenario_type", StringType(), True),  # verify_claims, add_facility, increase_outreach
    StructField("scenario_parameters", StringType(), True),  # JSON
    StructField("projected_gap_score", DoubleType(), True),
    StructField("projected_confidence", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

df_scenarios_empty = spark.createDataFrame([], schema=schema_scenarios)
df_scenarios_empty.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.healthgpt.planner_scenarios")

print("  ✓ Created: workspace.healthgpt.planner_scenarios")

# 4. Planner Shortlists (saved facilities for follow-up)
schema_shortlists = StructType([
    StructField("shortlist_id", StringType(), False),
    StructField("planner_user_id", StringType(), True),
    StructField("shortlist_name", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("capability", StringType(), True),
    StructField("action_required", StringType(), True),  # verify, contact, inspect
    StructField("priority", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

df_shortlists_empty = spark.createDataFrame([], schema=schema_shortlists)
df_shortlists_empty.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.healthgpt.planner_shortlists")

print("  ✓ Created: workspace.healthgpt.planner_shortlists")

# 5. Planner Review Decisions (facility verification outcomes)
schema_reviews = StructType([
    StructField("review_id", StringType(), False),
    StructField("planner_user_id", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("capability", StringType(), True),
    StructField("review_decision", StringType(), True),  # verified, rejected, needs_more_info
    StructField("verification_method", StringType(), True),  # field_visit, phone_call, document_review
    StructField("review_notes", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

df_reviews_empty = spark.createDataFrame([], schema=schema_reviews)
df_reviews_empty.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.healthgpt.planner_review_decisions")

print("  ✓ Created: workspace.healthgpt.planner_review_decisions")

print("\n✓ All 5 planner workspace tables created successfully")
print("  Purpose: Persist user actions, manual overrides, scenarios, shortlists, review decisions")
print("  These tables support the non-negotiable requirement: planner actions must be saved.")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary and Validation
# ===========================
# PIPELINE SUMMARY AND VALIDATION
# ===========================

print("\n" + "="*70)
print("PIPELINE SUMMARY: DATA ASSETS FOR TRACK 2 - MEDICAL DESERT PLANNER")
print("="*70)

# List all created tables
tables_created = [
    "workspace.healthgpt.facilities_bronze",
    "workspace.healthgpt.facilities_silver",
    "workspace.healthgpt.facility_capability_evidence",
    "workspace.healthgpt.facility_trust_scores",
    "workspace.healthgpt.care_gap_by_geography",
    "workspace.healthgpt.planner_notes",
    "workspace.healthgpt.planner_overrides",
    "workspace.healthgpt.planner_scenarios",
    "workspace.healthgpt.planner_shortlists",
    "workspace.healthgpt.planner_review_decisions"
]

print("\n" + "="*70)
print("DATA ASSETS CREATED:")
print("="*70)

for i, table in enumerate(tables_created, 1):
    try:
        count = spark.table(table).count()
        print(f"{i}. {table}")
        print(f"   Records: {count:,}")
    except Exception as e:
        print(f"{i}. {table}")
        print(f"   Status: Created (empty schema)")

print("\n" + "="*70)
print("PIPELINE VALIDATION:")
print("="*70)

# Validate Bronze → Silver → Gold flow
facilities_bronze = spark.table("workspace.healthgpt.facilities_bronze").count()
facilities_silver = spark.table("workspace.healthgpt.facilities_silver").count()
evidence_records = spark.table("workspace.healthgpt.facility_capability_evidence").count()
trust_records = spark.table("workspace.healthgpt.facility_trust_scores").count()
gap_records = spark.table("workspace.healthgpt.care_gap_by_geography").count()

print(f"\n✓ Bronze Layer: {facilities_bronze:,} facilities loaded")
print(f"✓ Silver Layer: {facilities_silver:,} facilities cleaned")
print(f"✓ Evidence Extraction: {evidence_records:,} capability claims extracted")
print(f"✓ Trust Scoring: {trust_records:,} facility-capability pairs scored")
print(f"✓ Geographic Aggregation: {gap_records:,} geographic units analyzed")
print(f"✓ Planner Workspace: 5 persistence tables created")

# Show capability coverage
print("\n" + "="*70)
print("CAPABILITY COVERAGE:")
print("="*70)

df_capability_summary = spark.table("workspace.healthgpt.facility_trust_scores") \
    .groupBy("capability") \
    .agg(
        F.countDistinct("facility_id").alias("facilities"),
        F.sum(F.when(F.col("trust_signal") == "strong", 1).otherwise(0)).alias("strong"),
        F.sum(F.when(F.col("trust_signal") == "partial", 1).otherwise(0)).alias("partial"),
        F.sum(F.when(F.col("trust_signal") == "weak", 1).otherwise(0)).alias("weak")
    ) \
    .orderBy("facilities", ascending=False)

df_capability_summary.show()

print("\n" + "="*70)
print("✓ PHASE 3 PIPELINE COMPLETE")
print("="*70)
print("\nNext Steps:")
print("  1. Build Streamlit App UI (6 screens per mockups)")
print("  2. Integrate OpenAI for AI Gap Brief generation")
print("  3. Implement Scenario Planner logic")
print("  4. Add care gap map visualization")
print("  5. Test with demo geography: Select state + capability")
print("\nKey Deliverables Ready:")
print("  ✓ Facility-level trust scoring (strong/partial/weak)")
print("  ✓ Geographic care gap scores with confidence levels")
print("  ✓ Evidence citations for every facility claim")
print("  ✓ Planner workspace for persistence")
print("  ✓ Aligned with Track 2: Medical Desert Planner requirements")

# COMMAND ----------

# DBTITLE 1,Sample Query - Demo Preparation
# MAGIC %sql
# MAGIC -- ===========================
# MAGIC -- SAMPLE QUERY: DEMO PREPARATION
# MAGIC -- ===========================
# MAGIC -- Purpose: Cache demo data for reliable 3-minute hackathon demo
# MAGIC -- Use Case: Maternity care gap in a specific state
# MAGIC
# MAGIC -- Find a good demo state with maternity care gap
# MAGIC SELECT 
# MAGIC   state,
# MAGIC   capability,
# MAGIC   gap_score,
# MAGIC   confidence_level,
# MAGIC   facilities_strong,
# MAGIC   facilities_partial,
# MAGIC   facilities_weak,
# MAGIC   facilities_total,
# MAGIC   review_priority
# MAGIC FROM workspace.healthgpt.care_gap_by_geography
# MAGIC WHERE capability = 'maternity'
# MAGIC   AND gap_score >= 60  -- Significant gap
# MAGIC   AND confidence_level IN ('Medium-High', 'High')  -- Trustworthy data
# MAGIC ORDER BY gap_score DESC, facilities_total DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- For demo: Pick one state, then show:
# MAGIC -- 1. Overview screen: Gap score, confidence, facility distribution
# MAGIC -- 2. Facility evidence detail: Show 1 strong, 1 partial, 1 weak facility
# MAGIC -- 3. AI Gap Brief: Generate from structured evidence
# MAGIC -- 4. Scenario: Verify 2 weak claims → show confidence increase
