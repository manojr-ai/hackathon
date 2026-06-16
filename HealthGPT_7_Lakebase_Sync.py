# Databricks notebook source
# ============================================================================
# HealthGPT_7_Lakebase_Sync.py
# Syncs Delta tables → Lakebase (PostgreSQL) using psycopg2 + OAuth tokens
# psycopg2 is pre-installed in Databricks — no pip install needed
# ============================================================================

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import psycopg2
import pandas as pd
import numpy as np
import requests
from pyspark.sql import functions as F
import time

print("=" * 70)
print("HealthGPT Lakebase Sync — Step 7")
print("=" * 70)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LAKEBASE_HOST = "ep-wild-snow-d8k94scg.database.us-east-2.cloud.databricks.com"
LAKEBASE_PORT = 5432
LAKEBASE_DB = "healthgpt"
LAKEBASE_USER = "manoj.rayalla@acuitybrands.com"
ENDPOINT_NAME = "projects/hackthon/branches/production/endpoints/primary"
BATCH_SIZE = 500  # rows per PostgreSQL commit batch

print(f"\nTarget host : {LAKEBASE_HOST}")
print(f"Database    : {LAKEBASE_DB}")
print(f"Endpoint    : {ENDPOINT_NAME}")
print(f"Batch size  : {BATCH_SIZE}")

# COMMAND ----------

# DBTITLE 1,Token Generation and Connection Helpers


def get_lakebase_token():
    """
    Generate an OAuth/JWT token for Lakebase (valid ~1 hour).

    Strategy:
    1. REST API /api/2.0/lakebase/credentials/generate  (most reliable)
       — uses this notebook's Databricks PAT as the Authorization header
       — returns a proper Lakebase JWT usable as the PostgreSQL password
    2. SDK w.postgres.generate_database_credential()    (SDK v0.40+ only)
    3. SDK w.api_client REST fallback
    """
    # Grab the notebook's host + PAT from dbutils context
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_pat = ctx.apiToken().get()
    # browserHostName() returns None in serverless job contexts; use apiUrl() with fallback
    try:
        workspace_host = ctx.apiUrl().get()
        if not workspace_host.startswith("https://"):
            workspace_host = "https://" + workspace_host
    except Exception:
        workspace_host = "https://dbc-f49e9aec-67ba.cloud.databricks.com"

    # ── Attempt 1: correct REST API endpoint ─────────────────────────────
    try:
        resp = requests.post(
            f"{workspace_host}/api/2.0/lakebase/credentials/generate",
            headers={
                "Authorization": f"Bearer {notebook_pat}",
                "Content-Type": "application/json",
            },
            json={"endpoint": ENDPOINT_NAME},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            tok = data.get("token") or data.get("access_token")
            if tok:
                print("  Token obtained via /api/2.0/lakebase/credentials/generate")
                return tok
        print(f"  REST attempt 1 returned {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"  REST attempt 1 error: {e}")

    # ── Attempt 2: alternate REST paths ──────────────────────────────────
    for path in [
        "/api/2.0/postgres/credentials",
        "/api/2.0/lakebase/credentials",
    ]:
        try:
            resp = requests.post(
                f"{workspace_host}{path}",
                headers={
                    "Authorization": f"Bearer {notebook_pat}",
                    "Content-Type": "application/json",
                },
                json={"endpoint": ENDPOINT_NAME},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                tok = data.get("token") or data.get("access_token")
                if tok:
                    print(f"  Token obtained via {path}")
                    return tok
        except Exception:
            pass

    # ── Attempt 3: SDK w.postgres (v0.40+ only) ───────────────────────────
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        cred = w.postgres.generate_database_credential(endpoint_name=ENDPOINT_NAME)
        tok = cred.token
        if tok:
            print("  Token obtained via SDK w.postgres.generate_database_credential")
            return tok
    except Exception as e:
        print(f"  SDK attempt failed: {e}")

    raise RuntimeError(
        "Could not obtain a Lakebase JWT token via any method.\n"
        "Verify that the endpoint exists and this compute has access:\n"
        f"  {ENDPOINT_NAME}"
    )


def get_connection(token=None):
    """Open a psycopg2 connection to Lakebase."""
    if token is None:
        token = get_lakebase_token()
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB,
        user=LAKEBASE_USER,
        password=token,
        sslmode="require",
        connect_timeout=30,
    )


def clean_row(row):
    """Convert NaN / numpy scalars to Python-native types for psycopg2.
    Also strips NUL bytes (\\x00) from strings — PostgreSQL rejects them.
    """
    result = []
    for v in row:
        if v is None:
            result.append(None)
        elif isinstance(v, float) and (v != v):  # NaN check
            result.append(None)
        elif hasattr(v, "item"):  # numpy scalar → Python
            cleaned = v.item()
            if isinstance(cleaned, str):
                cleaned = cleaned.replace("\x00", "")
            result.append(cleaned)
        elif isinstance(v, str):
            result.append(v.replace("\x00", ""))
        else:
            try:
                if pd.isna(v):
                    result.append(None)
                    continue
            except (TypeError, ValueError):
                pass
            result.append(v)
    return tuple(result)


def sync_to_postgres(spark_df, insert_sql, label):
    """
    Collect Spark DataFrame into the driver as a pandas DataFrame, then
    batch-insert into PostgreSQL in BATCH_SIZE chunks.

    NOTE: .offset() is NOT supported on Spark DataFrames in Python.
          We collect everything to pandas first (tables are small enough).
    """
    t0 = time.time()
    pdf = spark_df.toPandas()
    total = len(pdf)
    print(f"  {total:,} rows collected from Delta ({time.time() - t0:.1f}s)")

    token = get_lakebase_token()
    processed = 0

    with get_connection(token) as conn:
        with conn.cursor() as cur:
            rows = [clean_row(r) for r in pdf.itertuples(index=False, name=None)]
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                cur.executemany(insert_sql, batch)
                conn.commit()
                processed += len(batch)
                print(f"    {processed:,}/{total:,} rows  ({time.time() - t0:.0f}s)")

    elapsed = time.time() - t0
    print(f"  {label}: {processed:,} rows synced in {elapsed:.1f}s")
    return processed, elapsed


print("Helper functions defined.")

# COMMAND ----------

# DBTITLE 1,DDL — Create Tables and Indexes
# ---------------------------------------------------------------------------
# Create target tables and indexes in Lakebase if they don't exist yet
# ---------------------------------------------------------------------------
DDL_STATEMENTS = [
    # ── care_gap_summary ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS care_gap_summary (
        state                VARCHAR(100) NOT NULL,
        capability           VARCHAR(50)  NOT NULL,
        gap_score            DECIMAL(5,2),
        gap_severity         VARCHAR(20),
        confidence_level     VARCHAR(20),
        intervention_urgency VARCHAR(20),
        total_facilities     INT,
        strong_count         INT,
        partial_count        INT,
        weak_count           INT,
        strong_pct           DECIMAL(5,2),
        ml_gap_score         DECIMAL(5,2),
        underserved_pop      BIGINT,
        supply_adequacy      VARCHAR(20),
        city                 VARCHAR(100),
        geography_type       VARCHAR(20),
        PRIMARY KEY (state, capability)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_cgs_gap_score  ON care_gap_summary(gap_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_cgs_severity   ON care_gap_summary(gap_severity)",
    "CREATE INDEX IF NOT EXISTS idx_cgs_state      ON care_gap_summary(state)",
    "CREATE INDEX IF NOT EXISTS idx_cgs_capability ON care_gap_summary(capability)",
    # ── facility_detail ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS facility_detail (
        facility_id              VARCHAR(100) NOT NULL,
        facility_name            VARCHAR(255),
        state                    VARCHAR(100),
        city                     VARCHAR(100),
        capability               VARCHAR(50)  NOT NULL,
        rule_trust_signal        VARCHAR(20),
        rule_confidence          INT,
        ml_trust_signal          VARCHAR(20),
        ml_trust_score           INT,
        ai_probability           DECIMAL(5,4),
        ml_capability_confidence VARCHAR(20),
        dual_score_agreement     VARCHAR(20),
        trust_delta              INT,
        score_divergence         VARCHAR(20),
        review_priority          VARCHAR(20),
        evidence_count           INT,
        latitude                 DECIMAL(10,6),
        longitude                DECIMAL(10,6),
        data_quality             VARCHAR(20),
        PRIMARY KEY (facility_id, capability)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fd_state     ON facility_detail(state)",
    "CREATE INDEX IF NOT EXISTS idx_fd_city      ON facility_detail(city)",
    "CREATE INDEX IF NOT EXISTS idx_fd_review    ON facility_detail(review_priority)",
    "CREATE INDEX IF NOT EXISTS idx_fd_trust     ON facility_detail(ml_trust_signal)",
    "CREATE INDEX IF NOT EXISTS idx_fd_state_cap ON facility_detail(state, capability)",
    "CREATE INDEX IF NOT EXISTS idx_fd_geo       ON facility_detail(latitude, longitude)",
    # ── risk_indicators ───────────────────────────────────────────────────
    # NOTE: no 'description' column — source table gap_risk_indicators does
    #       not have that column (confirmed cols: indicator_name, priority,
    #       risk_pct, indicator_id, analysis_date)
    """
    CREATE TABLE IF NOT EXISTS risk_indicators (
        indicator_name VARCHAR(255) PRIMARY KEY,
        risk_pct       DECIMAL(5,2),
        priority       VARCHAR(20)
    )
    """,
]

print("Applying DDL …")
token = get_lakebase_token()
with get_connection(token) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt.strip())
print("  All tables and indexes created (or already exist).")

# COMMAND ----------

# DBTITLE 1,Sync Table 1: care_gap_summary
# ---------------------------------------------------------------------------
# Table 1: care_gap_summary  (923 rows expected)
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("Syncing Table 1: care_gap_summary")
print("=" * 70)

df1 = spark.table("workspace.healthgpt.care_gap_silver_ml").select(
    "state",
    "capability",
    F.col("composite_gap_score").cast("decimal(5,2)").alias("gap_score"),
    "gap_severity",
    "confidence_level",
    "intervention_urgency",
    F.col("total_facilities").cast("int"),
    F.col("strong_count").cast("int"),
    F.col("partial_count").cast("int"),
    F.col("weak_count").cast("int"),
    F.col("strong_pct").cast("decimal(5,2)"),
    F.col("ml_gap_score").cast("decimal(5,2)"),
    F.col("estimated_underserved_population").alias("underserved_pop"),
    "supply_adequacy",
    "city",
    "geography_type",
)

INSERT_GAP_SQL = """
    INSERT INTO care_gap_summary
        (state, capability, gap_score, gap_severity, confidence_level,
         intervention_urgency, total_facilities, strong_count, partial_count,
         weak_count, strong_pct, ml_gap_score, underserved_pop, supply_adequacy,
         city, geography_type)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (state, capability) DO UPDATE SET
        gap_score            = EXCLUDED.gap_score,
        gap_severity         = EXCLUDED.gap_severity,
        confidence_level     = EXCLUDED.confidence_level,
        intervention_urgency = EXCLUDED.intervention_urgency,
        total_facilities     = EXCLUDED.total_facilities,
        strong_count         = EXCLUDED.strong_count,
        partial_count        = EXCLUDED.partial_count,
        weak_count           = EXCLUDED.weak_count,
        strong_pct           = EXCLUDED.strong_pct,
        ml_gap_score         = EXCLUDED.ml_gap_score,
        underserved_pop      = EXCLUDED.underserved_pop,
        supply_adequacy      = EXCLUDED.supply_adequacy,
        city                 = EXCLUDED.city,
        geography_type       = EXCLUDED.geography_type
"""

sync_to_postgres(df1, INSERT_GAP_SQL, "care_gap_summary")

# COMMAND ----------

# DBTITLE 1,Sync Table 2: facility_detail
# ---------------------------------------------------------------------------
# Table 2: facility_detail  (27,978 rows expected)
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("Syncing Table 2: facility_detail")
print("=" * 70)

_fd_cols = [
    c.lower() for c in spark.table("workspace.healthgpt.facilities_silver_ml").columns
]

df2 = spark.table("workspace.healthgpt.facilities_silver_ml").select(
    "facility_id",
    "facility_name",
    "state",
    "city",
    "capability",
    "rule_trust_signal",
    F.col("rule_confidence_score").cast("int").alias("rule_confidence"),
    "ml_trust_signal",
    F.col("ml_trust_score").cast("int"),
    F.col("ml_capability_probability").cast("decimal(5,4)").alias("ai_probability"),
    F.col("ml_capability_confidence")
    if "ml_capability_confidence" in _fd_cols
    else F.lit(None).cast("string").alias("ml_capability_confidence"),
    "dual_score_agreement",
    F.col("trust_delta").cast("int"),
    F.col("score_divergence")
    if "score_divergence" in _fd_cols
    else F.lit(None).cast("string").alias("score_divergence"),
    "review_priority",
    F.col("evidence_count").cast("int"),
    F.col("latitude").cast("decimal(10,6)"),
    F.col("longitude").cast("decimal(10,6)"),
    "data_quality",
)

INSERT_FACILITY_SQL = """
    INSERT INTO facility_detail
        (facility_id, facility_name, state, city, capability,
         rule_trust_signal, rule_confidence, ml_trust_signal, ml_trust_score,
         ai_probability, ml_capability_confidence, dual_score_agreement,
         trust_delta, score_divergence, review_priority, evidence_count,
         latitude, longitude, data_quality)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (facility_id, capability) DO UPDATE SET
        facility_name            = EXCLUDED.facility_name,
        state                    = EXCLUDED.state,
        city                     = EXCLUDED.city,
        rule_trust_signal        = EXCLUDED.rule_trust_signal,
        rule_confidence          = EXCLUDED.rule_confidence,
        ml_trust_signal          = EXCLUDED.ml_trust_signal,
        ml_trust_score           = EXCLUDED.ml_trust_score,
        ai_probability           = EXCLUDED.ai_probability,
        ml_capability_confidence = EXCLUDED.ml_capability_confidence,
        dual_score_agreement     = EXCLUDED.dual_score_agreement,
        trust_delta              = EXCLUDED.trust_delta,
        score_divergence         = EXCLUDED.score_divergence,
        review_priority          = EXCLUDED.review_priority,
        evidence_count           = EXCLUDED.evidence_count,
        latitude                 = EXCLUDED.latitude,
        longitude                = EXCLUDED.longitude,
        data_quality             = EXCLUDED.data_quality
"""

sync_to_postgres(df2, INSERT_FACILITY_SQL, "facility_detail")

# COMMAND ----------

# DBTITLE 1,Sync Table 3: risk_indicators
# ---------------------------------------------------------------------------
# Table 3: risk_indicators  (10 rows expected)
# Source table columns: indicator_id, indicator_name, priority, risk_pct, analysis_date
# NOTE: no 'description' column exists in the source
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("Syncing Table 3: risk_indicators")
print("=" * 70)

df3 = spark.table("workspace.healthgpt.gap_risk_indicators").select(
    "indicator_name",
    F.col("risk_pct").cast("decimal(5,2)").alias("risk_pct"),
    "priority",
)

INSERT_RISK_SQL = """
    INSERT INTO risk_indicators (indicator_name, risk_pct, priority)
    VALUES (%s, %s, %s)
    ON CONFLICT (indicator_name) DO UPDATE SET
        risk_pct  = EXCLUDED.risk_pct,
        priority  = EXCLUDED.priority
"""

sync_to_postgres(df3, INSERT_RISK_SQL, "risk_indicators")

# COMMAND ----------

# DBTITLE 1,Verification — Row Counts
print("\n" + "=" * 70)
print("Verification — Row Counts")
print("=" * 70)

token = get_lakebase_token()
with get_connection(token) as conn:
    with conn.cursor() as cur:
        for tbl, expected in [
            ("care_gap_summary", 923),
            ("facility_detail", 27978),
            ("risk_indicators", 10),
        ]:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            count = cur.fetchone()[0]
            status = "✓" if count >= expected * 0.99 else "⚠ LOW"
            print(f"  {tbl:<30} {count:>8,} rows  (expected ~{expected:,})  {status}")

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "=" * 70)
print("SYNC COMPLETE")
print("=" * 70)
print(f"""
  Delta source → Lakebase target:
    workspace.healthgpt.care_gap_silver_ml   → care_gap_summary
    workspace.healthgpt.facilities_silver_ml → facility_detail
    workspace.healthgpt.gap_risk_indicators  → risk_indicators

  Lakebase : {LAKEBASE_HOST}
  Database : {LAKEBASE_DB}

  App is ready at:
  https://healthgpt-care-gap-planner-7474652404991785.aws.databricksapps.com
""")

