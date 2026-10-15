"""
Phase 2b: Build the narratives layer from the CFPB FOIA archive exports and
join it to the filtered structured complaints on Complaint ID.

Run from the repo root:
    python etl/build_narratives_layer.py 2>&1 | tee narratives_output.txt
"""
from pyspark.sql import SparkSession, functions as F

NARR_GLOB = "data/raw/narratives/*.csv"
STRUCT_PATH = "data/processed/complaints_filtered"   # from profile_cfpb.py
NARR_OUT = "data/processed/narratives"
ENRICHED_OUT = "data/processed/tickets_enriched"

spark = (
    SparkSession.builder.appName("cfpb_narratives")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- 1. Read the archive exports (narratives contain commas/quotes/newlines) --
raw = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(NARR_GLOB)
)

narr = (
    raw.select(
        F.col("Complaint ID").alias("Complaint ID"),
        F.col("Consumer complaint narrative").alias("narrative"),
    )
    .filter(F.col("narrative").isNotNull() & (F.length("narrative") > 20))
    .dropDuplicates(["Complaint ID"])
    .withColumn("narrative_len", F.length("narrative"))
)

narr.write.mode("overwrite").parquet(NARR_OUT)

narr = spark.read.parquet(NARR_OUT)  # re-read: avoids re-parsing the CSVs
n_narr = narr.count()
print(f"\n=== NARRATIVES (unique Complaint IDs, usable text): {n_narr:,} ===")
narr.select("narrative_len").summary("min", "25%", "50%", "75%", "max").show()

# --- 2. Join to structured complaints ---------------------------------------
struct = spark.read.parquet(STRUCT_PATH)
n_struct = struct.count()
print(f"=== STRUCTURED (filtered) ROWS: {n_struct:,} ===")

enriched = struct.join(narr, on="Complaint ID", how="left").withColumn(
    "has_narrative", F.col("narrative").isNotNull()
)
enriched.write.mode("overwrite").parquet(ENRICHED_OUT)

enriched = spark.read.parquet(ENRICHED_OUT)

print("\n=== NARRATIVE COVERAGE BY PRODUCT ===")
(
    enriched.groupBy("Product")
    .agg(
        F.count("*").alias("complaints"),
        F.sum(F.col("has_narrative").cast("int")).alias("with_narrative"),
    )
    .withColumn("coverage_pct", F.round(F.col("with_narrative") / F.col("complaints") * 100, 1))
    .orderBy(F.desc("complaints"))
    .show(truncate=False)
)

print("=== COVERAGE BY YEAR ===")
(
    enriched.withColumn("year", F.year("Date received"))
    .groupBy("year")
    .agg(
        F.count("*").alias("complaints"),
        F.sum(F.col("has_narrative").cast("int")).alias("with_narrative"),
    )
    .withColumn("coverage_pct", F.round(F.col("with_narrative") / F.col("complaints") * 100, 1))
    .orderBy("year")
    .show()
)

print(f"Written: {NARR_OUT} and {ENRICHED_OUT}")
spark.stop()
