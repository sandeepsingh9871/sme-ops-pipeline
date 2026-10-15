"""
Phase 1-2: Profile the raw CFPB complaints file and write a filtered parquet layer.

Run from the repo root:
    python etl/profile_cfpb.py
"""
from pyspark.sql import SparkSession, functions as F

RAW_PATH = "data/raw/complaints.csv"
OUT_PATH = "data/processed/complaints_filtered"

# Products relevant to the cards / payments / servicing story
KEEP_PRODUCTS = [
    "Checking or savings account",
    "Credit card",
    "Credit card or prepaid card",
    "Prepaid card",
    "Money transfer, virtual currency, or money service",
]

spark = (
    SparkSession.builder.appName("cfpb_profile")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# multiLine + escape are required: narratives contain commas, quotes and newlines
df = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(RAW_PATH)
)

print("\n=== SCHEMA ===")
df.printSchema()

total = df.count()
print(f"\n=== TOTAL ROWS: {total:,} ===")

print("\n=== NULL % PER COLUMN ===")
null_pct = df.select(
    [
        (F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / total * 100)
        .alias(c)
        for c in df.columns
    ]
)
null_pct.show(vertical=True, truncate=False)

print("\n=== TOP 15 PRODUCTS ===")
df.groupBy("Product").count().orderBy(F.desc("count")).show(15, truncate=False)

print("\n=== TIMELY RESPONSE? (SLA proxy) ===")
df.groupBy("Timely response?").count().show()

# --- Filter layer -----------------------------------------------------------
# NOTE: the bulk CSV has no "Consumer complaint narrative" column, so narratives
# are ingested separately (API) and joined later on Complaint ID.
date_col = F.coalesce(
    F.to_date("Date received", "yyyy-MM-dd"),
    F.to_date("Date received", "MM/dd/yyyy"),
)
filtered = (
    df.filter(F.col("Product").isin(KEEP_PRODUCTS))
    .withColumn("Date received", date_col)
    .filter(F.col("Date received") >= F.lit("2020-01-01"))
    .dropDuplicates(["Complaint ID"])
)

kept = filtered.count()
print(f"\n=== FILTERED ROWS: {kept:,} ({kept / total * 100:.2f}% of raw) ===")

(
    filtered.write.mode("overwrite")
    .partitionBy("Product")
    .parquet(OUT_PATH)
)
print(f"Written to {OUT_PATH}")

spark.stop()
