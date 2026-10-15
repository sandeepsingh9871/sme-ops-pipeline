"""
Phase 3: Register the processed data as Hive tables (Spark with Hive support,
embedded Derby metastore) and run HiveQL analytics on them.

Run from the repo root:
    python etl/create_hive_tables.py 2>&1 | tee hive_output.txt
"""
import re

from pyspark.sql import SparkSession, functions as F

ENRICHED = "data/processed/tickets_enriched"

spark = (
    SparkSession.builder.appName("cfpb_hive_tables")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.warehouse.dir", "data/warehouse")
    .config(
        "spark.hadoop.javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=data/metastore_db;create=true",
    )
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def snake(name: str) -> str:
    """'Timely response?' -> 'timely_response' (Hive/Parquet-safe column names)."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-zA-Z]+", "_", name)).strip("_").lower()


df = spark.read.parquet(ENRICHED)
df = df.toDF(*[snake(c) for c in df.columns])

# --- Table 1: structured complaints (no heavy text column) -------------------
sent = F.coalesce(
    F.to_date("date_sent_to_company", "yyyy-MM-dd"),
    F.to_date("date_sent_to_company", "MM/dd/yyyy"),
)
complaints = (
    df.drop("narrative", "narrative_len")
    .withColumn("date_sent_to_company", sent)
    .withColumn("days_to_company", F.datediff("date_sent_to_company", "date_received"))
    .withColumn("sla_breach", (F.col("timely_response") == "No").cast("int"))
    .withColumn("year", F.year("date_received"))
    .withColumn("month", F.month("date_received"))
)

# --- Table 2: narratives, kept separate so SLA queries never scan the text ---
narratives = df.filter(F.col("narrative").isNotNull()).select(
    "complaint_id", "narrative", "narrative_len"
)

spark.sql("CREATE DATABASE IF NOT EXISTS sme_ops")
spark.sql("DROP TABLE IF EXISTS sme_ops.complaints")
spark.sql("DROP TABLE IF EXISTS sme_ops.ticket_narratives")

(
    complaints.write.mode("overwrite")
    .format("parquet")
    .partitionBy("product")
    .saveAsTable("sme_ops.complaints")
)
narratives.write.mode("overwrite").format("parquet").saveAsTable("sme_ops.ticket_narratives")

print("\n=== TABLES ===")
spark.sql("SHOW TABLES IN sme_ops").show(truncate=False)
print("=== SCHEMA: sme_ops.complaints ===")
spark.sql("DESCRIBE sme_ops.complaints").show(40, truncate=False)

# --- HiveQL analytics --------------------------------------------------------
queries = {
    "SLA BREACH BY PRODUCT": """
        SELECT product, COUNT(*) AS complaints, SUM(sla_breach) AS breaches,
               ROUND(100 * SUM(sla_breach) / COUNT(*), 2) AS breach_pct
        FROM sme_ops.complaints
        GROUP BY product ORDER BY complaints DESC""",
    "YEARLY VOLUME, BREACH RATE, AVG DAYS TO COMPANY": """
        SELECT year, COUNT(*) AS complaints,
               ROUND(100 * SUM(sla_breach) / COUNT(*), 2) AS breach_pct,
               ROUND(AVG(days_to_company), 2) AS avg_days_to_company
        FROM sme_ops.complaints
        GROUP BY year ORDER BY year""",
    "TOP 10 ISSUES": """
        SELECT issue, COUNT(*) AS complaints
        FROM sme_ops.complaints
        GROUP BY issue ORDER BY complaints DESC LIMIT 10""",
    "COMPANY RESPONSE TYPES": """
        SELECT company_response_to_consumer, COUNT(*) AS complaints
        FROM sme_ops.complaints
        GROUP BY company_response_to_consumer ORDER BY complaints DESC""",
    "TOP 10 COMPANIES (>=1000 COMPLAINTS) BY BREACH RATE": """
        SELECT company, COUNT(*) AS complaints,
               ROUND(100 * SUM(sla_breach) / COUNT(*), 2) AS breach_pct
        FROM sme_ops.complaints
        GROUP BY company HAVING COUNT(*) >= 1000
        ORDER BY breach_pct DESC LIMIT 10""",
}
for title, sql in queries.items():
    print(f"\n=== {title} ===")
    spark.sql(sql).show(truncate=False)

spark.stop()
