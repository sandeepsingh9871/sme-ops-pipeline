"""
Phase 4: Automated data-quality validation on the Hive tables.

Writes:
  reports/dq_report.json   - full report of the latest run
  logs/dq_events.log       - one JSON line per check (ready for Splunk later)
Exit code 1 if any CRITICAL check fails, so Airflow/cron can stop the pipeline.

Run from the repo root:
    python etl/validate_data.py 2>&1 | tee validate_output.txt
"""
import datetime as dt
import json
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("cfpb_validate")
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
spark.sparkContext.setLogLevel("ERROR")

RUN_ID = uuid.uuid4().hex[:8]
checks = []


def add(name, passed, detail, severity="critical"):
    checks.append(
        {
            "run_id": RUN_ID,
            "ts": dt.datetime.now().isoformat(timespec="seconds"),
            "check": name,
            "status": "PASS" if passed else "FAIL",
            "severity": severity,
            "detail": detail,
        }
    )


c = spark.table("sme_ops.complaints")
n = spark.table("sme_ops.ticket_narratives")


a = c.agg(
    F.count("*").alias("rows"),
    F.countDistinct("complaint_id").alias("distinct_ids"),
    F.sum(F.col("complaint_id").isNull().cast("int")).alias("null_id"),
    F.sum(F.col("date_received").isNull().cast("int")).alias("null_date"),
    F.sum(F.col("product").isNull().cast("int")).alias("null_product"),
    F.sum(F.col("issue").isNull().cast("int")).alias("null_issue"),
    F.sum(F.col("days_to_company").isNull().cast("int")).alias("null_days"),
    F.sum((~F.coalesce(F.col("timely_response").isin("Yes", "No"), F.lit(False))).cast("int")).alias("bad_timely"),
    F.sum((F.col("date_received") < F.lit("2020-01-01")).cast("int")).alias("too_old"),
    F.sum((F.col("date_received") > F.current_date()).cast("int")).alias("future"),
    F.sum((F.col("days_to_company") < 0).cast("int")).alias("neg_days"),
).collect()[0]

rows = a["rows"]
add("min_row_count", rows >= 900_000, f"rows={rows:,} (min 900,000)")
add("complaint_id_not_null", a["null_id"] == 0, f"nulls={a['null_id']}")
add("complaint_id_unique", a["distinct_ids"] == rows, f"rows={rows:,} distinct={a['distinct_ids']:,}")
add("date_received_not_null", a["null_date"] == 0, f"nulls={a['null_date']}")
add("product_not_null", a["null_product"] == 0, f"nulls={a['null_product']}")
add("timely_response_valid_values", a["bad_timely"] == 0, f"bad_values={a['bad_timely']}")
add("dates_in_expected_range", a["too_old"] == 0 and a["future"] == 0, f"before_2020={a['too_old']} future={a['future']}")
add("days_to_company_non_negative", a["neg_days"] == 0, f"negative={a['neg_days']}")
add("issue_null_pct_below_0.1", a["null_issue"] / rows < 0.001, f"null_pct={a['null_issue'] / rows * 100:.4f}", "warn")
add("days_to_company_null_pct_below_1", a["null_days"] / rows < 0.01, f"null_pct={a['null_days'] / rows * 100:.4f}", "warn")

na = n.agg(
    F.count("*").alias("rows"),
    F.countDistinct("complaint_id").alias("distinct_ids"),
    F.sum((F.col("narrative_len") <= 20).cast("int")).alias("too_short"),
).collect()[0]
orphans = n.join(c.select("complaint_id"), "complaint_id", "left_anti").count()
coverage = na["rows"] / rows * 100

add("narrative_ids_unique", na["distinct_ids"] == na["rows"], f"rows={na['rows']:,} distinct={na['distinct_ids']:,}")
add("narrative_min_length", na["too_short"] == 0, f"too_short={na['too_short']}")
add("narrative_ids_exist_in_complaints", orphans == 0, f"orphans={orphans}")
add("narrative_coverage_at_least_40pct", coverage >= 40, f"coverage={coverage:.1f}%", "warn")

# --- Output ------------------------------------------------------------------
Path("reports").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)
Path("reports/dq_report.json").write_text(json.dumps(checks, indent=2))
with open("logs/dq_events.log", "a", encoding="utf-8") as f:
    for ch in checks:
        f.write(json.dumps(ch) + "\n")

print(f"\n=== DATA QUALITY REPORT (run {RUN_ID}) ===")
for ch in checks:
    print(f"[{ch['status']}] ({ch['severity']:<8}) {ch['check']:<38} {ch['detail']}")

critical_fail = [ch for ch in checks if ch["status"] == "FAIL" and ch["severity"] == "critical"]
warn_fail = [ch for ch in checks if ch["status"] == "FAIL" and ch["severity"] == "warn"]
print(f"\nSummary: {len(checks) - len(critical_fail) - len(warn_fail)} passed, "
      f"{len(warn_fail)} warnings, {len(critical_fail)} critical failures")

spark.stop()
sys.exit(1 if critical_fail else 0)
