"""
Phase 8: Export small, aggregated tables from the Hive layer for Power BI / Tableau.

Writes CSVs to reports/bi/. Run when no other Spark job is active (shared Derby metastore):
    python etl/export_for_bi.py 2>&1 | tee export_output.txt
"""
import csv
import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

OUT = Path("reports/bi")
OUT.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder.appName("cfpb_export_bi")
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


def export(name: str, sql: str) -> None:
    df = spark.sql(sql)
    rows = df.collect()
    with open(OUT / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])
    print(f"{name}.csv: {len(rows):,} rows")


export("monthly_summary", """
    SELECT make_date(year, month, 1) AS month_start, year, month, product,
           COUNT(*) AS complaints,
           SUM(sla_breach) AS late_responses,
           ROUND(100 * SUM(sla_breach) / COUNT(*), 2) AS late_pct,
           ROUND(AVG(days_to_company), 2) AS avg_days_to_company
    FROM sme_ops.complaints
    GROUP BY year, month, product
    ORDER BY month_start, product""")

export("issue_summary", """
    SELECT product, issue,
           COUNT(*) AS complaints,
           SUM(sla_breach) AS late_responses
    FROM sme_ops.complaints
    GROUP BY product, issue
    HAVING COUNT(*) >= 50
    ORDER BY complaints DESC""")

export("company_summary", """
    SELECT company, product,
           COUNT(*) AS complaints,
           SUM(sla_breach) AS late_responses,
           ROUND(100 * SUM(sla_breach) / COUNT(*), 2) AS late_pct
    FROM sme_ops.complaints
    GROUP BY company, product
    HAVING COUNT(*) >= 200
    ORDER BY complaints DESC""")

export("state_summary", """
    SELECT state,
           COUNT(*) AS complaints,
           SUM(sla_breach) AS late_responses,
           ROUND(100 * SUM(sla_breach) / COUNT(*), 2) AS late_pct
    FROM sme_ops.complaints
    WHERE state IS NOT NULL
    GROUP BY state
    ORDER BY complaints DESC""")

export("response_summary", """
    SELECT product, company_response_to_consumer AS response, COUNT(*) AS complaints
    FROM sme_ops.complaints
    GROUP BY product, company_response_to_consumer
    ORDER BY product, complaints DESC""")

export("narrative_coverage", """
    SELECT c.year,
           COUNT(*) AS complaints,
           COUNT(n.complaint_id) AS with_narrative,
           ROUND(100 * COUNT(n.complaint_id) / COUNT(*), 1) AS coverage_pct
    FROM sme_ops.complaints c
    LEFT JOIN sme_ops.ticket_narratives n ON c.complaint_id = n.complaint_id
    GROUP BY c.year
    ORDER BY c.year""")

spark.stop()

# --- Classifier outputs (from train_ticket_classifier.py) ---------------------------
cm_path = Path("reports/confusion_matrix.csv")
if cm_path.exists():
    cm = pd.read_csv(cm_path, index_col=0)
    long = (
        cm.rename_axis("true_issue")
        .reset_index()
        .melt(id_vars="true_issue", var_name="predicted_issue", value_name="tickets")
    )
    long.to_csv(OUT / "confusion_long.csv", index=False)
    print(f"confusion_long.csv: {len(long):,} rows")

tr_path = Path("reports/threshold_tradeoff.csv")
if tr_path.exists():
    shutil.copy(tr_path, OUT / "threshold_tradeoff.csv")
    print("threshold_tradeoff.csv: copied")

print(f"\nDone. Files in {OUT}/")
