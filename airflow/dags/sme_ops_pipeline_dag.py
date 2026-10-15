"""
Airflow DAG: SME onboarding & servicing analytics pipeline.

ingest_filter -> build_narratives -> hive_tables -> validate (GATE) -> train_classifier

If `validate` fails, Airflow marks `train_classifier` as upstream_failed and
never runs it, so a bad data load can't produce a model or reports.

Place this file in ~/sme-ops-pipeline/airflow/dags/ and point Airflow's
dags_folder there (see setup commands).
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = "/home/grimm/sme-ops-pipeline"
PY = f"{PROJECT}/venv/bin/python"  # project venv (PySpark, sklearn), not Airflow's venv

default_args = {
    "owner": "sandeep",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def step(task_id: str, script: str, retries: int | None = None) -> BashOperator:
    kwargs = {} if retries is None else {"retries": retries}
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT} && {PY} {script}",
        **kwargs,
    )


with DAG(
    dag_id="sme_ops_pipeline",
    description="CFPB complaints -> Hive tables -> validation -> NLP ticket routing",
    schedule="0 2 * * 1",          # Mondays 02:00 UTC
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cfpb", "spark", "nlp"],
) as dag:
    ingest_filter = step("ingest_filter", "etl/profile_cfpb.py")
    build_narratives = step("build_narratives", "etl/build_narratives_layer.py")
    hive_tables = step("hive_tables", "etl/create_hive_tables.py")
    validate = step("validate", "etl/validate_data.py", retries=0)  # a DQ failure is not transient
    train_classifier = step("train_classifier", "etl/train_ticket_classifier.py")

    ingest_filter >> build_narratives >> hive_tables >> validate >> train_classifier
