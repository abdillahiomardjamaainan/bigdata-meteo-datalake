from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "abdillahi",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

AIRFLOW_DIR = "/opt/airflow"

# ✅ Daily snapshot = date logique du run Airflow (PAS J-1)
SNAPSHOT_DATE = "{{ macros.ds_add(ds, 1) }}"

# ✅ Un identifiant unique par run (utile si tu relances)
RUN_ID = "{{ ts_nodash }}"

PARIS = pendulum.timezone("Europe/Paris")

with DAG(
    dag_id="movies_analytics_pipeline",
    schedule="07 20 * * *",  # ✅ tous les jours à 20h07 (Europe/Paris)
    start_date=pendulum.datetime(2026, 2, 17, tz=PARIS),
    catchup=False,
    default_args=DEFAULT_ARGS,
) as dag:

    fetch_tmdb = BashOperator(
        task_id="fetch_tmdb",
        bash_command=f"""
            set -e
            export SNAPSHOT_DATE="{SNAPSHOT_DATE}"
            export RUN_ID="{RUN_ID}"
            export OUTPUT_DIR="{AIRFLOW_DIR}/datalake/raw"
            python "{AIRFLOW_DIR}/scripts/ingest/fetch_tmdb.py"
        """,
    )

    fetch_omdb = BashOperator(
        task_id="fetch_omdb",
        bash_command=f"""
            set -e
            export SNAPSHOT_DATE="{SNAPSHOT_DATE}"
            export RUN_ID="{RUN_ID}"
            export OUTPUT_DIR="{AIRFLOW_DIR}/datalake/raw"
            python "{AIRFLOW_DIR}/scripts/ingest/fetch_omdb.py"
        """,
    )

    load_db = BashOperator(
        task_id="load_postgres",
        bash_command=f"""
            set -e
            export SNAPSHOT_DATE="{SNAPSHOT_DATE}"
            export RUN_ID="{RUN_ID}"
            export DATA_DIR="{AIRFLOW_DIR}/datalake/raw"
            export POSTGRES_HOST=postgres
            export POSTGRES_PORT=5432
            export POSTGRES_USER=postgres
            export POSTGRES_PASSWORD=postgres
            export POSTGRES_DB=datalake
            python "{AIRFLOW_DIR}/scripts/load/load_raw_to_postgres.py"
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            set -e
            cd "{AIRFLOW_DIR}/movies_analytics"
            dbt run
        """,
    )

    export_parquet = BashOperator(
        task_id="export_parquet",
        bash_command=f"""
            set -e
            export SNAPSHOT_DATE="{SNAPSHOT_DATE}"
            export RUN_ID="{RUN_ID}"
            export OUTPUT_DIR="{AIRFLOW_DIR}/datalake"
            export POSTGRES_HOST=postgres
            export POSTGRES_PORT=5432
            export POSTGRES_USER=postgres
            export POSTGRES_PASSWORD=postgres
            export POSTGRES_DB=datalake
            python "{AIRFLOW_DIR}/scripts/export/export_to_parquet.py"
        """,
    )

    index_es = BashOperator(
        task_id="index_elasticsearch",
        bash_command=f"""
            set -e
            export SNAPSHOT_DATE="{SNAPSHOT_DATE}"
            export RUN_ID="{RUN_ID}"
            export ES_HOST=http://elasticsearch:9200
            export OUTPUT_DIR="{AIRFLOW_DIR}/datalake"
            python "{AIRFLOW_DIR}/scripts/index/index_elasticsearch.py"
        """,
    )

    fetch_tmdb >> fetch_omdb >> load_db >> dbt_run >> export_parquet >> index_es
