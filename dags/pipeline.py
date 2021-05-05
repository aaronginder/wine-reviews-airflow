# Imports
import datetime
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
)
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.contrib.operators import bigquery_operator


# Define default pipeline arguments
default_args = {
    "owner": "Aaron Ginder",
    "start_date": datetime.datetime(2021, 4, 18),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

# Define DAG Variables
config = {
    # Global Config
    "project_id": "inspiring-rite-311915",
    "staging_dataset": "Wines_Demo",
    "source_bucket": "wine-reviews-ingestion-pipeline",
    "destination_bucket": "wine-reviews-ingestion-pipeline",
    "dag_id": "wines-review-dag",
    "bigquery_conn": "bigquery_default",
    "gcp_conn": "google_cloud_storage_default",
    # Tasks
    "tasks": {
        "load_reviews_to_bigquery": {
            "source_objects": "wine_reviews_demo_*.csv",
            "bigquery_table": "1_WINE_REVIEWS_RAW",
            "destination_objects": f"wine_reviews_demo_{datetime.date.today()}",
            "schema": f"schema/1_WINE_REVIEWS_RAW.json",
        },
        "standardise_reviews": {
            "bigquery_table": "2_CLEANED_REVIEWS",
        },
        "analyse_reviews": {
            "bigquery_table": "3_WINE_REVIEW_STATS",
        },
    },
}

# Define DAG
dag = models.DAG(
    dag_id=config["dag_id"],
    start_date=datetime.datetime.now(),
    schedule_interval="@once",
    default_args=default_args,
    description="Wine reviews pipeline",
    concurrency=5,
    max_active_runs=1,
)


# Custom Python functions
def check_bq_table_exists(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_schema_path: str,
    dag: models.DAG,
    task_id: str,
    **kwargs,
):
    hook = BigQueryHook(use_legacy_sql=False)

    sql = f"""
    SELECT * FROM {dataset_id}.INFORMATION_SCHEMA.TABLES
    WHERE table_name = '{table_id}';
    """
    query = hook.get_first(sql=sql)
    print(query)
    if not query:
        try:
            bigquery_operator.BigQueryCreateEmptyTableOperator(
                task_id=task_id,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                gcs_schema_object=gcs_schema_path,
                dag=dag,
                **kwargs,
            )
            return True
        except Exception:
            return False


# Define data pipeline tasks
start_pipeline = BashOperator(
    task_id="start-pipeline", bash_command="echo starting pipeline", dag=dag
)

load_reviews_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id="load-reviews-to-bigquery",
    bucket=config["source_bucket"],
    source_objects=[
        f'ingress/{config["tasks"]["load_reviews_to_bigquery"]["source_objects"]}'
    ],
    destination_project_dataset_table=f"{config['project_id']}:{config['staging_dataset']}.{config['tasks']['load_reviews_to_bigquery']['bigquery_table']}",
    source_format="csv",
    skip_leading_rows=1,
    field_delimiter=",",
    ignore_unknown_values=True,
    write_disposition="WRITE_APPEND",
    max_bad_records=100,
    schema_object="schema/1_WINE_REVIEWS_RAW.json",
)

check_wine_data_load = BigQueryCheckOperator(
    task_id="check-wine-data-load",
    sql=f"SELECT COUNT(*) FROM `{config['project_id']}.{config['staging_dataset']}.{config['tasks']['load_reviews_to_bigquery']['bigquery_table']}`",
    gcp_conn_id=config["bigquery_conn"],
    retries=1,
    retry_delay=5,
    default_args=default_args,
    use_legacy_sql=False,
)

archive_wine_data = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id=f"move-raw-data-to-archive",
    source_bucket="wine-reviews-ingestion-pipeline",
    source_object=f"ingress/{config['tasks']['load_reviews_to_bigquery']['source_objects']}",
    destination_bucket="wine-reviews-ingestion-pipeline",
    destination_object=f"success/{config['tasks']['load_reviews_to_bigquery']['destination_objects']}",
    gcp_conn_id=config["bigquery_conn"],
    move_object=True,
)

standardise_reviews = BigQueryOperator(
    task_id="standardise-reviews",
    sql=f"""
    SELECT DISTINCT
        country,
        region_1 AS region,
        region_2 AS sub_region,
        province,
        variety,
        winery,
        designation,
        description,
        points,
        price,
        CURRENT_DATE() AS ingestion_date,
        CURRENT_TIMESTAMP() AS ingestion_time
        FROM
        `{config['project_id']}.{config['staging_dataset']}.{config['tasks']['load_reviews_to_bigquery']['bigquery_table']}`
        WHERE region_1 IS NOT NULL
    """,
    destination_dataset_table=f"{config['staging_dataset']}.{config['tasks']['standardise_reviews']['bigquery_table']}",
    gcp_conn_id=config["bigquery_conn"],
    write_disposition="WRITE_APPEND",
    default_args=default_args,
    use_legacy_sql=False,
)

analyse_reviews = BigQueryOperator(
    task_id="analyse-reviews",
    sql=f"""
    SELECT DISTINCT
        country,
        province,
        variety,
        AVG(points) OVER (PARTITION BY country, province, variety) AS avg_province_variety_points,
        AVG(points) OVER (PARTITION BY country, variety) AS avg_country_points,
        AVG(points) OVER (PARTITION BY variety) AS avg_variety_points,
        AVG(points) OVER (PARTITION BY country, winery) AS avg_winery_by_country_points,
        AVG(points) OVER (PARTITION BY winery) AS avg_wine_points,
        ROUND(AVG(price) OVER (PARTITION BY country, province, variety), 2) AS avg_variety_price_by_country,
        AVG(price) OVER (PARTITION BY country, winery) AS avg_wine_price_by_country,
        COUNT(winery) OVER (PARTITION BY country, province, variety, winery) AS num_reviews,
        COUNT(winery) OVER (PARTITION BY country, variety) AS num_reviews_in_variety_group_by_country,
        ingestion_date,
        ingestion_time
        FROM
        `{config['project_id']}.{config['staging_dataset']}.{config['tasks']['standardise_reviews']['bigquery_table']}`
        WHERE ingestion_date = ingestion_date 
    """,
    destination_dataset_table=f"{config['staging_dataset']}.{config['tasks']['analyse_reviews']['bigquery_table']}",
    gcp_conn_id=config["bigquery_conn"],
    write_disposition="WRITE_APPEND",
    default_args=default_args,
    use_legacy_sql=False,
)

do_something = DummyOperator(task_id="example-task")

finish_pipeline = BashOperator(
    task_id="finish-pipeline", bash_command="echo finish pipeline", dag=dag
)

(
    start_pipeline
    >> load_reviews_to_bigquery
    >> [check_wine_data_load, do_something]
    >> archive_wine_data
    >> standardise_reviews
    >> analyse_reviews
    >> finish_pipeline
)
