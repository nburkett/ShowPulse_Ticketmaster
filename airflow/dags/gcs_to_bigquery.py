# airflow imports
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
import json
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
from google.cloud import bigquery


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")
BUCKET = 'kafka-spark-data'

# path = f'./data/{color}/{year}'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


schema = [
    {"name": "event_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_zipcode", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "venues_timezone", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_state_full", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_state_short", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_country_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_country_short", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "venue_longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "venue_latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "attraction_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_segment_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_segment_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_genre_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_genre_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_subgenre_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "attraction_subgenre_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_start_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ticket_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_start_time", "type": "STRING", "mode": "NULLABLE"},
    {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
    {"name": "min_price", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "max_price", "type": "FLOAT", "mode": "NULLABLE"}
]

def deduplicate_data():
    # Initialize BigQuery client
    client = bigquery.Client()

    # Perform deduplication
    query = f"""
        INSERT INTO `{PROJECT_ID}.twitter_kafka_pyspark_test.ticketmaster_airflow_prod`
        SELECT *
        FROM `{PROJECT_ID}.twitter_kafka_pyspark_test.ticketmaster_airflow_stg`
        WHERE event_id NOT IN (
            SELECT event_id
            FROM `{PROJECT_ID}.twitter_kafka_pyspark_test.ticketmaster_airflow_prod`
        )
    """

    # Run the deduplication query
    client.query(query)


default_args = {
    'owner': 'nburkett',
    # 'description': 'Hourly data pipeline to generate dims and facts for streamify',
    'schedule_interval': '@hourly',
    'start_date': datetime.now(),
    'catchup': False,
    'max_active_runs': 1,
    'retries': 1,
    # 'user_defined_macros': MACRO_VARS,
    'tags': ['ticketmaster', 'GCS', 'BigQuery'],
}

with DAG(
    dag_id='ticketmaster_GCS_to_BQ_dag',
    default_args=default_args,
) as dag:
        
    move_files_task = GCSToBigQueryOperator(
    task_id='move_files_to_bigquery',
    bucket=BUCKET,
    source_objects=['raw-spark-data/*.csv'],
    write_disposition ='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    destination_project_dataset_table=f'{PROJECT_ID}:twitter_kafka_pyspark_test.ticketmaster_airflow_stg',
    autodetect=True,
    schema_fields=schema,
    )

    deduplicate_data_task = PythonOperator(
        task_id='deduplicate_data',
        python_callable=deduplicate_data,
    )
    move_files_task >> deduplicate_data_task


    # gsutil cp gs://kafka-spark-data/raw-spark-data/part-00000-1b22615e-d518-4a77-a89e-fdd159238389-c000.snappy.parquet - | tr -d '\000' | gsutil cp - gs://kafka-spark-data/raw-spark-data/fixed_file.snappy.parquet   
