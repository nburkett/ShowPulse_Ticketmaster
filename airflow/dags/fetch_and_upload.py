# airflow imports
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from google.cloud import storage
import os


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


color = 'yellow'
year = '2021'
month = '01'
dataset_file = f"{color}_tripdata_{year}-{month}.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

# path = f'./data/{color}/{year}'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# @task
# def create_directory():
#     # path = f'../data/{color}/{year}'
#     if not os.path.exists(path_to_local_home):
#         os.makedirs(path_to_local_home)
#         print(f"Directory {path_to_local_home} created.")
#     else:
#         print(f"Directory {path_to_local_home} already exists.")


@task
def print_user():
    print(os.getlogin())


@task
def test():
    print("TESTING THIS EXAMPLE DAG")

    # print({{ run_id }})

@task ### NEED TO FIX 
def get_bash_var():
    bash_command="echo 'The run_id is: {{ ds_nodash }}'"
    op = BashOperator(task_id="bash_show_var", bash_command=bash_command)
    return op.execute(context={})


@task
def bash_get_data():
    # Define the bash script to execute
    bash_command = f'curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}'
    # Create a BashOperator to execute the script
    op = BashOperator(task_id="bash_get_data", bash_command=bash_command)
    output = op.execute(context={})

    ### rename the file using python 
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # Define the new file name
    new_file_name = f'{color}_tripdata_{current_time}.parquet'
    # Rename the file
    os.rename(f'{dataset_file}', new_file_name)

    print(f"File renamed from {dataset_file} to {new_file_name}")
    # Return the file name
    return new_file_name

# @task
# def gzip_parquet_file(filename):
@task
def gzip_parquet_file(new_file_name):
    # Define the bash script to execute
    bash_command = f'gzip {path_to_local_home}/{new_file_name}'
    # Create a BashOperator to execute the script
    op = BashOperator(task_id="bash_gzip_parquet_file", bash_command=bash_command)
    output = op.execute(context={})
    print(f"File {new_file_name} gzipped.")


@task
def local_to_gcs(project_id, bucket, filename):
    ### setting max chunk size to 5MB
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024

    client = storage.Client()
    bucket = client.bucket(bucket)

    uploaded_file_name= f"raw/{color}/{year}/{filename}"
    blob = bucket.blob(uploaded_file_name)

    local_file = f"{path_to_local_home}/{filename}"
    blob.upload_from_filename(local_file)
    ## print out success message 
    print(f"File {filename} uploaded to Project: {project_id} and Bucket: {bucket}.")
    #print location of the file ion the data lake 
    return f"gs://{bucket}/{uploaded_file_name}"

@task
def delete_local_file(filename):
    os.remove(f"{path_to_local_home}/{filename}")
    print(f"File {filename} deleted from local directory.")

@task
def check_for_files():
    bash_command = f'ls'
    # Create a BashOperator to execute the script
    op = BashOperator(task_id="bash_view_files", bash_command=bash_command)
    output = op.execute(context={})

with DAG(
        dag_id="ny_taxi_local",
        schedule_interval='@daily',
        start_date=datetime(2023, 5, 13),
        catchup=False,
        tags=["NY Taxi Data"]) as dag:

    with TaskGroup("tests", tooltip="Transform and stage data") as tests:
        # test()
        get_bash_var()
        # print_user()
        # create_directory()
        # task order
        # get_bash_var() >> print_user()

    with TaskGroup("get_data", tooltip="Transform and stage data") as get_data:
        file_name = bash_get_data()

    with TaskGroup("upload_data_to_GCS_Lake", tooltip="Transform and stage data") as data_lake_upload:
        gcs_path = local_to_gcs(PROJECT_ID, BUCKET, file_name)

    with TaskGroup("delete_local_file", tooltip="Clean up all local files") as clean_up:
        delete_local_file(file_name)
        check_for_files()
        # task order
        # delete_local_file >> check_for_files



[tests, get_data] >> data_lake_upload >> clean_up
