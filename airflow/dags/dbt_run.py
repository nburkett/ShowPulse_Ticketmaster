import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'nburkett'
    # 'start_date' : days_ago(1),
}

with DAG(
    dag_id = f'showpulse_dbt_dag',
    default_args = default_args,
    description = f'Hourly data pipeline to generate dims and facts for streamify',
    schedule_interval=timedelta(minutes=1), 
    start_date=datetime(2023,6,17),
    catchup=False,
    max_active_runs=1,
    tags=['showpulse','dbt']
) as dag:


    # install_dbt_task = BashOperator(
    #     task_id='install_dbt',
    #     bash_command='pip3 install dbt-bigquery',
    #     dag=dag
    # )
    pwd1 = BashOperator(
            task_id = 'pwd1',
            bash_command = 'pwd && ls'
        )

    change_dir = BashOperator(
            task_id = 'dbt_change_dir',
            bash_command = 'cd /dbt/ticketmaster_transformation'
        )

    pwd2 = BashOperator(
            task_id = 'pwd2',
            bash_command = 'cd /dbt/ticketmaster_transformation && pwd && ls'
        )
    
    run_dbt_task = BashOperator(
            task_id = 'dbt_run',
            bash_command = 'cd /dbt/ticketmaster_transformation && dbt run'
        )


    # install_dbt_task >> 
    pwd1 >> change_dir >> pwd2 >> run_dbt_task