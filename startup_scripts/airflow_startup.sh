#!/bin/bash

# echo "Changing permissions for dbt folder..."
# cd ~/streamify/ && sudo chmod -R 777 dbt

echo "Building airflow docker images..."
cd ../airflow
docker-compose build

echo "Running airflow-init..."
echo "Starting up airflow in detached mode..."
docker-compose up -d
# docker-compose  airflow-init up -d


# docker-compose up -d

# echo "Airflow started successfully."
# echo "Airflow is running in detached mode. "
# echo "Run 'docker-compose logs --follow' to see the logs."