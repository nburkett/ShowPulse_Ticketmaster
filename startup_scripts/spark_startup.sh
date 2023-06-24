#!/bin/bash

echo "Building spark docker images..."
cd ~/pipeline_project/spark
docker-compose build


# DOWNLOAD JARS
# DOWNLOAD 
echo "Running airflow-init..."
docker-compose up airflow-init

echo "Create Topic"
docker-compose up -d

echo "Airflow started successfully."
echo "Airflow is running in detached mode. "
echo "Run 'docker-compose logs --follow' to see the logs."