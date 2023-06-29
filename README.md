# ShowPulse_Ticketmaster

A data pipeline with Kafka, Spark Streaming, dbt, Docker, Airflow, and GCP!

## Description

### Objective

This projects uses the Ticketmaster API to collect, transform and then visualize information on events happening in a given city in real time! The data is pre-processed & appended to a BigQuery data warehouse in and then a batch job further processes the data and turns it into the desired tables for my dashboard. Using this dashboard, we will be able to see _________

The project will stream events generated from a fake music streaming service (like Spotify) and create a data pipeline that consumes the real-time data. The data coming in would be similar to an event of a user listening to a song, navigating on the website, authenticating. The data would be processed in real-time and stored to the data lake periodically (every two minutes). The hourly batch job will then consume this data, apply transformations, and create the desired tables for our dashboard to generate analytics. We will try to analyze metrics like popular songs, active users, user demographics etc.
