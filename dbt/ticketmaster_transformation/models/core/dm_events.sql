
{{ config(materialized = 'table') }}

SELECT event_id,event_name,event_type,event_url
FROM {{ source('staging','kafka_pyspark')}}

