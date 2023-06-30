
{{ config(materialized='table') }}

with deduplicated_data as (

  select distinct 
  event_name,
  event_type,
  event_id,
  event_url,
  venue_name,
  venue_id,
  CAST(venue_zipcode AS INT) as venue_zipcode,
  venues_timezone,
  venue_city,
  venue_state_full,
  venue_state_short,
  venue_country_name,
  venue_country_short,
  venue_address,
  safe_cast(venue_longitude AS FLOAT64) as venue_longitude,
  safe_cast(venue_latitude AS FLOAT64) as venue_latitude,
  attraction_name,
  attraction_type,
  attraction_id,
  attraction_url,
  attraction_segment_id,
  attraction_segment_name,
  attraction_genre_id,
  attraction_genre_name,
  attraction_subgenre_id,
  attraction_subgenre_name,
  event_start_date,
  ticket_status,
  event_start_time,
  currency,
  safe_cast(min_price AS FLOAT64) as min_price,
  safe_cast(max_price AS FLOAT64 ) as max_price
  from {{ source('staging', 'kafka_pyspark') }}
)

select *
from deduplicated_data

