{{ config(materialized = 'table') }}

SELECT 
    "LocationID" as locationid,
    "Borough" as borough,
    "Zone" as zone,
    replace(service_zone, 'Boro', 'Green') as service_zone

FROM {{ ref('taxi_zone_lookup') }}