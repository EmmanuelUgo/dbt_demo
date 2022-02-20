WITH fhv_data AS(
    SELECT *
    FROM {{ ref('stg_fhv_tripdata') }}
    
    ),

    dim_zones AS(
        SELECT *
        FROM {{ ref('dim_zones')}}
        WHERE borough != 'Unknown'
    )

SELECT 
        pl.trip_id,
        pl.dispatching_base_num,
		pl.pickup_locationid,
		dz.borough as pickup_borough,
		dz.zone as pickup_zone,
		pl.pickup_datetime,
		pl.dropoff_locationid,
		dl.borough as dropoff_borough,
		dl.zone as dropoff_zone,
		pl.dropoff_datetime,
		pl.sr_flag
FROM fhv_data pl
INNER JOIN dim_zones dz
ON pl.pickup_locationid = dz.locationid
INNER JOIN dim_zones dl
ON pl.dropoff_locationid = dl.locationid
