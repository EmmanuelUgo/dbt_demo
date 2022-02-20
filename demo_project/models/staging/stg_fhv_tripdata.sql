
WITH fhv_trips AS(
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY dispatching_base_num, pickup_datetime) rn
    FROM {{ source('staging','fhv_tripdata') }}
    WHERE dispatching_base_num IS NOT NULL

)


SELECT 
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime' ]) }} as trip_id,
    cast(dispatching_base_num as text) as dispatching_base_num,
    cast("PULocationID" as integer) as  pickup_locationid,
    cast("DOLocationID" as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    cast("SR_Flag" as character) as sr_flag
    
FROM fhv_trips
WHERE rn = 1

-- build --m <model.sql> --var 'is_test_run: false'

{% if var('is_test_run', default = true) %}

LIMIT 100

{% endif %}
