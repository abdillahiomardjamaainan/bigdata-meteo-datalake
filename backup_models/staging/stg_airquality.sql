{{
    config(
        materialized='table',
        schema='staging'
    )
}}

SELECT
    city,
    latitude,
    longitude,
    timestamp AS measured_at,
    aqi,
    pm2_5,
    pm10,
    no2,
    o3,
    co,
    loaded_at,

-- Colonnes calculées


CASE 
        WHEN aqi = 1 THEN 'Good'
        WHEN aqi = 2 THEN 'Fair'
        WHEN aqi = 3 THEN 'Moderate'
        WHEN aqi = 4 THEN 'Poor'
        WHEN aqi = 5 THEN 'Very Poor'
    END AS air_quality_category

FROM {{ source('raw', 'airquality') }}