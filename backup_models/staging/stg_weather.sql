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
    temperature,
    feels_like,
    humidity,
    pressure,
    wind_speed,
    weather_main,
    weather_description,
    loaded_at,

-- Colonnes calculées

CASE 
        WHEN temperature < 0 THEN 'Freezing'
        WHEN temperature BETWEEN 0 AND 10 THEN 'Cold'
        WHEN temperature BETWEEN 10 AND 20 THEN 'Mild'
        WHEN temperature BETWEEN 20 AND 30 THEN 'Warm'
        ELSE 'Hot'
    END AS temperature_category

FROM {{ source('raw', 'weather') }}