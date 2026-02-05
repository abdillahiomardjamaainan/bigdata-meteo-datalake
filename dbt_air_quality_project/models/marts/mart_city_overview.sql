{{
    config(
        materialized='table',
        schema='marts'
    )
}}


WITH latest_air_quality AS (
    SELECT DISTINCT ON (city)
        city,
        aqi,
        air_quality_category,
        pm2_5,
        pm10,
        no2,
        measured_at AS air_measured_at
    FROM {{ ref('stg_airquality') }}
    ORDER BY city, measured_at DESC
),

latest_weather AS (
    SELECT DISTINCT ON (city)
        city,
        temperature,
        temperature_category,
        humidity,
        wind_speed,
        weather_main,
        measured_at AS weather_measured_at
    FROM {{ ref('stg_weather') }}
    ORDER BY city, measured_at DESC
),

population_data AS (
    SELECT
        city,
        country,
        country_code,
        latitude,
        longitude,
        population,
        CASE
            WHEN population > 5000000 THEN 'Mega'
            WHEN population > 1000000 THEN 'Large'
            ELSE 'Medium'
        END AS city_size
    FROM {{ ref('stg_population') }}
)

SELECT
    -- Identité de la ville
    p.city, p.country, p.country_code, p.population, p.city_size,

-- 🌍 Coordonnées géographiques (pour carte Kibana)
p.latitude, p.longitude,

-- Air Quality
aq.aqi,
aq.air_quality_category,
aq.pm2_5,
aq.pm10,
aq.no2,
aq.air_measured_at,

-- Weather
w.temperature,
w.temperature_category,
w.humidity,
w.wind_speed,
w.weather_main,
w.weather_measured_at,

-- Indicateurs calculés
CASE 
        WHEN aq.aqi <= 2 AND w.temperature BETWEEN 10 AND 25 THEN 'Excellent'
        WHEN aq.aqi = 3 AND w.temperature BETWEEN 10 AND 25 THEN 'Good'
        WHEN aq.aqi >= 4 OR w.temperature < 0 OR w.temperature > 30 THEN 'Poor'
        ELSE 'Average'
    END AS livability_score,
    
    ROUND(p.population::numeric / 1000000, 2) AS population_millions

FROM population_data p
LEFT JOIN latest_air_quality aq ON p.city = aq.city
LEFT JOIN latest_weather w ON p.city = w.city