{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT aq.city, pop.country, pop.population, aq.aqi, aq.pm2_5, aq.pm10, aq.no2, aq.measured_at,

-- Alertes simples (normes OMS)
CASE
    WHEN aq.pm2_5 > 25 THEN 'PM2.5 Alert'
    WHEN aq.pm10 > 50 THEN 'PM10 Alert'
    WHEN aq.no2 > 40 THEN 'NO2 Alert'
    ELSE 'Safe'
END AS alert_type,

-- Niveau d'alerte
CASE
    WHEN aq.pm2_5 > 50
    OR aq.pm10 > 100 THEN 'CRITICAL'
    WHEN aq.pm2_5 > 25
    OR aq.pm10 > 50
    OR aq.no2 > 40 THEN 'WARNING'
    ELSE 'SAFE'
END AS alert_level,

-- Message pour Kibana
CASE 
        WHEN aq.pm2_5 > 25 THEN aq.city || ' exceeds PM2.5 limit (' || ROUND(aq.pm2_5::numeric, 1) || ' µg/m³)'
        WHEN aq.pm10 > 50 THEN aq.city || ' exceeds PM10 limit (' || ROUND(aq.pm10::numeric, 1) || ' µg/m³)'
        ELSE 'Air quality within safe limits'
    END AS alert_message

FROM {{ ref('stg_airquality') }} aq
JOIN {{ ref('stg_population') }} pop ON aq.city = pop.city

-- ⚠️ FILTRE : Seulement les alertes réelles
WHERE
    aq.pm2_5 > 25
    OR aq.pm10 > 50
    OR aq.no2 > 40
    OR aq.aqi >= 4
ORDER BY aq.pm2_5 DESC