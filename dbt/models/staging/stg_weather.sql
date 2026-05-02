SELECT
    weather_id,
    city_name,
    weather_date,
    temperature_min AS temp_min,
    temperature_max AS temp_max,
    temperature_avg AS temp_avg,
    humidity,
    precipitation,
    wind_speed,
    weather_code,
    ingested_at
FROM raw.weather_raw
WHERE city_name IS NOT NULL
  AND weather_date IS NOT NULL