SELECT
    city_name,
    weather_date,

    temp_min,
    temp_max,
    temp_avg,
    temp_max - temp_min AS temp_range,

    humidity,
    precipitation,
    wind_speed,
    weather_code,

    CASE
        WHEN precipitation > 0 THEN TRUE
        ELSE FALSE
    END AS is_rainy_day,

    CASE
        WHEN temp_avg >= 25 THEN TRUE
        ELSE FALSE
    END AS is_hot_day,

    CASE
        WHEN wind_speed >= 50 THEN TRUE
        ELSE FALSE
    END AS is_windy_day,

    ingested_at

FROM {{ ref('stg_weather') }}