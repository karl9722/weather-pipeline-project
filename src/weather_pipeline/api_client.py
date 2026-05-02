import os
import time
import requests
import psycopg
from datetime import date
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "weather_db"),
        user=os.getenv("POSTGRES_USER", "weather_user"),
        password=os.getenv("POSTGRES_PASSWORD", "weather_pass"),
    )


def geocode_city(city_name: str):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{city_name}, France",
        "format": "json",
        "limit": 1,
        "addressdetails": 1,
    }
    headers = {
        "User-Agent": "weather-pipeline-student-project"
    }

    response = requests.get(url, params=params, headers=headers, timeout=20)
    response.raise_for_status()

    results = response.json()
    if not results:
        raise ValueError(f"Aucune coordonnée trouvée pour {city_name}")

    item = results[0]
    address = item.get("address", {})

    return {
        "city_name": city_name,
        "department": address.get("county") or address.get("state"),
        "latitude": float(item["lat"]),
        "longitude": float(item["lon"]),
    }


def fetch_weather(city_name: str, latitude: float, longitude: float):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": ",".join([
            "temperature_2m_min",
            "temperature_2m_max",
            "temperature_2m_mean",
            "precipitation_sum",
            "wind_speed_10m_max",
            "weather_code",
        ]),
        "hourly": "relative_humidity_2m",
        "timezone": "Europe/Paris",
        "forecast_days": 1,
    }

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    daily = data["daily"]

    # Humidité moyenne sur les 24 heures
    humidity_values = data.get("hourly", {}).get("relative_humidity_2m", [])
    humidity_avg = (
        sum(humidity_values) / len(humidity_values)
        if humidity_values
        else None
    )

    return {
        "city_name": city_name,
        "weather_date": daily["time"][0],
        "temperature_min": daily["temperature_2m_min"][0],
        "temperature_max": daily["temperature_2m_max"][0],
        "temperature_avg": daily["temperature_2m_mean"][0],
        "humidity": humidity_avg,
        "precipitation": daily["precipitation_sum"][0],
        "wind_speed": daily["wind_speed_10m_max"][0],
        "weather_code": daily["weather_code"][0],
    }


def insert_city(city):
    query = """
        INSERT INTO raw.cities_raw
        (city_name, department, latitude, longitude)
        VALUES (%s, %s, %s, %s);
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    city["city_name"],
                    city["department"],
                    city["latitude"],
                    city["longitude"],
                ),
            )


def insert_weather(weather):
    query = """
        INSERT INTO raw.weather_raw
        (
            city_name,
            weather_date,
            temperature_min,
            temperature_max,
            temperature_avg,
            humidity,
            precipitation,
            wind_speed,
            weather_code
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    weather["city_name"],
                    weather["weather_date"],
                    weather["temperature_min"],
                    weather["temperature_max"],
                    weather["temperature_avg"],
                    weather["humidity"],
                    weather["precipitation"],
                    weather["wind_speed"],
                    weather["weather_code"],
                ),
            )


def run_ingestion():
    cities = os.getenv("CITIES", "Paris,Lyon,Marseille").split(",")

    inserted_rows = 0

    for city_name in cities:
        city_name = city_name.strip()

        print(f"Traitement de la ville : {city_name}")

        city = geocode_city(city_name)
        insert_city(city)

        # Nominatim demande de ne pas faire trop de requêtes rapides
        time.sleep(1)

        weather = fetch_weather(
            city["city_name"],
            city["latitude"],
            city["longitude"],
        )
        insert_weather(weather)

        inserted_rows += 1

    print(f"Ingestion terminée : {inserted_rows} lignes météo insérées")


if __name__ == "__main__":
    run_ingestion()