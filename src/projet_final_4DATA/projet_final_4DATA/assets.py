import requests
import pandas as pd
from datetime import datetime
from .config import DangerousMeteoConfig
from sqlalchemy import text
from dagster import asset, AutoMaterializePolicy

@asset(
    auto_materialize_policy=AutoMaterializePolicy.lazy(), # Empêche l'asset d'être matérialisé à chaque fois que les autres assets le sont
    required_resource_keys={"database"}
)
def get_coordinates(context) -> None:
    """ Récupère les coordonnées des villes depuis l'API Nominatim """

    raw_cities_coordinates = requests.get(
        "https://geo.api.gouv.fr/communes?fields=nom,centre,codeDepartement"
    ).json() # Récupère le nom et les coordonnées (dans le format suivante : "centre": { "type": "Point", "coordinates": [longitude, latitude] } )

    # On crée un dataframe qui coller à la table pour ne récupérer que les éléments qui nous intéressent de manière propre (on retire toute la partie centre pour ne garder que longitude/latitude, et on n'ajoute pas si les villes n'ont pas de coordonnées)
    cities_df = pd.json_normalize(raw_cities_coordinates)
    
    cities_df = cities_df[cities_df["centre.coordinates"].notna()]
    cities_df['longitude'] = cities_df["centre.coordinates"].apply(lambda x: x[0])
    cities_df['latitude'] = cities_df["centre.coordinates"].apply(lambda x: x[1])
    cities_df = cities_df[["nom", "latitude", "longitude", "codeDepartement"]]
    
    cities_df = cities_df.rename(columns={"nom": "name", "codeDepartement": "department"}) # On renomme les colonne pour la lisiblité et consistance dans les noms
    cities_df = cities_df.head(10) # On limite à 10 car sinon c'est très long à générer

    context.log.info("Fin du traitement de la table")

    # On écrit le tout dans la table cities
    with context.resources.database.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS cities CASCADE;"))
        conn.execute(text("""CREATE TABLE IF NOT EXISTS cities (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, department VARCHAR(100), latitude FLOAT, longitude FLOAT);"""))
        
        context.log.info("Fin de la création de la table")

        conn.execute(text("""
            INSERT INTO cities (name, department, latitude, longitude)
            VALUES (:name, :department, :latitude, :longitude)
        """), cities_df[["name", "department", "latitude", "longitude"]].to_dict(orient="records"))

        result = conn.execute(text("SELECT COUNT(*) FROM cities"))
        context.log.info(conn.execute(text("SELECT current_database(), current_schema()")).fetchall())

        conn.commit()


@asset(
    deps=["get_coordinates"],
    required_resource_keys={"database"}
)
def get_raw_meteo_data(context) -> None:
    """ Récupère temporairement les données météorologiques brutes """

    with context.resources.database.begin() as conn:
        cities = pd.read_sql("SELECT id, name, latitude, longitude FROM cities", conn)
        meteo_dataframe = []

        for index, city in cities.iterrows():
            raw_meteo_data = requests.get(
                f"https://api.open-meteo.com/v1/forecast?latitude={city['latitude']}&longitude={city['longitude']}&hourly=temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m&past_days=0&forecast_days=1"
            ).json()

            if "hourly" not in raw_meteo_data:
                context.log.warning(f"API KO pour city_id={city['id']} -> {raw_meteo_data}")
                continue

            temp_df = pd.DataFrame(raw_meteo_data['hourly'])
            temp_df['city_id'] = city['id']
            meteo_dataframe.append(temp_df)
        
        meteo_dataframe = pd.concat(meteo_dataframe, ignore_index=True) # Ignore index permet d'ignorer les index internes des dataframes contenus dans la liste
        meteo_dataframe = meteo_dataframe.rename(columns={"temperature_2m": "temperature", "relative_humidity_2m": "humidity", "wind_speed_10m": "wind_speed" }) # Même raison que pour l'asset précédent
        
        # conn.execute(text("DROP TABLE IF EXISTS raw_meteo CASCADE;"))
        # conn.execute(text("CREATE TABLE IF NOT EXISTS raw_meteo (id SERIAL PRIMARY KEY, city_id INT REFERENCES cities(id), time TIMESTAMP, weather_code INT, temperature FLOAT, humidity INT, precipitation FLOAT, wind_speed FLOAT);"))

        conn.execute(text("TRUNCATE TABLE raw_meteo;")) # On la vide car elle sert que de table intermédiaire pour stocker mes données avant le nettoyage
        conn.execute(text("""
            INSERT INTO raw_meteo (city_id, time, temperature, weather_code, humidity, precipitation, wind_speed)
            VALUES (:city_id, :time, :temperature, :weather_code, :humidity, :precipitation, :wind_speed)
        """), meteo_dataframe[["city_id", "time", "temperature", "weather_code", "humidity", "precipitation", "wind_speed"]].to_dict(orient="records")
)

        conn.commit()

@asset(
    deps=["get_raw_meteo_data"],
    required_resource_keys={"database"}
)
def clean_meteo_data(context) -> None:
    """ Utilise les données brutes récupérés précédemment puis les nettoie pour ne garder que les informations utiles"""
    
    columns_to_fill = ['temperature', 'humidity', 'precipitation', 'wind_speed']

    with context.resources.database.begin() as conn:
        clean_meteo_dataframe = pd.read_sql("SELECT * FROM raw_meteo WHERE time >= NOW() - INTERVAL '1 day';", conn)

        clean_meteo_dataframe = clean_meteo_dataframe.sort_values(['city_id', 'time']) # On trie par ville, puis par temps pour faire les opérations d'après
        clean_meteo_dataframe[columns_to_fill] = (clean_meteo_dataframe.groupby('city_id')[columns_to_fill] .transform(lambda x: x.interpolate()) ) # Remplit les valeurs vides par une interpolation, car une moyenne serait incohérent pour des données météos, idem pour minimum/maximum
        clean_meteo_dataframe[columns_to_fill] = (clean_meteo_dataframe.groupby('city_id')[columns_to_fill].ffill().bfill()) # Si rien n'a été récupéré précédemment, on prend la valeur au dessus la plus proche, et si toujours rien, la plus basse
        clean_meteo_dataframe['weather_code'] = (clean_meteo_dataframe.groupby('city_id')['weather_code'].ffill()) # Si une valeur est manquante, on remplit par le dernier code météo récupéré pour une ville, car généralement la météo ne change pas brutalement en 1 heure.

        clean_meteo_dataframe = clean_meteo_dataframe.drop_duplicates(subset=['city_id', 'time']) # On supprime tous les doublons d'une ville sur une même heure

        # conn.execute(text("DROP TABLE IF EXISTS clean_meteo CASCADE;"))
        # conn.execute(text("CREATE TABLE IF NOT EXISTS clean_meteo (id SERIAL PRIMARY KEY, city_id INT REFERENCES cities(id), time TIMESTAMP, weather_code INT, temperature FLOAT, humidity INT, precipitation FLOAT, wind_speed FLOAT);"))
        
        conn.execute(text("DELETE FROM clean_meteo WHERE time >= CURRENT_DATE;"))
        conn.execute(text("""
            INSERT INTO clean_meteo (city_id, time, temperature, weather_code, humidity, precipitation, wind_speed)
            VALUES (:city_id, :time, :temperature, :weather_code, :humidity, :precipitation, :wind_speed)
        """), clean_meteo_dataframe.to_dict(orient="records"))
        
        conn.commit()

# Pas nécessaire finalement puisque c'est fait plus haut lors de la récupération des données brutes
# @asset
# def agregate_cities_and_meteo() -> None:
#     """ Associe les données météos au ville en utilisant les coordonnées """

@asset(
    deps=["clean_meteo_data"],
    required_resource_keys={"database"}
)
def meteo_metrics(context) -> None:
    """ Réalise des calculs et moyonnes sur les données météos de chaque ville """

    with context.resources.database.begin() as conn:
        # conn.execute(text("DROP TABLE IF EXISTS metrics_meteo CASCADE;")) # A enlever, ne sert que pour tester niveau dagster
        # conn.execute(text("CREATE TABLE IF NOT EXISTS metrics_meteo (id SERIAL PRIMARY KEY, city_id INT REFERENCES cities(id), time TIMESTAMP, weather_code_avg INT, weather_code_worst INT, temperature_avg FLOAT, temperature_min FLOAT, temperature_max FLOAT, humidity_avg FLOAT, precipitation_avg FLOAT, precipitation_sum FLOAT, wind_speed_avg FLOAT, wind_speed_max FLOAT);"))
        # Enlever les requêtes du dessus une fois les tests dagster fini
        
        conn.execute(text("DELETE FROM metrics_meteo WHERE time >= CURRENT_DATE;"))
        conn.execute(text("""
            INSERT INTO metrics_meteo (
                city_id,
                time,
                weather_code_avg,
                weather_code_worst,
                temperature_avg,
                temperature_min,
                temperature_max,
                humidity_avg,
                precipitation_avg,
                precipitation_sum,
                wind_speed_avg,
                wind_speed_max
            )
            WITH base AS (
                SELECT
                    city_id,
                    DATE(time) AS day,
                    temperature,
                    humidity,
                    precipitation,
                    wind_speed,
                    weather_code
                FROM clean_meteo
                WHERE time >= NOW() - INTERVAL '1 day'
            ),

            meteo_metrics_calc AS (SELECT
                    city_id,
                    day,
                    weather_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY city_id, day
                        ORDER BY COUNT(*) DESC
                    ) AS rn
                FROM base
                GROUP BY city_id, day, weather_code
            )

            SELECT
                base.city_id,
                base.day,
                meteo_metrics_calc.weather_code AS weather_code_avg,
                MAX(base.weather_code) AS weather_code_worst,
                ROUND(AVG(base.temperature)::numeric, 2) AS temperature_avg,
                MIN(base.temperature) AS temperature_min,
                MAX(base.temperature) AS temperature_max,
                ROUND(AVG(base.humidity)::numeric, 2) AS humidity_avg,
                ROUND(AVG(base.precipitation)::numeric, 2) AS precipitation_avg,
                SUM(base.precipitation) AS precipitation_sum,
                ROUND(AVG(base.wind_speed)::numeric, 2) AS wind_speed_avg,
                MAX(base.wind_speed) AS wind_speed_max
            FROM base
            JOIN meteo_metrics_calc
                ON base.city_id = meteo_metrics_calc.city_id
                AND base.day = meteo_metrics_calc.day
                AND meteo_metrics_calc.rn = 1
            GROUP BY base.city_id, base.day, meteo_metrics_calc.weather_code;
            """
        ))
        
        conn.commit()

@asset(
    required_resource_keys={"database"}
)
def delete_old_data(context) -> None:
    """ Supprime les données météos qui ne sont plus utiles (plus vieille que le jour actuel) """

    with context.resources.database.begin() as conn:
        conn.execute(text("DELETE FROM clean_meteo WHERE time < NOW() - INTERVAL '60 day'"))
        conn.execute(text("DELETE FROM metrics_meteo WHERE time < NOW() - INTERVAL '365 day'"))
        conn.execute(text("DELETE FROM dangerous_meteo WHERE time < NOW() - INTERVAL '365 day'"))

@asset(
    required_resource_keys={"database"}
)
def dangerous_meteo_record(context, config: DangerousMeteoConfig) -> None:
    """ Insère les données de météo dangereuses détectés par le capteur """
    
    with context.resources.database.begin() as conn:
        # conn.execute(text("DROP TABLE IF EXISTS dangerous_meteo CASCADE;")) # A enlever, ne sert que pour tester niveau dagster
        # conn.execute(text("CREATE TABLE IF NOT EXISTS dangerous_meteo (id SERIAL PRIMARY KEY, city_id INT REFERENCES cities(id), time TIMESTAMP, weather_code INT, temperature FLOAT, humidity INT, precipitation FLOAT, wind_speed FLOAT);"))
        # Enlever les requêtes du dessus une fois les tests dagster fini

        conn.execute(text("""
            INSERT INTO dangerous_meteo (city_id, time, weather_code, temperature, humidity, precipitation, wind_speed)
            VALUES (:city_id, :time, :weather_code, :temperature, :humidity, :precipitation, :wind_speed)
        """), { "city_id": config.city_id, "time": datetime.fromisoformat(config.time), "weather_code": config.weather_code, "temperature": config.temperature, "humidity": config.humidity, "precipitation": config.precipitation, "wind_speed": config.wind_speed,})