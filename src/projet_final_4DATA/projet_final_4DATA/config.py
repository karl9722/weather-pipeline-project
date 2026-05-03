from dagster import Config

class DangerousMeteoConfig(Config):
    city_id: int
    time: str
    weather_code: int
    temperature: float
    humidity: int
    precipitation: float
    wind_speed: float