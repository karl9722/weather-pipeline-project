from dagster import Definitions, load_assets_from_modules
from .resources import database
from .jobs import delete_old_data_job, hourly_data_refresh_job, daily_metrics_meteo_job, dangerous_meteo_record_job
from .schedules import clean_old_data_schedule, hourly_data_refresh_schedule, daily_metrics_meteo_schedule
from .sensors import detect_dangerous_meteo_sensor

from . import assets

all_assets = load_assets_from_modules([assets])
all_jobs = [delete_old_data_job, hourly_data_refresh_job, daily_metrics_meteo_job, dangerous_meteo_record_job]
all_schedules = [clean_old_data_schedule, hourly_data_refresh_schedule, daily_metrics_meteo_schedule]
all_sensors= [detect_dangerous_meteo_sensor]

defs = Definitions(
    assets=all_assets,
    resources={
        "database": database
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors
)
