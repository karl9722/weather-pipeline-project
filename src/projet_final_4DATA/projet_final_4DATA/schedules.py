from dagster import ScheduleDefinition
from .jobs import delete_old_data_job, hourly_data_refresh_job, daily_metrics_meteo_job

# Schedule de maintenance
clean_old_data_schedule = ScheduleDefinition(
    job=delete_old_data_job,
    cron_schedule="0 2 * 1 *", # Tous les lundis 1 du mois à 2 heures du matin
)

# Schedule de récupération des données météorologiques brutes
hourly_data_refresh_schedule = ScheduleDefinition(
    job=hourly_data_refresh_job,
    cron_schedule="0 * * * *" # Toute les heures
)

# Schedule de calcul des métriques sur les données météorologiques
daily_metrics_meteo_schedule = ScheduleDefinition(
    job=daily_metrics_meteo_job,
    cron_schedule="0 2 * * *", # Tous les jours à 2 heures du matin
)