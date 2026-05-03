from dagster import define_asset_job, AssetSelection

meteo_metrics = AssetSelection.assets("meteo_metrics")
dangerous_meteo_record = AssetSelection.assets("dangerous_meteo_record")
delete_old_data = AssetSelection.assets("delete_old_data")
get_coordinates = AssetSelection.assets("get_coordinates") # On le récupère juste pour l'empêcher d'être exécuter au-delà de la première fois

hourly_data_refresh_job = define_asset_job(
    name="hourly_data_refresh_job",
    selection=AssetSelection.all() - meteo_metrics - delete_old_data - get_coordinates - dangerous_meteo_record
)

daily_metrics_meteo_job = define_asset_job(
    name="daily_metrics_meteo_job",
    selection=meteo_metrics
)

delete_old_data_job = define_asset_job(
    name="delete_old_data_job",
    selection=delete_old_data
)

dangerous_meteo_record_job = define_asset_job(
    name="dangerous_meteo_record_job",
    selection=dangerous_meteo_record
)