from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

import os
import json
from sqlalchemy import text
from .jobs import dangerous_meteo_record_job

dangerous_meteo = [3, 48, 57, 67, 73, 75, 82, 86, 95, 96, 99]

@sensor(
    job=dangerous_meteo_record_job,
    required_resource_keys={"database"}
)
def detect_dangerous_meteo_sensor(context: SensorEvaluationContext):
    last_line_treated = json.loads(context.cursor) if context.cursor else { "time": "2025-04-20T00:00:00", "id": 0 }
    runs_to_request = []

    with context.resources.database.begin() as conn:
        recent_lines = conn.execute(text("SELECT * FROM clean_meteo WHERE (time, id) > (:time, :id) ORDER BY time, id ASC;"), 
        { "time": last_line_treated["time"], "id": last_line_treated["id"]}
        )

        for line in recent_lines:
            if line.weather_code in dangerous_meteo:
                runs_to_request.append(
                    RunRequest(
                        run_key=str(line.id),
                        run_config={
                            "ops": {
                                "dangerous_meteo_record": {
                                    "config": {
                                        "city_id": line.city_id,
                                        "time": line.time.isoformat(),
                                        "weather_code": line.weather_code,
                                        "temperature": line.temperature,
                                        "humidity": line.humidity,
                                        "precipitation": line.precipitation,
                                        "wind_speed": line.wind_speed
                                    }
                                }
                            }
                        },
                    )
                )
            
            last_line_treated = {
                "id": line.id,
                "time": line.time.isoformat()
            }

    return SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(last_line_treated)
    )