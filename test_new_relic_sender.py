
from datetime import datetime, timezone

from monitoring.new_relic_events import send_event

send_event(
    "InventoryPipelineTest",
    {
        "team": "team_1",
        "pipeline": "medicine_inventory",
        "stage": "monitoring_test",
        "status": "SUCCESS",
        "durationSeconds": 2,
        "inputRows": 10,
        "rejectedRows": 0,
        "eventTime": datetime.now(timezone.utc).isoformat(),
    },
)
