
import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone

license_key = os.environ.get("NEW_RELIC_LICENSE_KEY")

if not license_key:
    raise RuntimeError("NEW_RELIC_LICENSE_KEY environment variable is not set")

event = {
    "eventType": "InventoryPipelineTest",
    "team": "team_1",
    "pipeline": "medicine_inventory",
    "stage": "connectivity_test",
    "status": "SUCCESS",
    "eventTime": datetime.now(timezone.utc).isoformat()
}

# US data-ingest endpoint; use the EU endpoint if your account is in the EU.
url = "https://insights-collector.newrelic.com/v1/accounts/8612925/events"

request = urllib.request.Request(
    url,
    data=json.dumps([event]).encode("utf-8"),
    headers={
        "Api-Key": license_key,
        "Content-Type": "application/json"
    },
    method="POST"
)

try:
    with urllib.request.urlopen(request, timeout=20) as response:
        print("HTTP status:", response.status)
        print("New Relic accepted the test event.")
except urllib.error.HTTPError as error:
    print("HTTP error:", error.code)
    print(error.read().decode())
    raise
