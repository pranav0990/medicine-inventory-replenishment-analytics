
"""Reusable New Relic custom-event sender for the inventory capstone."""

import json
import os
import urllib.error
import urllib.request


def send_event(event_type: str, attributes: dict) -> None:
    """Send one custom event to New Relic."""

    license_key = os.getenv("NEW_RELIC_LICENSE_KEY")
    account_id = os.getenv("NEW_RELIC_ACCOUNT_ID")
    region = os.getenv("NEW_RELIC_REGION", "US").upper()

    if not license_key:
        raise RuntimeError("NEW_RELIC_LICENSE_KEY is not configured")

    if not account_id:
        raise RuntimeError("NEW_RELIC_ACCOUNT_ID is not configured")

    if region not in {"US", "EU"}:
        raise ValueError("NEW_RELIC_REGION must be US or EU")

    if not event_type.isidentifier():
        raise ValueError("Invalid New Relic event type")

    event = {
        "eventType": event_type,
        **attributes,
    }

    if "eventType" in attributes:
        raise ValueError("Do not pass eventType inside attributes")

    endpoint = (
        "https://insights-collector.newrelic.com"
        if region == "US"
        else "https://insights-collector.eu01.nr-data.net"
    )

    url = f"{endpoint}/v1/accounts/{account_id}/events"

    request = urllib.request.Request(
        url,
        data=json.dumps([event]).encode("utf-8"),
        headers={
            "Api-Key": license_key,
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            if response.status not in (200, 202):
                raise RuntimeError(
                    f"Unexpected New Relic response: {response.status}"
                )
            print(f"New Relic accepted event: {event_type}")

    except urllib.error.HTTPError as exc:
        # Do not print credentials or the request headers.
        print(f"New Relic returned HTTP {exc.code}")
        raise
