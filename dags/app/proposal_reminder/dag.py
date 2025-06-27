from typing import Any

import requests
from airflow.sdk import Metadata, Variable, asset


def get_proposal_summary() -> dict[str, Any]:
    response = requests.get(
        "https://tw.pycon.org/prs/api/proposals/summary/",
        headers={
            "Content-Type": "application/json",
            "authorization": Variable.get("PYCON_API_TOKEN"),
        },
    )
    response.raise_for_status()
    return response.json()


@asset(
    schedule="0 16 * * *",  # At 16:00 (00:00 +8)
    is_paused_upon_creation=True,
    tags=["discord"],
)
def proposal_count(self):
    summary = get_proposal_summary()
    n_talk = summary["num_proposed_talk"]
    n_tutorial = summary["num_proposed_tutorial"]

    yield Metadata(
        self,
        extra={
            "webhook_endpoint_key": "DISCORD_PROGRAM_REMINDER_WEBHOOK",
            "username": "Program talk reminder",
            "content": f"目前投稿議程數: {n_talk}; 課程數: {n_tutorial}",
        },
    )
