import requests
from airflow.sdk import Metadata, Variable, asset

# DEFAULT_ARGS = {
#     "owner": "Henry Lee",
#     "depends_on_past": False,
#     "start_date": datetime(2025, 2, 25),
#     "end_date": datetime(2025, 4, 9),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }


def get_proposal_summary() -> dict:
    response = requests.get(
        "https://tw.pycon.org/prs/api/proposals/summary/",
        headers={
            "Content-Type": "application/json",
            "authorization": Variable.get("PYCON_API_TOKEN"),
        },
    )
    return response.json()


@asset(
    schedule="0 16 * * *",  # At 16:00 (00:00 +8)
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
