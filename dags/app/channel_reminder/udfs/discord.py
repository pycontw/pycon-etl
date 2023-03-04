import requests
from airflow.models import Variable


def main(msg: str) -> None:
    requests.post(
        Variable.get("DISCORD_CHORES_REMINDER_WEBHOOK"),
        json={"username": "Data Team Airflow reminder", "content": msg},
    )
