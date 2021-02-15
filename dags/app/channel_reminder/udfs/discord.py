import os

import requests


def main(team: str, msg: str) -> None:
    requests.post(
        os.getenv(f"{team}_DISCORD_WEBHOOK", ""),
        json={"username": "Data Team Airflow reminder", "content": msg},
    )
