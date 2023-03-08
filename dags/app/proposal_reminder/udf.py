import requests
from airflow.models import Variable


def main():
    summary = get_proposal_summary()
    send_discord_reminder(
        n_talk=summary["num_proposed_talk"],
        n_tutorial=summary["num_proposed_tutorial"],
    )


def get_proposal_summary() -> dict:
    url = "https://tw.pycon.org/prs/api/proposals/summary/"
    headers = {
        "Content-Type": "application/json",
        "authorization": Variable.get("PYCON_API_TOKEN"),
    }
    response = requests.get(url, headers=headers)
    return response.json()


def send_discord_reminder(n_talk: int, n_tutorial: int) -> None:
    webhook_url = Variable.get("DISCORD_PROGRAM_REMINDER_WEBHOOK")
    msg = f"目前投稿議程數: {n_talk}; 課程數: {n_tutorial}"
    requests.post(
        url=webhook_url, json={"username": "Program talk reminder", "content": msg},
    )
