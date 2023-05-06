import requests
from airflow.models import Variable
from app import discord

def main() -> None:
    summary = get_proposal_summary()
    n_talk = summary["num_proposed_talk"]
    n_tutorial = summary["num_proposed_tutorial"]
    kwargs = {
        "webhook_url": Variable.get("DISCORD_PROGRAM_REMINDER_WEBHOOK"),
        "username": "Program talk reminder",
        "msg": f"目前投稿議程數: {n_talk}; 課程數: {n_tutorial}"
    }
    discord.send_webhook_message(**kwargs)


def get_proposal_summary() -> dict:
    url = "https://tw.pycon.org/prs/api/proposals/summary/"
    headers = {
        "Content-Type": "application/json",
        "authorization": Variable.get("PYCON_API_TOKEN"),
    }
    response = requests.get(url, headers=headers)
    return response.json()
