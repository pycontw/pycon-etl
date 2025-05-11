import requests
from airflow.models import Variable


def get_proposal_summary() -> dict:
    url = "https://tw.pycon.org/prs/api/proposals/summary/"
    headers = {
        "Content-Type": "application/json",
        "authorization": Variable.get("PYCON_API_TOKEN"),
    }
    response = requests.get(url, headers=headers)
    return response.json()
