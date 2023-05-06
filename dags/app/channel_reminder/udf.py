from app import discord
from airflow.models import Variable

def main() -> None:
    kwargs = {
        "webhook_url": Variable.get("DISCORD_CHORES_REMINDER_WEBHOOK"),
        "username": "Data Team Airflow reminder",
        "msg": (
            "<@&790739794148982796> <@&755827317904769184> <@&791157626099859487>\n",
            "記得大會結束後，要有一個人負責去取消 Klaviyo 的訂閱，不然我們每個月會一直繳 $NTD2000 喔！"
        )
    }
    discord.send_webhook_message(**kwargs)
