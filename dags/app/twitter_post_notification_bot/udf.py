import requests
from airflow import settings
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker


def main() -> None:
    url = "https://twitter135.p.rapidapi.com/v2/UserTweets/"
    # 499339900 is PyConTW's twitter id
    querystring = {"id": "499339900", "count": "1"}
    headers = {
        "X-RapidAPI-Key": Variable.get("RAPIDAPIAPI_KEY"),
        "X-RapidAPI-Host": "twitter135.p.rapidapi.com",
    }
    webhook_url = Variable.get("DISCORD_POST_NOTIFICATION_WEBHOOK")
    response = requests.get(url, headers=headers, params=querystring)
    response_json = response.json()
    try:
        Session = sessionmaker(bind=settings.engine)
        # Update the variable using a context manager
        variable_key = "TWITTER_LATEST_REST_ID"
        rest_id = response_json["data"]["user"]["result"]["timeline_v2"]["timeline"][
            "instructions"
        ][1]["entries"][0]["content"]["itemContent"]["tweet_results"]["result"][
            "rest_id"
        ]
        full_text = response_json["data"]["user"]["result"]["timeline_v2"]["timeline"][
            "instructions"
        ][1]["entries"][0]["content"]["itemContent"]["tweet_results"]["result"][
            "legacy"
        ]["full_text"]
        rest_id_in_DB = Variable.get(variable_key)
        if rest_id_in_DB < rest_id:
            # Create a session
            session = Session()

            # Query the variable by key
            variable = session.query(Variable).filter_by(key=variable_key).first()

            # Update the variable value
            variable.set_val(rest_id)

            msg = f"new twitter post: https://twitter.com/PyConTW/status/{rest_id}\n\n{full_text}"
            requests.post(
                url=webhook_url,
                json={"username": "Twitter Post Notification", "content": msg},
            )

            # Commit the changes to the database
            session.commit()

            # Close the session
            session.close()
    except Exception:
        requests.post(
            url=webhook_url,
            json={
                "username": "Twitter Post Notification",
                "content": str(response_json),
            },
        )
