import requests
import tenacity

session = requests.session()

RETRY_ARGS = dict(
    wait=tenacity.wait_random(min=1, max=10),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)


@tenacity.retry(**RETRY_ARGS)
def send_webhook_message(webhook_url: str, username: str, msg: str) -> None:
    session.post(
        webhook_url,
        json={"username": username, "content": msg},
    )
