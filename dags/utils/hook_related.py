import tenacity

RETRY_ARGS = {
    "stop": tenacity.stop_after_attempt(10),
    "wait": tenacity.wait_fixed(120),
    "reraise": True,
}
