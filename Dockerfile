FROM apache/airflow:2.1.3-python3.8
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    GOOGLE_APPLICATION_CREDENTIALS='/usr/local/airflow/service-account.json'

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && chmod -R 777 /usr/local/lib/python3.8/site-packages \
    && chmod 777 -R /usr/local/include/python3.8/ \
    && chmod -R 777 /usr/local/bin/
USER airflow
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock
RUN pip install --no-cache-dir poetry==1.1.8 \
    && poetry export --without-hashes -f requirements.txt -o ./requirements.txt \
    # flask-openid does not correctly specify version constraints https://github.com/python-poetry/poetry/issues/1287
    && echo "remove python-openid from poetry packages as it's pulled in incorrectly by flask-openid" \
    && sed -i '/^python-openid==/d' ./requirements.txt \
    && pip install --user --no-cache-dir --upgrade pip==21.1.2 \
    && pip install --user --no-cache-dir --no-warn-script-location -r ./requirements.txt \
    && rm -rf ~/.cache ./requirements.txt \
    && rm -rf "$POETRY_CACHE_DIR" \
    && pip uninstall -yq poetry