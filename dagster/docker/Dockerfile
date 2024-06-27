FROM python:3.10-slim as poetry-export


WORKDIR /export_staging

# install curl
RUN apt-get update && apt-get install curl -y

# install poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# copy poetry files
COPY pyproject.toml poetry.lock poetry.toml /export_staging/

# export dependencies into requirements.txt
RUN /root/.local/bin/poetry export -f requirements.txt --output requirements.txt --without-hashes
FROM python:3.10-slim

COPY --from=poetry-export /export_staging/requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Set DAGSTER_HOME, then copy dagster config and workspace YAML into it
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

COPY docker/dagster.yaml $DAGSTER_HOME
COPY docker/workspace.yaml $DAGSTER_HOME

COPY /src $DAGSTER_HOME/src


WORKDIR $DAGSTER_HOME

