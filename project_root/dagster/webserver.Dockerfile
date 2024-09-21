FROM python:3.10-slim

COPY requirements_webserver.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME

COPY dagster.yaml workspace.yaml $DAGSTER_HOME

WORKDIR $DAGSTER_HOME