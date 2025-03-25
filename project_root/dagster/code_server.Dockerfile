FROM python:3.10-slim

# COPY ./database /code/database
# RUN pip install -e /code/database

WORKDIR /opt/dagster/app

COPY ./database /opt/dagster/app/database

RUN pip install -e /opt/dagster/app/database

COPY ./dagster/vvs_dagster /opt/dagster/app/vvs_dagster

RUN pip install -e /opt/dagster/app/vvs_dagster

# COPY ./getting_started_etl_tutorial /opt/dagster/app/getting_started_etl_tutorial

# RUN mv /opt/dagster/app/getting_started_etl_tutorial/data /opt/dagster/app/data

# RUN pip install -e /opt/dagster/app/getting_started_etl_tutorial

EXPOSE ${DAGSTER_CODE_SERVER_PORT}

CMD dagster api grpc -h 0.0.0.0 -p ${DAGSTER_CODE_SERVER_PORT} -m vvs_dagster.definitions
# CMD dagster api grpc -h 0.0.0.0 -p ${DAGSTER_CODE_SERVER_PORT} -m getting_started_etl_tutorial.etl_tutorial.definitions
