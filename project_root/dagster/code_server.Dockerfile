FROM python:3.10-slim

WORKDIR /opt/dagster/app

COPY ./database /opt/dagster/app/database

RUN pip install -e /opt/dagster/app/database

COPY ./dagster/vvs_dagster /opt/dagster/app/vvs_dagster

RUN pip install -e /opt/dagster/app/vvs_dagster

EXPOSE ${DAGSTER_CODE_SERVER_PORT}

CMD dagster api grpc -h 0.0.0.0 -p ${DAGSTER_CODE_SERVER_PORT} -m vvs_dagster.definitions
