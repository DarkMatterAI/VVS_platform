FROM python:3.10-slim

COPY requirements_code_server.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

WORKDIR /opt/dagster/app

COPY ./dagster_code /opt/dagster/app/dagster_code

EXPOSE ${DAGSTER_CODE_SERVER_PORT}

CMD dagster api grpc -h 0.0.0.0 -p ${DAGSTER_CODE_SERVER_PORT} -m dagster_code.repository
