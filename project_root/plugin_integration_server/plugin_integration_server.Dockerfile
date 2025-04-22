FROM python:3.10-slim

WORKDIR /code

COPY ./plugin_integration_server/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./database /code/database
RUN pip install -e /code/database

COPY ./plugin_integration_server/app /code/app

CMD ["fastapi", "run", "app/main.py", "--port", "7862", "--host", "0.0.0.0", "--workers", "1"]
