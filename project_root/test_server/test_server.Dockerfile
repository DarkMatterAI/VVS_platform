FROM python:3.9-slim

WORKDIR /code

COPY ./test_server/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./database /code/database
RUN pip install -e /code/database

COPY ./test_server/app /code/app

CMD ["fastapi", "run", "app/main.py", "--port", "7862", "--host", "0.0.0.0", "--workers", "1"]
