FROM python:3.10-slim

WORKDIR /code

# COPY ./requirements.txt /code/requirements.txt
COPY ./tests/test_consumer/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# COPY ./src /code/src
COPY ./tests/test_consumer/src /code/src

CMD ["python", "-u", "-m", "src.main"]

