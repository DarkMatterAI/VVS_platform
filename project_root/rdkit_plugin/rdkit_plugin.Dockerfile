FROM python:3.9-slim

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./src /code/src

COPY ./tests /code/tests 

RUN chmod +x /code/tests/run_tests.sh

CMD ["python", "-u", "-m", "src.main"]

