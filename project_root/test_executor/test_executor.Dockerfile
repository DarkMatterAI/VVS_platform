FROM python:3.10-slim

WORKDIR /code

COPY ./test_executor/requirements.txt /code/requirements.txt
RUN pip install -r requirements.txt

COPY ./database /code/database
RUN pip install -e /code/database

COPY ./test_executor/src/ .

CMD ["python", "-u", "main.py"]

