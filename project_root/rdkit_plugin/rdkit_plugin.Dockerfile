FROM python:3.9-slim

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

RUN apt-get update && \
    apt-get install -y git && \
    git clone --depth 1 https://github.com/Laboratoire-de-Chemoinformatique/Synt-On /tmp/Synt-On && \
    mv /tmp/Synt-On /code/synt_on && \
    touch /code/synt_on/__init__.py && \
    rm -rf /tmp/Synt-On && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH="${PYTHONPATH}:/code"

COPY ./src /code/src

COPY ./tests /code/tests 

RUN chmod +x /code/tests/run_tests.sh

CMD ["python", "-u", "-m", "src.main"]

