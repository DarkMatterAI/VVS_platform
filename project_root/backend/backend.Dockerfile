FROM python:3.9-slim

WORKDIR /code

COPY ./backend/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./database /code/database
RUN pip install -e /code/database

COPY ./backend/tests /code/tests 
RUN chmod +x /code/tests/run_tests.sh
COPY ./backend/app /code/app

CMD ["fastapi", "run", "app/main.py", "--port", "7861", "--host", "0.0.0.0", "--workers", "1"]
