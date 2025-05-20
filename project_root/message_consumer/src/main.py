import time

from .worker import worker 
from .connections import setup_rabbitmq

if __name__ == "__main__":
    # wait for rabbitmq
    time.sleep(4)

    setup_rabbitmq()
    worker()