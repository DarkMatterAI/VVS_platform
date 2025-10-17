import time

from .worker import worker 

if __name__ == "__main__":
    # wait for rabbitmq
    time.sleep(3)

    worker()
