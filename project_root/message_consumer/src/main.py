import time
import argparse 

from .worker import worker 
from .connections import setup_rabbitmq


def parse_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--is_dlx", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    # wait for rabbitmq
    time.sleep(3)

    setup_rabbitmq()

    worker(args.is_dlx)
