import os 
import sys 
import time 
import signal
import multiprocessing

from .crud_records import create_records, delete_records
from .worker import worker

class SignalHandler:
    def __init__(self, records):
        self.records = records

    def handle(self, signum, frame):
        print(f"Main process received signal {signum}. Terminating workers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
        delete_records(self.records)
        sys.exit(0)

if __name__ == "__main__":
    # wait for rabbitmq
    time.sleep(3)

    print("Creating mock workers")
    records = create_records()
    
    num_workers = int(os.getenv('TEST_CONSUMER_WORKERS', 1))
    print(f"Starting {num_workers} workers...")
    
    processes = []
    for i in range(num_workers):
        worker_id = f"W{i+1}"
        p = multiprocessing.Process(target=worker, args=(worker_id, records, ), name=worker_id)
        p.start()
        processes.append(p)

    worker_id = f"W{i+2}"
    p = multiprocessing.Process(target=worker, args=(worker_id, records, True), name=worker_id)
    p.start()
    processes.append(p)
    
    # Set up signal handling for the main process
    signal_handler = SignalHandler(records)
    signal.signal(signal.SIGTERM, signal_handler.handle)
    signal.signal(signal.SIGINT, signal_handler.handle)
    
    for p in processes:
        p.join()
