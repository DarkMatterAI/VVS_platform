
import sys
import time 
import signal
import multiprocessing

from .message_consumer import start_consumer 
from .dlx_consumer import start_consumer as start_dlx_consumer

def worker(worker_id, records, is_dlx=False):
    connection = None 
    channel = None 

    def cleanup():
        worker_id = multiprocessing.current_process().name
        print(f"Cleaning up message consumer {worker_id}")
        if channel:
            try:
                print('Closing rabbitmq channel')
                channel.stop_consuming()
            except Exception as e:
                print(f"Error stopping consumption: {e}")
        if connection:
            try:
                print('Closing rabbitmq connection')
                connection.close()
            except Exception as e:
                print(f"Error closing RabbitMQ connection: {e}")

    def signal_handler(signum, frame):
        print(f"Received signal {signum}. Initiating shutdown...")
        cleanup()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    while True:
        try:
            if is_dlx:
                channel, connection = start_dlx_consumer(worker_id)
            else:
                channel, connection = start_consumer(worker_id, records)
            channel.start_consuming()
        except Exception as e:
            print(f"Worker {worker_id}: Error in consumer: {e}")
            time.sleep(3)  # Wait before attempting to reconnect
