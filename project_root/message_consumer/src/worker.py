
import sys
import time 
import signal

from .consumer import start_consumer 

def worker(is_dlx=False):
    connection = None 
    channel = None 

    def cleanup():
        print(f"Cleaning up message consumer")
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
            channel, connection = start_consumer(is_dlx)
            channel.start_consuming()
        except Exception as e:
            print(f"Worker: Error in consumer: {e}")
            time.sleep(3)  # Wait before attempting to reconnect
