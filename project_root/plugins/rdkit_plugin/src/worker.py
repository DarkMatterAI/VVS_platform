import sys
import time 
import signal

from .message_consumer import start_consumer 
from .utils import date_print

def worker():
    connection = None 
    channel = None 
    engine = None 

    def cleanup():
        date_print('Cleaning up RabbitMQ message consumer')
        if channel:
            try:
                date_print('Closing RabbitMQ channel')
                channel.stop_consuming()
            except Exception as e:
                date_print('Error stopping RabbitMQ channel: {e}')
        if connection:
            try:
                date_print('Closing RabbitMQ connection')
                connection.close()
            except Exception as e:
                date_print(f"Error closing RabbitMQ connection: {e}")

        if engine:
            try:
                date_print('Closing Postgres connection')
                engine.dispose()
            except Exception as e:
                date_print(f"Error closing Postgres connection: {e}")

    def signal_handler(signum, frame):
        date_print(f"Received signal {signum}. Initiating shutdown...")
        cleanup()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    while True:
        try:
            channel, connection, engine = start_consumer()
            channel.start_consuming()
        except Exception as e:
            date_print(f'Error in consumer: {e}')
            time.sleep(3)


