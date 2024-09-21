
import sys
import time 
import signal

from .message_consumer import start_consumer 

def cleanup():
    print(f"Cleaning up message consumer")

def signal_handler(signum, frame):
    print(f"Received signal {signum}. Initiating shutdown...")
    cleanup()
    sys.exit(0)

def worker():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    while True:
        try:
            start_consumer()
        except Exception as e:
            print(f"Error in consumer: {e}")
            time.sleep(3)  # Wait before attempting to reconnect