import os
import pika
from sqlalchemy import text 
from .utils import date_print

EXCHANGE_NAME = os.environ['RABBITMQ_EXCHANGE_NAME']

rabbitmq_params = pika.ConnectionParameters(
    host='rabbitmq',
    port=int(os.getenv('RABBITMQ_PORT', 5672)),
    credentials=pika.PlainCredentials(
        os.getenv('RABBITMQ_DEFAULT_USER'),
        os.getenv('RABBITMQ_DEFAULT_PASS')
    )
)

postgres_host = 'postgresql'
postgres_username = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_db = os.getenv('POSTGRES_DB')

DB_URL = f"postgresql://{postgres_username}:{postgres_password}@{postgres_host}/{postgres_db}"

# def get_plugin_record(engine, plugin_id):
#     date_print(f"Querying database for plugin {plugin_id}")
#     with engine.connect() as conn:
#         query = "SELECT * FROM plugins WHERE id = :id"
#         query_result = conn.execute(text(query), {"id": plugin_id})
#         record = query_result.fetchone()
#     return record._asdict() if record else {}

def get_plugin_record(engine, plugin_id):
    date_print(f"Querying database for plugin {plugin_id}")
    with engine.connect() as conn:
        query = """
        SELECT p.*, fp.*, ap.*
        FROM plugins p
        LEFT JOIN filter_plugins fp ON p.id = fp.id
        LEFT JOIN assembly_plugins ap ON p.id = ap.id
        WHERE p.id = :id
        """
        query_result = conn.execute(text(query), {"id": plugin_id})
        record = query_result.fetchone()

    return record._asdict() if record else {}

def get_plugin_from_routing_key(engine, routing_key):
    date_print(f"Fetching plugin record from routing key {routing_key}")
    _, group_key, plugin_type, plugin_id, item_id, request_id = routing_key.split('.')
    record = get_plugin_record(engine, plugin_id)
    return record, plugin_id


