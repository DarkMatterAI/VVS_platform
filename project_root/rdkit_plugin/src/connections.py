import pika
from sqlalchemy import text 
from cachetools import cached
from cachetools.keys import hashkey

from vvs_database import settings 

from .utils import date_print

DB_URL = settings.SQLALCHEMY_DATABASE_URL_SYNC
EXCHANGE_NAME = settings.RABBITMQ_EXCHANGE_NAME

rabbitmq_params = pika.ConnectionParameters(
    host='rabbitmq',
    port=settings.RABBITMQ_PORT,
    credentials=pika.PlainCredentials(
        settings.RABBITMQ_DEFAULT_USER,
        settings.RABBITMQ_DEFAULT_PASS
    )
)

@cached(cache={}, key=lambda engine, plugin_id: hashkey(plugin_id))
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


