
from sqlalchemy import text 
from dagster import In, Out, Nothing, op


def postgres_query_get_plugin(context, conn, plugin_id):
    context.log.info(f"Postgres query plugin record for plugin {plugin_id}")
    query = "SELECT * FROM plugins WHERE id = :id"
    query_result = conn.execute(text(query), {"id": plugin_id})
    record = query_result.fetchone()
    return record._asdict() if record else {}

def postgres_query_get_plugin_embeddings(context, conn, plugin_id, records=True):
    if records:
        context.log.info(f"Postgres query linked embedding records for plugin {plugin_id}")
        query = """
        SELECT p.*, ep.vector_length, ep.distance_metric
        FROM plugins p
        INNER JOIN embedding_plugins ep ON p.id = ep.id
        INNER JOIN plugin_embeddings pe ON p.id = pe.embedding_id
        WHERE pe.plugin_id = :id
        """
        query_result = conn.execute(text(query), {"id": plugin_id})
        records = [row._asdict() for row in query_result]
        record_ids = [i['id'] for i in records]
    else:
        context.log.info(f"Postgres query linked embedding ids for plugin {plugin_id}")
        query = """
        SELECT embedding_id
        FROM plugin_embeddings
        WHERE plugin_id = :id
        """
        query_result = conn.execute(text(query), {"id": plugin_id})
        records = None 
        record_ids = [row['embedding_id'] for row in query_result.mappings()]

    return records, record_ids

def postgres_query_get_plugin_with_embeddings(context, conn, plugin_id, records=True):
    record = postgres_query_get_plugin(context, conn, plugin_id)
    if record:
        embedding_records, embedding_ids = postgres_query_get_plugin_embeddings(context, conn, plugin_id, records=records)
        record['embedding_records'] = embedding_records 
        record['embedding_ids'] = embedding_ids 
    return record 


@op(
        ins={'plugin_id' : In(int)},
        out={
            'plugin_record' : Out(dict, is_required=False),
            'not_found' : Out(Nothing, is_required=False)
        },
        required_resource_keys={"postgres"}
)
def postgres_get_plugin_record(context, plugin_id):
    engine = context.resources.postgres
    with engine.connect() as conn:
        record = postgres_query_get_plugin_with_embeddings(context, conn, plugin_id)
    if record:
        return {'plugin_record' : record}
    else:
        return {'not_found' : None}

