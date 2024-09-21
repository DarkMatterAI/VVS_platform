



# from sqlalchemy import text 
# from dagster import Config, op, In, Out 

# from ..resources import PostgresResourceConfig

# class GetPluginConfig(Config):
#     plugin_id: int 

# def postgres_query_get_plugin(context, conn, plugin_id):
#     context.log.info(f"Postgres query plugin record for plugin {plugin_id}")
#     query = """
#     SELECT p.*, mp.input_embedding_id
#     FROM plugins p
#     LEFT JOIN mapper_plugins mp ON p.id = mp.id
#     WHERE p.id = :id
#     """
#     query_result = conn.execute(text(query), {"id": plugin_id})
#     record = query_result.fetchone()
#     return record._asdict() if record else {}

# def postgres_query_get_plugin_embeddings(context, conn, plugin_id):
#     context.log.info(f"Postgres query linked embedding records for plugin {plugin_id}")
#     query = """
#     SELECT p.*, ep.vector_length, ep.distance_metric
#     FROM plugins p
#     INNER JOIN embedding_plugins ep ON p.id = ep.id
#     LEFT JOIN plugin_embeddings pe ON p.id = pe.embedding_id
#     WHERE pe.plugin_id = :id OR p.id = (
#         SELECT input_embedding_id 
#         FROM mapper_plugins 
#         WHERE id = :id
#     )
#     """
#     query_result = conn.execute(text(query), {"id": plugin_id})
#     records = [row._asdict() for row in query_result]
#     record_ids = [i['id'] for i in records]

#     return records, record_ids

# def postgres_query_get_plugin_with_embeddings(context, engine, plugin_id):
#     with engine.connect() as conn:
#         record = postgres_query_get_plugin(context, conn, plugin_id)
#         if record:
#             embedding_records, embedding_ids = postgres_query_get_plugin_embeddings(context, conn, plugin_id)
#             record['embedding_records'] = embedding_records 
#             record['embedding_ids'] = embedding_ids 
#     return record 

# @op(out={"plugin_record": Out(dict)})
# def get_plugin_from_config(context, config: GetPluginConfig, postgres: PostgresResourceConfig):
#     engine = postgres.get_engine() 
#     record = postgres_query_get_plugin_with_embeddings(context, engine, config.plugin_id)
#     engine.dispose()
#     context.log.info(f"{record}")
#     return record 

# @op(
#     ins={"plugin_id" : In(int)},
#     out={"plugin_record" : Out(dict)}
# )
# def get_plugin_from_op(context, plugin_id: int, postgres: PostgresResourceConfig):
#     engine = postgres.get_engine() 
#     record = postgres_query_get_plugin_with_embeddings(context, engine, plugin_id)
#     engine.dispose()
#     context.log.info(f"{record}")
#     return record 
