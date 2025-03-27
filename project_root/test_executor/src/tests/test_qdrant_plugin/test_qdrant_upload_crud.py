import pytest 

from tests.utils.backend_utils import backend_delete_plugin

from vvs_database.crud import create_qdrant_upload_job
from vvs_database.schemas import CreateQdrantUploadJob

plugin_api_str = '/api/v1/plugins'

@pytest.mark.asyncio
async def test_qdrant_upload_crud_db(db_session, 
                                     backend_client, 
                                     upload_test_files, 
                                     test_data_source):
    upload_test_files('zinc_10.csv')

    data_record, _ = test_data_source(1)

    create_data = CreateQdrantUploadJob(plugin_id=data_record['id'],
                                        embedding_configs=None,
                                        filename='zinc_10.csv')
    
    # job, job_plugins = await create_qdrant_upload_job(db_session, create_data, test=True)
    # backend_delete_plugin(backend_client, plugin_api_str, data_record)
    # backend_delete_plugin(backend_client, plugin_api_str, embedding_record)


