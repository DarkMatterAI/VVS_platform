import pytest 

from vvs_database.crud import create_qdrant_upload_job
from vvs_database.schemas import CreateQdrantUploadJob, ExecutePlugin, ExecuteParams
from vvs_database.utils import object_as_dict
from vvs_database.exceptions import NotFoundError, ValidationError

plugin_api_str = '/api/v1/plugins'

user_items = [{'external_id': 'ZINC000807669219',
               'item': 'O=C(NCc1ccc(Br)cc1F)C(=O)NCC1(Cc2ccccc2)CC1'}]

@pytest.mark.asyncio
@pytest.mark.parametrize("n_embeddings", [1, 2])
@pytest.mark.parametrize("use_file", [True, False])
@pytest.mark.parametrize("use_embedding_configs", [True, False])
@pytest.mark.parametrize("use_backend", [True, False])
async def test_qdrant_upload_crud_db(db_session, 
                                     backend_client,
                                     job_cleanup,
                                     upload_test_files, 
                                     test_data_source,
                                     n_embeddings,
                                     use_file,
                                     use_embedding_configs,
                                     use_backend):
    data_record, embedding_records = test_data_source(n_embeddings)

    filename = None
    items = None 
    embedding_configs = None 

    if use_file:
        filename = 'zinc_10.csv'
        upload_test_files(filename)
    else:
        items = user_items 

    if use_embedding_configs:
        embedding_configs = [ExecutePlugin(plugin_id=record['id'],
                                           execute_params=ExecuteParams())
                             for record in embedding_records]

    create_data = CreateQdrantUploadJob(plugin_id=data_record['id'],
                                        embedding_configs=embedding_configs,
                                        filename=filename,
                                        items=items)
    
    if use_backend:
        response = backend_client.post('/api/v1/qdrant_plugins/create_upload_job',
                                    json=create_data.model_dump(),
                                    params={'is_test' : True})
        response.raise_for_status()
        job = response.json()
    else:
        job, _ = await create_qdrant_upload_job(db_session, create_data, test=True)
        job = object_as_dict(job)

    job_cleanup(job)



@pytest.mark.parametrize("test_type", ['both', 'missing_both', 'empty_items'])
def test_qdrant_upload_backend_schema_validation(backend_client,
                                                 test_data_source,
                                                 test_type):
    data_record, embedding_records = test_data_source(1)

    filename = None
    items = None
    if test_type == 'both':
        filename = 'zinc_10.csv'
        items = user_items
    elif test_type == 'empty_items':
        items = []

    create_data = {
        'plugin_id' : data_record['id'],
        'embedding_configs' : None,
        'filename' : filename,
        'items' : items
    }

    response = backend_client.post('/api/v1/qdrant_plugins/create_upload_job',
                                   json=create_data)
    assert response.status_code == 422

@pytest.mark.asyncio
@pytest.mark.parametrize("use_backend", [True, False])
async def test_qdrant_upload_missing_plugin(db_session, 
                                            backend_client, 
                                            use_backend):
    create_data = {
        'plugin_id' : 9999999,
        'embedding_configs' : None,
        'filename' : None,
        'items' : user_items
    }

    if use_backend:
        response = backend_client.post('/api/v1/qdrant_plugins/create_upload_job',
                                    json=create_data)
        assert response.status_code == 404
    else:
        create_data = CreateQdrantUploadJob(**create_data)
        with pytest.raises(NotFoundError):
            job, _ = await create_qdrant_upload_job(db_session, create_data, test=True)

@pytest.mark.asyncio
@pytest.mark.parametrize("use_backend", [True, False])
async def test_qdrant_upload_missing_file(db_session, 
                                          backend_client, 
                                          test_data_source,
                                          use_backend):
    data_record, _ = test_data_source(1)
    create_data = {
        'plugin_id' : data_record['id'],
        'embedding_configs' : None,
        'filename' : 'nonexistant_file_asbdtfynghjghtyrgef.csv',
        'items' : None
    }

    if use_backend:
        response = backend_client.post('/api/v1/qdrant_plugins/create_upload_job',
                                    json=create_data)
        assert response.status_code == 422
    else:
        create_data = CreateQdrantUploadJob(**create_data)
        with pytest.raises(ValidationError):
            job, _ = await create_qdrant_upload_job(db_session, create_data, test=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_backend", [True, False])
async def test_qdrant_upload_wrong_plugin_type(db_session, 
                                               backend_client, 
                                               test_data_source,
                                               use_backend):
    data_record, embedding_records = test_data_source(1)
    create_data = {
        'plugin_id' : embedding_records[0]['id'],
        'embedding_configs' : None,
        'filename' : None,
        'items' : user_items 
    }

    if use_backend:
        response = backend_client.post('/api/v1/qdrant_plugins/create_upload_job',
                                    json=create_data)
        assert response.status_code == 422
    else:
        create_data = CreateQdrantUploadJob(**create_data)
        with pytest.raises(ValidationError):
            job, _ = await create_qdrant_upload_job(db_session, create_data, test=True)


