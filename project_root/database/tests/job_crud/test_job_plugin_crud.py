import pytest
import sqlalchemy

from vvs_database import crud 
from vvs_database.exceptions import ReferenceError

@pytest.mark.asyncio
async def test_job_plugin_create(db_session,
                                 create_job, 
                                 create_test_embedding):
    job = await create_job()
    plugin = await create_test_embedding()
    job_plugin = await crud.create_job_plugin(db_session, job.id, plugin.id)
    assert job_plugin.job_id == job.id 
    assert job_plugin.plugin_id == plugin.id 

@pytest.mark.asyncio
async def test_job_plugin_create_fails_invalid_ids(db_session):
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        job_plugin = await crud.create_job_plugin(db_session, 10000000, 20000000)

@pytest.mark.asyncio
async def test_job_plugin_get(db_session, create_job_plugin):
    job_plugin = await crud.get_job_plugin(db_session, 999999999, 999999999)
    assert job_plugin is None 

    job_json = {'test' : 'test'}
    job, plugin, job_plugin = await create_job_plugin(job_json=job_json)

    job_plugin_get = await crud.get_job_plugin(db_session, job.id, plugin.id)
    assert job_plugin_get is not None 
    assert job_plugin_get.job_id == job.id
    assert job_plugin_get.plugin_id == plugin.id 
    await db_session.commit()

@pytest.mark.asyncio
async def test_get_job_plugins(db_session,
                               create_job, 
                               create_test_embedding):
    job = await create_job()
    n_plugins = 3
    plugin_ids = []
    for i in range(n_plugins):
        plugin = await create_test_embedding()
        plugin_ids.append(plugin.id)

    job_plugins = await crud.bulk_create_job_plugins(db_session, job.id, plugin_ids)
    assert len(job_plugins) == n_plugins 

    job_plugins_get = await crud.get_job_plugins(db_session, job.id)
    assert len(job_plugins_get) == n_plugins 

    job_record = await crud.get_job(db_session, job.id, load_plugins=True)
    for jp in job_record.plugins:
        assert jp.plugin_id in plugin_ids
        assert jp.plugin.id in plugin_ids
    await db_session.commit()

@pytest.mark.asyncio
async def test_job_plugin_delete(db_session, 
                                 create_job_plugin):
    job, plugin, job_plugin = await create_job_plugin()
    result = await crud.delete_job_plugin(db_session, job_plugin)

    job_plugin_get = await crud.get_job_plugin(db_session, job.id, plugin.id)
    assert job_plugin_get is None 
    await db_session.commit()

@pytest.mark.asyncio
async def test_job_plugin_delete_job_propagation(db_session, 
                                                 create_job_plugin):
    job, plugin, job_plugin = await create_job_plugin()
    _ = await crud.delete_job(db_session, job)

    job_plugin_get = await crud.get_job_plugin(db_session, job.id, plugin.id)
    assert job_plugin_get is None 

    response = await crud.get_plugin(db_session, plugin.id)
    assert response is not None 
    await db_session.commit()

# @pytest.mark.asyncio
# async def test_job_plugin_delete_plugin_propagation(db_session, 
#                                                  create_job_plugin):
#     job, plugin, job_plugin = await create_job_plugin()
#     _ = await crud.delete_plugin(db_session, plugin.id)

#     job_plugin_get = await crud.get_job_plugin(db_session, job.id, plugin.id)
#     assert job_plugin_get is None 

#     response = await crud.get_job(db_session, job.id)
#     assert response is not None 
#     await db_session.commit()

@pytest.mark.asyncio
async def test_plugin_delete_blocked_then_allowed_after_job_delete(
    db_session,
    create_job_plugin,
):
    # 1) create job, plugin, job-plugin link
    job, plugin, job_plugin = await create_job_plugin()

    # 2) delete plugin → should raise ReferenceError due to preflight
    with pytest.raises(ReferenceError):
        await crud.delete_plugin(db_session, plugin.id)

    # verify the association still exists and job still present
    assert await crud.get_job_plugin(db_session, job.id, plugin.id) is not None
    assert await crud.get_job(db_session, job.id) is not None

    # 3) delete the job
    await crud.delete_job(db_session, job)
    assert await crud.get_job(db_session, job.id) is None

    # 4) delete the plugin (now allowed)
    deleted = await crud.delete_plugin(db_session, plugin.id)
    assert deleted is not None

    # verify plugin is gone and association is gone
    assert await crud.get_plugin(db_session, plugin.id, with_error=False) is None
    assert await crud.get_job_plugin(db_session, job.id, plugin.id) is None
    await db_session.commit()
