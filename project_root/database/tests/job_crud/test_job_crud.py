import pytest

from vvs_database import crud 

@pytest.mark.asyncio
async def test_job_create(create_job):
    job_json = {'test' : 'test'}
    job_record = await create_job(job_json=job_json)
    assert job_record.job_json == job_json

@pytest.mark.asyncio
async def test_job_get(db_session, create_job):
    job_record = await crud.get_job(db_session, 999999)
    assert job_record is None 

    job = await create_job()
    assert job is not None 

    job_record = await crud.get_job(db_session, job.id)
    assert job_record is not None 

@pytest.mark.asyncio
async def test_job_delete(db_session, create_job):
    job = await create_job()

    _ = await crud.delete_job(db_session, job)

    job_record = await crud.get_job(db_session, job.id)
    assert job_record is None 

@pytest.mark.asyncio
async def test_job_update(db_session, create_job):
    job = await create_job()

    job_json = {'test' : 'test'}
    job = await crud.update_job(db_session, job.id, job_json=job_json)
    assert job.job_json == job_json 

    job_record = await crud.get_job(db_session, job.id)
    assert job_record.job_json == job_json 

