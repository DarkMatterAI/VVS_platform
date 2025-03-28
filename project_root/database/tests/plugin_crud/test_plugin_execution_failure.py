import pytest
from vvs_database import crud 

@pytest.mark.asyncio
async def test_plugin_execution_failure(db_session, create_test_embedding):
    plugin = await create_test_embedding()

    records = [{
        "plugin_id": plugin.id,
        "job_id": None,
        "failure_reason": "test",
        "failure_detail": "test",
        "request": None
    } for i in range(3)]
    records[0]["request"] = {"test": "test"}

    current_records = await crud.get_execution_failures(db_session, plugin.id)
    assert len(current_records) == 0

    db_records = await crud.upsert_execution_failures(db_session, records)
    assert len(records) == len(db_records)

    for record, db_record in zip(records, db_records):
        assert record["plugin_id"] == db_record.plugin_id
        assert record["failure_reason"] == db_record.failure_reason
        assert record["failure_detail"] == db_record.failure_detail
        assert record["request"] == db_record.request

    new_records = await crud.get_execution_failures(db_session, plugin.id)
    assert len(new_records) == len(db_records)

    await crud.delete_plugin(db_session, plugin.id)

    post_delete_records = await crud.get_execution_failures(db_session, plugin.id)
    assert len(post_delete_records) == 0

    await db_session.commit()

