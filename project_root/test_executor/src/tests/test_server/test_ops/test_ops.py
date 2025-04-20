import pytest 

from tests.utils.op_utils import (
    item_op_test_helper,
    data_op_test_helper,
    mapper_op_test_helper,
    assembly_op_test_helper,
    single_data_op_test_helper,
    decomposed_data_op_test_helper,
    mapper_data_op_test_helper
)

@pytest.mark.asyncio
async def test_item_op(db_session, backend_client):
    await item_op_test_helper(db_session, backend_client, "mock_%_api_%")

@pytest.mark.asyncio
async def test_data_op(db_session, backend_client):
    await data_op_test_helper(db_session, backend_client, "mock_%_api_%")

@pytest.mark.asyncio
async def test_mapper_op(db_session, backend_client):
    await mapper_op_test_helper(db_session, backend_client, "mock_%_api_%")

@pytest.mark.asyncio
async def test_assembly_op(db_session, backend_client):
    await assembly_op_test_helper(db_session, backend_client, "mock_%_api_%")

@pytest.mark.asyncio
async def test_single_data_op(db_session, backend_client):
    await single_data_op_test_helper(db_session, backend_client, "mock_%_api_%")

@pytest.mark.asyncio
async def test_decomposed_data_op(db_session, backend_client):
    await decomposed_data_op_test_helper(db_session, backend_client, "mock_%_api_%")

@pytest.mark.asyncio
async def test_mapper_data_op(db_session, backend_client):
    await mapper_data_op_test_helper(db_session, backend_client, "mock_%_api_%")

