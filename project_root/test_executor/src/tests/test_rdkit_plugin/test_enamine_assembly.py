import pytest 

from tests.utils.backend_utils import backend_get_plugins_by_filter
from tests.utils.request_data import generate_rdkit_assembly_request, validate_response, validate_api_response
from tests.utils.rabbitmq_utils import rabbitmq_publish, collect_replies # poll_redis
from tests.utils.backend_utils import backend_execute_plugin
from tests.utils.db_utils import validate_assembly_checkin

reaction_data = [
    {'id': 22, 'r1': 'CS(=O)(=O)c1ccc(CCN)cc1', 'r2': 'O=C(O)c1cc(C(F)(F)F)n[nH]1'},
    {'id': 11, 'r1': 'NNC(=O)c1ccccc1', 'r2': 'O=C(O)C(F)c1ccc(F)cc1'},
    {'id': 527, 'r1': 'Cc1cc(CCN)c2cn[nH]c2c1', 'r2': 'Cc1cccc(Nc2ccccc2C(=O)O)c1C'},
    {'id': 240690, 'r1': 'CON(C)C1CCNCC1', 'r2': 'O=C(O)c1cccc(CC(F)(F)F)c1'},
    {'id': 2430, 'r1': 'CCc1cccc(N)c1CC', 'r2': 'Cc1cc(F)c(C)cc1N'},
    {'id': 2708, 'r1': 'CC(N)Cc1ccc(Cl)s1', 'r2': 'CC1CC(N)CS1'},
    {'id': 2230, 'r1': 'CNCc1ccc2[nH]nnc2c1', 'r2': 'CC(C)Oc1cnc(Cl)nc1'},
    {'id': 2718, 'r1': 'Cn1nc2c(c1N)CCC2', 'r2': 'NCCOCC(F)F'},
    {'id': 40, 'r1': 'NCC12CC(CN1Cc1ccccc1)C2', 'r2': 'O=S(=O)(Cl)CCCC1CCOCC1'},
    {'id': 27, 'r1': 'Nc1nc2nc(Cl)ccc2s1', 'r2': 'CC(C)(C)OC(=O)CCCCCCCCCBr'},
    {'id': 271948, 'r1': 'NCCCC1(C(F)(F)F)N=N1', 'r2': 'NCc1cncc(Cl)c1'},
    {'id': 1458, 'r1': 'Cc1cc(C(=O)O)cc(O)n1', 'r2': 'NC(=O)c1cnc(Cl)cn1'},
    {'id': 'All', 'r1': 'Cc1cc(C(=O)O)cc(O)n1', 'r2': 'NC(=O)c1cnc(Cl)cn1'}
]
reaction_dict = {i['id']: i for i in reaction_data}

def get_reaction_plugin(backend_client, reaction_id):
    """Get the plugin for a specific reaction ID"""
    filter_record = backend_get_plugins_by_filter(
        backend_client, 
        plugin_type='assembly',
        plugin_class='internal_rdkit',
        group_key='rdkit_plugin',
        name_pattern=f"Enamine Reaction {reaction_id}"
    )[0]
    return filter_record


def test_enamine_assembly_created(backend_client):
    """Test that the expected number of reaction plugins exist"""
    plugins = backend_get_plugins_by_filter(
        backend_client, 
        plugin_type='assembly',
        plugin_class='internal_rdkit',
        group_key='rdkit_plugin',
        name_pattern=f"%Enamine Reaction%"
    )
    assert len(plugins) == 13, "Incorrect number of Enamine assembly plugins"


@pytest.mark.asyncio
@pytest.mark.parametrize("reaction_id", [r['id'] for r in reaction_data])
async def test_rdkit_enamine_assembly_consumer(db_session, rabbitmq_connection, redis_connection, 
                                               backend_client, reaction_id):
    plugin = get_reaction_plugin(backend_client, reaction_id)
    reactants = reaction_dict[reaction_id]
    parents = [[reactants['r1'], reactants['r2']]]
    request_data = await generate_rdkit_assembly_request(db_session, parents, plugin, to_model=True)

    conn, ch = rabbitmq_connection
    result       = ch.queue_declare(queue="", exclusive=True)
    reply_queue  = result.method.queue

    corr_ids  = rabbitmq_publish(ch, request_data, reply_queue)
    responses = collect_replies(conn, ch, reply_queue, corr_ids,
                                interval=0.05, timeout=10.0)
    validate_response(plugin, responses)
    await db_session.commit()

@pytest.mark.asyncio
@pytest.mark.parametrize("reaction_id", [r['id'] for r in reaction_data])
async def test_rdkit_assembly_backend(db_session, backend_client, reaction_id):
    plugin = get_reaction_plugin(backend_client, reaction_id)
    reactants = reaction_dict[reaction_id]
    parents = [[reactants['r1'], reactants['r2']]]
    request_data = await generate_rdkit_assembly_request(db_session, parents, plugin)
    response = backend_execute_plugin(backend_client, request_data, plugin['id'])
    validate_api_response(plugin, response, 200)
    await validate_assembly_checkin(db_session, request_data, response.json(), plugin)
    await db_session.commit()

