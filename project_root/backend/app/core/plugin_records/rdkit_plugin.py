from app.crud import plugin_crud as crud 
from app import schemas

RDKIT_FILTERS = [
    {
        "name": "Rule of 5 Filter",
        "type": "filter",
        "plugin_class": "internal_rdkit",
        "execution_type": "queue",
        "group_key": "rdkit_plugin",
        "timeout": 600,
        "max_concurrency": 64,
        "max_retries": 2,
        "config" : {
            "property_filters" : [
                {"property_name" : "LogP", "Min_val" : None, "max_val" : 5.0},
                {"property_name" : "Molecular Weight", "Min_val" : None, "max_val" : 500.0},
                {"property_name" : "Hydrogen Bond Donors", "Min_val" : None, "max_val" : 5.0},
                {"property_name" : "Hydrogen Bond Acceptors", "Min_val" : None, "max_val" : 5.0},
            ]
        }
    }
]


async def init_rdkit_records(db):
    for record in RDKIT_FILTERS:
        current_record = await crud.get_plugins(db, filter_params={'name' : record['name']})
        if not current_record:
            print(f"Creating RDKit record {record['name']}")
            record = schemas.FilterPluginCreate(**record)
            response = await crud.create_plugin(db=db, plugin=record)
            print(response)