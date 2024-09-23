from src.chem.property_filter import property_filter
from src.chem.mol_props import FILTER_CATALOGUES, PROP_FUNCS

def get_inputs():
    plugin_record = {
        'config' : {
            'property_filters' : [],
            'catalog_filters' : [],
            'smarts_filters' : [],
        }
    }
    message_data = {
        'item' : ''
    }
    return plugin_record, message_data

def test_property_filter():
    plugin_record, message_data = get_inputs()
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'

    plugin_record['config']['property_filters'] = [{'property_name' : 'Ring Count', 'min_val' : 1, 'max_val' : None}]
    result = property_filter(plugin_record, message_data)
    assert result ==  ({'valid': True}, True, None)

    plugin_record['config']['property_filters'] = [{'property_name' : 'Ring Count', 'min_val' : None, 'max_val' : 1}]
    result = property_filter(plugin_record, message_data)
    assert result[0] ==  {'valid': False}

    plugin_record['config']['property_filters'] = [{'property_name' : 'Ring Count', 'min_val' : 4, 'max_val' : None}]
    result = property_filter(plugin_record, message_data)
    assert result[0] ==  {'valid': False}

def test_property_filters():
    plugin_record, message_data = get_inputs()
    plugin_record['config']['property_filters'] = [{'property_name' : i, 'min_val' : 1, 'max_val' : 10}
                                         for i in PROP_FUNCS.keys()]
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'
    result = property_filter(plugin_record, message_data)

def test_invalid_mol():
    plugin_record, message_data = get_inputs()
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](Ocn2)n(C)n1'
    plugin_record['config']['property_filters'] = [{'property_name' : 'Ring Count', 'min_val' : 1, 'max_val' : None}]
    result = property_filter(plugin_record, message_data)
    assert result == ({'valid': False}, True, None)

def test_invalid_prop():
    plugin_record, message_data = get_inputs()
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'
    plugin_record['config']['property_filters'] = [{'property_name' : 'RingCount', 'min_val' : 1, 'max_val' : None}]
    result = property_filter(plugin_record, message_data)
    assert result == ({}, False, f"Invalid property name RingCount")

def test_catalog_filter():
    plugin_record, message_data = get_inputs()
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'
    plugin_record['config']['catalog_filters'] = [{'catalog_name' : 'ZINC'}]
    result = property_filter(plugin_record, message_data)
    assert result[0] ==  {'valid': True}

def test_catalog_filters():
    plugin_record, message_data = get_inputs()
    plugin_record['config']['property_filters'] = [{'catalog_name' : i} for i in FILTER_CATALOGUES.keys()]
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'
    result = property_filter(plugin_record, message_data)

def test_invalid_catalog():
    plugin_record, message_data = get_inputs()
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'
    plugin_record['config']['catalog_filters'] = [{'catalog_name' : 'ZC'}]
    result = property_filter(plugin_record, message_data)
    assert result == ({}, False, f"Invalid filter catalog ZC")

def test_smarts_filter():
    plugin_record, message_data = get_inputs()
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'
    plugin_record['smarts_filters'] = [{'smarts' : '[#6]', 'min_val' : 1}]
    result = property_filter(plugin_record, message_data)
    assert result[0] ==  {'valid': True}

    plugin_record['config']['smarts_filters'] = [{'smarts' : '[#6]', 'min_val' : 1000}]
    result = property_filter(plugin_record, message_data)
    assert result[0] ==  {'valid': False}

def test_invalid_smarts():
    plugin_record, message_data = get_inputs()
    message_data['item'] = 'CC(C)c1cc(C(=O)NC[C@@H](O)c2ccccn2)n(C)n1'
    plugin_record['config']['smarts_filters'] = [{'smarts' : 'asfbsd', 'min_val' : 1000}]
    result = property_filter(plugin_record, message_data)
    assert result == ({}, False, f"Invalid smarts string asfbsd")

