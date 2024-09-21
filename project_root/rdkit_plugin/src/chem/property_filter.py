from .mol_props import PROP_FUNCS, FILTER_CATALOGUES, to_mol, smart_to_mol

def check_bounds(value, filter_dict):
    min_val = filter_dict.get('min_val')
    max_val = filter_dict.get('max_val')
    min_val = float('-inf') if min_val is None else min_val
    max_val = float('inf') if max_val is None else max_val
    return min_val <= value <= max_val

def process_property_filters(filters, mol):
    for f in filters:
        name = f.get('property_name', None)
        if name is None:
            return False, f"Property filter dict missing property_name field {f}"

        if name not in PROP_FUNCS:
            return False, f"Invalid property name {name}"
        if not check_bounds(PROP_FUNCS[name](mol), f):
            return False, None
    return True, None

def process_catalog_filters(filters, mol):
    for f in filters:
        name = f.get('catalog_name', None)
        if name is None:
            return False, f"Catalog filter dict missing catalog_name field {f}"
        
        if name not in FILTER_CATALOGUES:
            return False, f"Invalid filter catalog {name}"
        if FILTER_CATALOGUES[name].HasMatch(mol):
            return False, None
    return True, None

def process_smarts_filters(filters, mol):
    for f in filters:
        smarts_str = f.get('smarts', None)
        if smarts_str is None:
            return False, f"Smarts filter dict missing smarts field {f}"
        
        smarts = smart_to_mol(smarts_str)
        if smarts is None:
            return False, f"Invalid smarts string {f['smarts']}"
        if not check_bounds(len(mol.GetSubstructMatches(smarts)), f):
            return False, None
    return True, None

def property_filter(plugin_record, message_data):
    mol = to_mol(message_data['item'])
    if mol is None:
        return {'valid': False}, True, None

    config = plugin_record['config']

    filter_funcs = [process_property_filters, process_catalog_filters, process_smarts_filters]
    config_key = ['property_filters', 'catalog_filters', 'smarts_filters']

    for i in range(len(filter_funcs)):
        valid, reason = filter_funcs[i](config.get(config_key[i], []), mol)
        if not valid:
            return ({}, False, reason) if reason else ({'valid': False}, True, None)

    return {'valid': True}, True, None
