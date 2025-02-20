from rdkit import Chem 
from rdkit.Chem import AllChem

def to_mol(smile):
    try:
        mol = Chem.MolFromSmiles(smile)
    except:
        mol = None
    return mol

def to_smile(mol):
    try:
        smile = Chem.MolToSmiles(mol)
    except:
        smile = None 
    return smile 

def smart_to_mol(smarts):
    try:
        mol = Chem.MolFromSmarts(smarts)
    except:
        mol = None
    return mol

def smarts_to_rxn(rxn_smarts):
    try:
        rxn = AllChem.ReactionFromSmarts(rxn_smarts)
    except:
        rxn = None 
    return rxn 

def to_inchi_key(smile):
    mol = to_mol(smile)
    if mol is not None:
        key = Chem.MolToInchiKey(mol)
    else:
        key = None
    return key 

def clean_reaction_product(product, requires_hs):
    try:
        # note there can be canonicalization issues if 
        # sanitization happens before removing Hs
        if requires_hs:
            product = Chem.RemoveHs(product)
        Chem.SanitizeMol(product)
            
        product_smile = to_smile(product)
        return product_smile
    except:
        return None
    
def get_unique_products(products, requires_hs):
    output = []
    for product_list in products:
        for product in product_list:
            product = clean_reaction_product(product, requires_hs)
            if product is not None:
                output.append(product)
    output = list(set(output))
    return output 

def parse_assembly_inputs(message_data):
    parents = message_data['parents']
    parents = sorted(parents, key=lambda x: x['assembly_index'])
    smiles = [i['item'] for i in parents]
    mols = [to_mol(i) for i in smiles]
    return mols, smiles, parents

def check_num_parents(parents, plugin_record):
    if len(parents) != plugin_record['num_parents']:
        return False, f"Plugin {plugin_record['id']} expected {plugin_record['num_parents']} parents, got {len(parents)}"
    assembly_idxs = [i['assembly_index'] for i in parents]
    if len(assembly_idxs) != len(set(assembly_idxs)):
        return False, f"Found duplicate assembly indices {assembly_idxs}"
    
    return True, ''

