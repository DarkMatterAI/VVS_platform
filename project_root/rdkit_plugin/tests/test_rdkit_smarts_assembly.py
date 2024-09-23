from src.chem.reaction_assembly import reaction_assembly

def get_plugin():
    return {'id' : 1, 'num_parents' : None, 'config' : {}}

def format_inputs(test_config):
    inputs = {'request_id' : '', 'parents' : [{'assembly_index' : i, 'item' : test_config['inputs'][i]}
                                               for i in range(len(test_config['inputs']))]}
    return inputs 

def format_smarts(test_config):
    ss = [{'smarts' : i[0], 'requires_hs' : i[1]} for i in test_config['single_smarts']]
    ms = []
    for step, smarts in enumerate(test_config['multi_smarts']):
        step_smarts = [{'smarts' : i[0], 'requires_hs' : i[1]} for i in smarts]
        ms.append({'step' : step, 'reactions' : step_smarts})
        
    return ss, ms

def run_assembly_test(test_config):
    plugin = get_plugin()
    
    if test_config['num_parents'] is not None:
        plugin['num_parents'] = test_config['num_parents']
    else:
        plugin['num_parents'] = len(test_config['inputs'])
        
    ss, ms = format_smarts(test_config)
        
    plugin['config']['single_stage_reactions'] = ss
    plugin['config']['multi_stage_reactions'] = ms
    
    inputs = format_inputs(test_config)
    output, valid, reason = reaction_assembly(plugin, inputs)
    
    output_smiles = [i['item'] for i in output.get('result', [])]
    return output_smiles, valid, reason
    
def test_ss_single():
    test_config = {
        'single_smarts' : [('[C:1].[N:2].[O:3]>>[C:1][N:2][O:3]', False)],
        'multi_smarts' : [],
        'num_parents' : None,
        'inputs' : ['C', 'N', 'O'],
        'outputs' : set(['CNO'])
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert valid 
    assert set(output_smiles) == test_config['outputs']

def test_ss_multiple():
    test_config = {
        'single_smarts' : [('[C:1].[N:2].[O:3]>>[C:1][N:2][O:3]', False),
                           ('[C:1].[N:2].[O:3]>>[N:2][C:1][O:3]', False)],
        'multi_smarts' : [],
        'num_parents' : None,
        'inputs' : ['C', 'N', 'O'],
        'outputs' : set(['CNO', 'NCO'])
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert valid 
    assert set(output_smiles) == test_config['outputs']

def test_ms_one_step():
    test_config = {
        'single_smarts' : [],
        'multi_smarts' : [
            [('[C:1].[N:2]>>[C:1]-[N:2]', False)],
        ],
        'num_parents' : None,
        'inputs' : ['C', 'N'],
        'outputs' : set(['CN'])
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert valid 
    assert set(output_smiles) == test_config['outputs']

def test_ms_two_step():
    test_config = {
        'single_smarts' : [],
        'multi_smarts' : [
            [('[C:1].[N:2]>>[C:1]-[N:2]', False)],
            [('[C:1]N.[O:2]>>[C:1]N[O:2]', False)],
        ],
        'num_parents' : None,
        'inputs' : ['C', 'N', 'O'],
        'outputs' : set(['CNO'])
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert valid 
    assert set(output_smiles) == test_config['outputs']

def test_ms_three_step():
    test_config = {
        'single_smarts' : [],
        'multi_smarts' : [
            [('[C:1].[N:2]>>[C:1]-[N:2]', False)],
            [('[C:1]N.[O:2]>>[C:1]N[O:2]', False)],
            [('[C:1].[N:2]>>[C:1]-[N:2]', False)],
        ],
        'num_parents' : None,
        'inputs' : ['C', 'N', 'O', 'N'],
        'outputs' : set(['NCNO'])
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert valid 
    assert set(output_smiles) == test_config['outputs']

def test_single_parent_mismatch():
    test_config = {
        'single_smarts' : [('[C:1].[N:2].[O:3]>>[C:1][N:2][O:3]', False)],
        'multi_smarts' : [],
        'num_parents' : 2,
        'inputs' : ['C', 'N', 'O'],
        'outputs' : []
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert not valid 
    assert not output_smiles
    assert reason == 'Plugin 1 expected 2 parents, got 3'

def test_multi_parent_mismatch():
    test_config = {
        'single_smarts' : [],
        'multi_smarts' : [
            [('[C:1].[N:2]>>[C:1]-[N:2]', False)],
            [('[C:1]N.[O:2]>>[C:1]N[O:2]', False)],
            [('[C:1].[N:2]>>[C:1]-[N:2]', False)],
        ],
        'num_parents' : 4,
        'inputs' : ['C', 'N', 'O'],
        'outputs' : set(['NCNO'])
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert not valid 
    assert not output_smiles
    assert reason == 'Plugin 1 expected 4 parents, got 3'

def test_singe_invalid_smarts():
    test_config = {
        'single_smarts' : [('[C>>', False)],
        'multi_smarts' : [],
        'num_parents' : 2,
        'inputs' : ['C', 'N'],
        'outputs' : []
    }
    output_smiles, valid, reason = run_assembly_test(test_config)
    assert not valid 
    assert not output_smiles
    assert reason == 'All reactions failed to parse'

