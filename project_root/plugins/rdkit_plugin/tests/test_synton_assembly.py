from src.chem.synton_assembly import synton_assembly

SYNTON_NAMES = ["O-acylation", 
                "Olefination", 
                "Condensation_of_Y-NH2_with_carbonyl_compounds", 
                "Amine_sulphoacylation", 
                "C-C couplings", 
                "Radical_reactions", 
                "N-acylation", 
                "O-alkylation_arylation", 
                "Metal organics C-C bong assembling", 
                "S-alkylation_arylation", 
                "Alkylation_arylation_of_NH-lactam", 
                "Alkylation_arylation_of_NH-heterocycles", 
                "Amine_alkylation_arylation"
                ]

def get_plugin():
    return {'id' : 1, 'num_parents' : None, 'config' : {}}

def get_config_step(index, reactions):
    return {'step' : index, 'reactions' : reactions}

def test_single_step():
    plugin = get_plugin()
    num_parents = 2
    plugin['num_parents'] = num_parents 
    plugin['config'] = {'synt_on_reaction_stages' : [get_config_step(i, SYNTON_NAMES)]
                        for i in range(num_parents-1)}
    
    inputs = {
        "parents": [
            {"assembly_index" : 0, "id" : 1, "external_id" : "1", "item" : "O=P(NCc1ccc(Br)cc1)(Oc1ccccc1)Oc1ccccc1"},
            {"assembly_index" : 1, "id" : 2, "external_id" : "2", "item" : "CC(C)CCNCCO"},
        ]
    }

    result, valid, reason = synton_assembly(plugin, inputs)
    assert valid 

def test_two_step():
    plugin = get_plugin()
    num_parents = 3
    plugin['num_parents'] = num_parents 
    plugin['config'] = {'synt_on_reaction_stages' : [get_config_step(i, SYNTON_NAMES)]
                        for i in range(num_parents-1)}
    
    inputs = {
        "parents": [
            {"assembly_index" : 0, "id" : 1, "external_id" : "1", "item" : "O=P(NCc1ccc(Br)cc1)(Oc1ccccc1)Oc1ccccc1"},
            {"assembly_index" : 1, "id" : 2, "external_id" : "2", "item" : "CC(C)CCNCCO"},
            {"assembly_index" : 2, "id" : 2, "external_id" : "2", "item" : "Cc1ccc(C(=O)C(Cl)c2ccc(C)cc2)cc1"}
        ]
    }

    result, valid, reason = synton_assembly(plugin, inputs)
    assert valid 


