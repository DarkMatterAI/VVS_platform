
from .rdkit_utils import parse_assembly_inputs, check_num_parents

from .synton_utils import SYNTON_REACTIONS, SyntOnReactionNames

class SyntOnReactionConfig():
    def __init__(self, config):
        self.config = config['synt_on_reaction_stages']
        self.validate_config()

    def validate_config(self):
        for step in self.config:
            reactions = step['reactions']
            for reaction_name in reactions:
                try:
                    _ = SyntOnReactionNames(reaction_name)
                except:
                    self.valid = False 
                    self.invalid_name = reaction_name
                    return 
        self.valid = True 

    def react(self, parent_smiles):

        previous_reactants = [parent_smiles[0]]
        for i, step in enumerate(self.config):
            reaction_names = step['reactions']

            current_reactant = parent_smiles[i+1]
            next_prev = []

            for previous_reactant in previous_reactants:
                synthon_p, terminal_p = SYNTON_REACTIONS.react_pair(previous_reactant, 
                                                                    current_reactant,
                                                                    reaction_names
                                                                    )
                next_prev += synthon_p 

            previous_reactants = list(set(next_prev))

        return terminal_p

def synton_assembly(plugin_record, message_data):
    parent_mols, parent_smiles, parents = parse_assembly_inputs(message_data)
    if any([i is None for i in parent_mols]):
        return {'valid': False}, True, None
    
    parent_check, reason = check_num_parents(parents, plugin_record)
    if not parent_check:
        return {}, False, reason 
    
    reactions = SyntOnReactionConfig(plugin_record['config'])
    if not reactions.valid:
        return {}, False, f"Invalid reaction name {reactions.invalid_name}"
    
    products = reactions.react(parent_smiles)
    
    result = {'valid' : bool(products), 
              'result' : [{'item' : i, 'external_id' : None} for i in products]}
    
    return result, True, None
