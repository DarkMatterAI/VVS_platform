from rdkit import Chem 

from .rdkit_utils import (to_mol, 
                          smarts_to_rxn, 
                          to_smile, 
                          to_inchi_key,
                          get_unique_products,
                          parse_assembly_inputs, 
                          check_num_parents
                          )

from ..utils import date_print

class Reaction():
    def __init__(self, smarts_config):
        self.config = smarts_config
        self.smarts = smarts_config.get('smarts', '')
        self.requires_hs = smarts_config.get('requires_hs', False)
        self.reaction = smarts_to_rxn(self.smarts)
        self.num_reactants = len(self.smarts.split('>>')[0].split('.'))
        self.validate()
        
    def validate(self):
        self.valid = False
        if self.reaction is not None:
            try:
                self.reaction.Initialize()
                self.valid = True
            except:
                pass
            
    def react(self, reactants):
        if (not self.valid) or (len(reactants) != self.num_reactants):
            return []
        
        if self.requires_hs:
            reactants = [Chem.AddHs(i) for i in reactants]
        
        products = self.reaction.RunReactants(reactants)
        products = get_unique_products(products, self.requires_hs)
        return products

class SingleStageReaction():
    def __init__(self, reaction_list):
        reactions = [Reaction(i) for i in reaction_list]
        self.reactions = [i for i in reactions if i.valid]
        self.valid = len(self.reactions) > 0
        
    def __len__(self):
        return len(self.reactions)
        
    def react(self, reactants):
        if not self.valid:
            return []
        
        products = []
        for reaction in self.reactions:
            products += reaction.react(reactants)
        products = list(set(products))
        return products

class MultiStageReaction():
    def __init__(self, reaction_config):
        self.reaction_config = sorted([{'step' : i['step'], 'reactions' : SingleStageReaction(i['reactions'])}
                               for i in reaction_config], key=lambda x: x['step'])
        self.validate()
        self.num_steps = len(self.reaction_config)
        
    def validate(self):
        self.valid = bool(self.reaction_config)
        for step in self.reaction_config:
            reactions = step['reactions']
            if not reactions.valid:
                self.valid = False
                return
            
    def react(self, reactants):
        if (not self.valid) or (len(reactants)-1 != self.num_steps):
            return [] 
        
        reaction_pointer = 0
        current_reactants = [reactants[0]]
        
        while reaction_pointer < len(self.reaction_config):
            current_reaction = self.reaction_config[reaction_pointer]['reactions']
            
            next_reactant = reactants[reaction_pointer+1]
            next_inputs = []
            
            date_print(f"Running reaction {reaction_pointer} of {self.num_steps}" \
                       f" with {len(current_reactants)} previous inputs")
            
            for current_reactant in current_reactants:
                next_inputs += current_reaction.react([current_reactant, next_reactant])
                    
            next_inputs = [to_mol(i) for i in set(next_inputs)]
            next_inputs = [i for i in next_inputs if i is not None]
            
            date_print(f"Reaction {reaction_pointer} of {self.num_steps}" \
                       f" with {len(current_reactants)} inputs generate " \
                       f"{len(next_inputs)} products")
            
            current_reactants = next_inputs
            reaction_pointer += 1
            
        output = current_reactants
        output = [to_smile(i) for i in output]
        output = [i for i in output if i is not None]

        date_print(f"Finished multi-step reaction with {len(output)} results")
        return output

class ReactionConfig():
    def __init__(self, config):
        self.single_stage = SingleStageReaction(config['single_stage_reactions'])
        self.multi_stage = MultiStageReaction(config['multi_stage_reactions'])
        self.valid = self.single_stage.valid or self.multi_stage.valid
        
    def react(self, reactants):
        products = []
        if self.single_stage.valid:
            date_print(f"Running single stage reactions")
            products += self.single_stage.react(reactants)
        
        if self.multi_stage.valid:
            date_print(f"Running multu stage reactions")
            products += self.multi_stage.react(reactants)
            
        products = list(set(products))
        return products 

def reaction_assembly(plugin_record, message_data):
    parent_mols, _, parents = parse_assembly_inputs(message_data)
    if any([i is None for i in parent_mols]):
        return {'valid': False}, True, None
    
    parent_check, reason = check_num_parents(parents, plugin_record)
    if not parent_check:
        return {}, False, reason 
    
    reactions = ReactionConfig(plugin_record['config'])
    if not reactions.valid:
        return {}, False, 'All reactions failed to parse' 
    
    products1 = reactions.react(parent_mols)
    products2 = reactions.react(parent_mols[::-1])
    products = list(set(products1+products2))
    inchi_keys = [to_inchi_key(i) for i in products]
    
    result = {'valid' : bool(products), 
              'result' : [{'item' : i, 'external_id' : j} for 
                          (i,j) in zip(products, inchi_keys)]}
    
    return result, True, None

