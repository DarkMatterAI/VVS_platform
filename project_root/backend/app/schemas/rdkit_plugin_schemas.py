from enum import Enum
from pydantic import BaseModel, RootModel, field_validator, model_validator, ValidationError
from typing import List, Dict, Union, Optional

from app.schemas.plugin_crud_schemas import (FilterPluginCreate, 
                                             PluginUpdate, 
                                             FilterPluginInDB,
                                             PluginType,
                                             PluginExecutionType,
                                             PluginType,
                                             AssemblyPluginCreate,
                                             PluginInDBUnion
                                             )

class PropertyName(str, Enum):
    NUM_COMPOUNDS = "Number of Compounds"
    NUM_DUMMY_ATOMS = "Number of Dummy Atoms"
    TPSA = "TPSA"
    LOGP = "LogP"
    MOLECULAR_WEIGHT = "Molecular Weight"
    HEAVY_ATOM_COUNT = "Heavy Atom Count"
    ATOM_COUNT = "Atom Count"
    HETEROATOM_COUNT = "Heteroatom Count"
    SPIRO_ATOM_COUNT = "Spiro Atom Count"
    BRIDGEHEAD_ATOM_COUNT = "Bridgehead Atom Count"
    STEREOCENTER_COUNT = "Stereocenter Count"
    HBD = "Hydrogen Bond Donors"
    HBA = "Hydrogen Bond Acceptors"
    FORMAL_CHARGE = "Formal Charge"
    ROTATABLE_BONDS = "Rotatable Bonds"
    LOOSE_ROTATABLE_BONDS = "Loose Rotatable Bonds"
    ROTATABLE_CHAIN_LENGTH = "Rotatable Chain Length"
    MAX_RING_SIZE = "Max Ring Size"
    MIN_RING_SIZE = "Min Ring Size"
    RING_COUNT = "Ring Count"
    AROMATIC_RING_COUNT = "Ring Count (Aromatic)"
    SATURATED_RING_COUNT = "Ring Count (Saturated)"
    ALIPHATIC_RING_COUNT = "Ring Count (Aliphatic)"
    HETEROCYCLE_COUNT = "Heterocycle Count"
    AROMATIC_HETEROCYCLE_COUNT = "Heterocycle Count (Aromatic)"
    SATURATED_HETEROCYCLE_COUNT = "Heterocycle Count (Saturated)"
    ALIPHATIC_HETEROCYCLE_COUNT = "Heterocycle Count (Aliphatic)"
    AROMATIC_CARBOCYCLES = "Carbocycles (Aromatic)"
    SATURATED_CARBOCYCLES = "Carbocycles (Saturated)"
    ALIPHATIC_CARBOCYCLES = "Carbocycles (Aliphatic)"
    AMIDE_BOND_COUNT = "Amide Bond Count"
    FRACTION_SP3 = "Fraction SP3"
    QED = "QED"
    SA_SCORE = "SA Score"
    MOLAR_REFRACTIVITY = "Molar Refractivity"
    RADICAL_COUNT = "Radical Count"

class CatalogName(str, Enum):
    PAINS = 'PAINS'
    PAINS_A = 'PAINS_A'
    PAINS_B = 'PAINS_B'
    PAINS_C = 'PAINS_C'
    BRENK = 'BRENK'
    NIH = 'NIH'
    ZINC = 'ZINC'


field_mapping = {
    'execution_type' : PluginExecutionType.QUEUE,
    'group_key' : 'rdkit_plugin'
}

def check_bounds(filter):
    return not ((filter.min_val is None and filter.max_val is None) or
                (filter.min_val is not None and filter.max_val is not None and filter.max_val < filter.min_val))

def parse_filters_generic(filters, key_attr, validator, check_bounds_flag=False):
    result = {}
    for filter in filters:
        key = getattr(filter, key_attr)
        if key in result:
            raise ValueError(f"{key_attr.capitalize()} filter {key} appears more than once")
        
        validator(key)

        if check_bounds_flag and not check_bounds(filter):
            continue

        result[key] = filter.model_dump() if hasattr(filter, 'model_dump') else key

    return {f"{key_attr.split('_')[0]}_filters": list(result.values())}

def parse_filters(v):
    filter_config = {
        **parse_filters_generic(v.property_filters, 'property_name', PropertyName, True),
        **parse_filters_generic(v.catalog_filters, 'catalog_name', CatalogName),
        **parse_filters_generic(v.smarts_filters, 'smarts', lambda x: x, True)
    }
    if not any([filter_config.get('property_filters', []), 
                filter_config.get('catalog_filters', []),
                filter_config.get('smarts_filters', [])]):
        raise ValueError(f"Must be at least one valid property filter, found zero")

    return RDKitFilterConfig(**filter_config)
    # return filter_config


class PropertyFilter(BaseModel):
    property_name: PropertyName
    min_val: Optional[float] = None
    max_val: Optional[float] = None

class SmartsFilter(BaseModel):
    smarts: str 
    min_val: Optional[float] = None
    max_val: Optional[float] = None
        
class CatalogFilter(BaseModel):
    catalog_name: str

class RDKitFilterConfig(BaseModel):
    property_filters: List[PropertyFilter]=[]
    catalog_filters: List[CatalogFilter]=[]
    smarts_filters: List[SmartsFilter]=[]


class RDKitFilterCreate(FilterPluginCreate):
    type: PluginType=PluginType.FILTER
    execution_type: PluginExecutionType=PluginExecutionType.QUEUE
    group_key: str='rdkit_plugin'
    config: RDKitFilterConfig

    @field_validator('execution_type', 'group_key')
    def set_default_fields(cls, v, info):
        return field_mapping[info.field_name]
    
    @field_validator('config')
    def parse_config(cls, v, info):
        return parse_filters(v)
 

class RDKitFilterUpdate(PluginUpdate):
    execution_type: PluginExecutionType=PluginExecutionType.QUEUE
    group_key: str='rdkit_plugin'
    config: RDKitFilterConfig

    @field_validator('execution_type', 'group_key')
    def set_default_fields(cls, v, info):
        return field_mapping[info.field_name]

    @field_validator('config')
    def parse_config(cls, v, info):
        return parse_filters(v)


class SmartsConfig(BaseModel):
    smarts: str 
    requires_hs: bool=False 
        
    @field_validator('smarts')
    def parse_smarts(cls, v, info):
        if ('>>' not in v) or (len(v.split('>>')) != 2):
            raise ValueError(f"Expected reaction smarts with the form [reactants]>>[products], found {v}")
            
        reactants, products = v.split('>>')
        num_reactants = len(reactants.split('.'))
        num_products = len(products.split('.'))
        
        if not reactants:
            raise ValueError(f"Reaction smarts missing reactants: {v}")
        elif num_reactants < 2:
            raise ValueError(f"Reaction must have at least two reactants, found {num_reactants}: {v}")
            
        if not products:
            raise ValueError(f"Reaction smarts missing products: {v}")
        elif num_products != 1:
            raise ValueError(f"Reaction must have at least one product, found {num_products}: {v}")
        return v
        
    @property
    def num_reactants(self):
        return len(self.smarts.split('>>')[0].split('.'))
    
    @property
    def num_products(self):
        return len(self.smarts.split('>>')[1].split('.'))


class SmartsReactionStep(BaseModel):
    step: int
    reactions: List[SmartsConfig]
        
class ReactionConfig(BaseModel):
    single_stage_reactions: List[SmartsConfig]
    multi_stage_reactions: List[SmartsReactionStep]
    
    @field_validator('single_stage_reactions')
    def validate_single_stage(cls, v, info):
        num_reactants = set([i.num_reactants for i in v])
        if num_reactants and (len(num_reactants) != 1):        
            raise ValueError(f"Single stage reactions must all have the same number of inputs " \
                             f"- found {num_reactants}")
            
        return v
    
    @field_validator('multi_stage_reactions')
    def validate_multi_stage(cls, v, info):
        step_vals = [i.step for i in v]
        if len(step_vals) != len(set(step_vals)):
            raise ValueError(f"Duplicate reaction steps found: {step_vals}")
            
        for reaction_step in v:
            reactions = reaction_step.reactions
            for reaction in reactions:
                if reaction.num_reactants != 2:
                    raise ValueError(f"Multi step reaction {reaction.smarts} must have exactly two " \
                                     f"reactants, found {reaction.num_reactants}")
            
        return v
    
    @model_validator(mode='after')
    def validate_reactions(cls, values):
        if (len(values.single_stage_reactions)==0 and 
            len(values.multi_stage_reactions)==0):
            raise ValueError(f"No reactions found")
        
        return values
    
def check_parent_reaction_match(values):
    if getattr(values, 'num_parents', None) is None:
        raise ValueError(f"Reaction assembly missing num_parents")
    
    for reaction in values.config.single_stage_reactions:
        if reaction.num_reactants != values.num_parents:
            raise ValueError(f"Number of reactants must match number of parents for single stage reaction" \
                                f" - found {reaction.num_reactants}, {values.num_parents}")
            
    n_stages = len(values.config.multi_stage_reactions)
    if n_stages > 0:
        if n_stages != values.num_parents-1:
            raise ValueError(f"Assembly with {values.num_parents} parents has {n_stages} "\
                            f"reaction stages, expected {values.num_parents-1}")

    return values 

class ReactionAssmeblyCreate(AssemblyPluginCreate):
    type: PluginType=PluginType.ASSEMBLY
    execution_type: PluginExecutionType=PluginExecutionType.QUEUE
    group_key: str='rdkit_plugin'
    config: ReactionConfig

    @field_validator('execution_type', 'group_key')
    def set_default_fields(cls, v, info):
        return field_mapping[info.field_name]

    @model_validator(mode='after')
    def validate_reaction_smarts(cls, values):
        values = check_parent_reaction_match(values)
        return values 

class ReactionAssemblyUpdate(PluginUpdate):
    execution_type: PluginExecutionType=PluginExecutionType.QUEUE
    group_key: str='rdkit_plugin'
    config: ReactionConfig

    @field_validator('execution_type', 'group_key')
    def set_default_fields(cls, v, info):
        return field_mapping[info.field_name]
    
    @model_validator(mode='after')
    def validate_reaction_smarts(cls, values):
        values = check_parent_reaction_match(values)
        return values 
    

class SyntOnReactionNames(str, Enum):
    O_ACYLATION = "O-acylation"
    OLEFINATION = "Olefination"
    CONDENSATION_OF_Y_NH2_WITH_CARBONYL_COMPOUNDS = "Condensation_of_Y-NH2_with_carbonyl_compounds"
    AMINE_SULPHOACYLATION = "Amine_sulphoacylation"
    C_C_COUPLINGS = "C-C couplings"
    RADICAL_REACTIONS = "Radical_reactions"
    N_ACYLATION = "N-acylation"
    O_ALKYLATION_ARYLATION = "O-alkylation_arylation"
    METAL_ORGANICS_C_C_BONG_ASSEMBLING = "Metal organics C-C bong assembling"
    S_ALKYLATION_ARYLATION = "S-alkylation_arylation"
    ALKYLATION_ARYLATION_OF_NH_LACTAM = "Alkylation_arylation_of_NH-lactam"
    ALKYLATION_ARYLATION_OF_NH_HETEROCYCLES = "Alkylation_arylation_of_NH-heterocycles"
    AMINE_ALKYLATION_ARYLATION = "Amine_alkylation_arylation"


class SyntOnReactionStep(BaseModel):
    step: int
    reactions: List[SyntOnReactionNames]

    @field_validator('reactions')
    def parse_reactions(cls, v, info):
        if len(v) != len(set(v)):
            raise ValueError(f"Duplicate reaction names found {v}")
        return v 


class SyntOnReactionConfig(BaseModel):
    synt_on_reaction_stages: List[SyntOnReactionStep]

    @field_validator('synt_on_reaction_stages')
    def validate_multi_stage(cls, v, info):
        if len(v) == 0:
            raise ValueError(f"No reactions found")

        step_vals = [i.step for i in v]
        if len(step_vals) != len(set(step_vals)):
            raise ValueError(f"Duplicate reaction steps found: {step_vals}")
        return v 

class SyntOnAssmeblyCreate(AssemblyPluginCreate):
    type: PluginType=PluginType.ASSEMBLY
    execution_type: PluginExecutionType=PluginExecutionType.QUEUE
    group_key: str='rdkit_plugin'
    config: SyntOnReactionConfig

    @field_validator('execution_type', 'group_key')
    def set_default_fields(cls, v, info):
        return field_mapping[info.field_name]

    @model_validator(mode='after')
    def validate_reaction_smarts(cls, values):
        if getattr(values, 'num_parents', None) is None:
            raise ValueError(f"Reaction assembly missing num_parents")
        
        n_stages = len(values.config.synt_on_reaction_stages)
        if n_stages != values.num_parents-1:
            raise ValueError(f"Assembly with {values.num_parents} parents has {n_stages} "\
                            f"reaction stages, expected {values.num_parents-1}")
        return values 

class SyntOnAssemblyUpdate(PluginUpdate):
    execution_type: PluginExecutionType=PluginExecutionType.QUEUE
    group_key: str='rdkit_plugin'
    config: SyntOnReactionConfig

    @field_validator('execution_type', 'group_key')
    def set_default_fields(cls, v, info):
        return field_mapping[info.field_name]
    
    @model_validator(mode='after')
    def validate_reaction_smarts(cls, values):
        if getattr(values, 'num_parents', None) is None:
            raise ValueError(f"Reaction assembly missing num_parents")
        
        n_stages = len(values.config.synt_on_reaction_stages)
        if n_stages != values.num_parents-1:
            raise ValueError(f"Assembly with {values.num_parents} parents has {n_stages} "\
                            f"reaction stages, expected {values.num_parents-1}")
        return values 
    
RDKitPluginCreateUnion = Union[
    RDKitFilterCreate,
    ReactionAssmeblyCreate,
    SyntOnAssmeblyCreate
]

class RDKitPluginCreate(RootModel):
    root: RDKitPluginCreateUnion

RDKitPluginUpdateUnion = Union[
    RDKitFilterUpdate,
    ReactionAssemblyUpdate,
    SyntOnAssemblyUpdate
]

class RDKitPluginUpdate(RootModel):
    root: RDKitPluginUpdateUnion
