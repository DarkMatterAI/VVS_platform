from enum import Enum
from pydantic import BaseModel, field_validator
from typing import List, Dict, Tuple, Optional

from app.schemas.plugin_crud_schemas import (FilterPluginCreate, 
                                             PluginUpdate, 
                                             FilterPluginInDB,
                                             PluginType,
                                             PluginExecutionType)

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
    'type' : PluginType.FILTER,
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
        raise ValueError(f"Must be at least one valid filter, found zero")

    return filter_config


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

    @field_validator('type', 'execution_type', 'group_key')
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
