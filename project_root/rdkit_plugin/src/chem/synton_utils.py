from synt_on.src.SyntOn_BBs import mainSynthonsGenerator
from synt_on.src.SyntOn_Classifier import BBClassifier

import xml.etree.ElementTree as ET
import re
from enum import Enum 

from rdkit import Chem 
from rdkit import RDLogger                                                                                                                                                               
RDLogger.DisableLog('rdApp.*') 

from .rdkit_utils import to_mol, to_smile, smarts_to_rxn, get_unique_products

from ..utils import deduplicate_list, date_print

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

def smile_to_synthon(smile, keep_pg=False):

    classes = BBClassifier(mol=to_mol(smile))
    
    azoles,fSynt = mainSynthonsGenerator(smile, keep_pg, classes, returnBoolAndDict=True)

    smiles = list(fSynt.keys())
    rxns = list(fSynt.values())
    rxns = [list(i) for i in rxns]
    return smiles, rxns

def get_synthon_marks(smile):
    '''
    extracts reaction tag marks from synthon
    
    ie `'CC1(C)CC(N[CH:10]=O)CC(C)(CNC(=O)NCc2cc([CH:10]=O)ccn2)C1' -> ['C:10']`
    '''
    pat = re.compile("\[\w*:\w*\]")
    current_marks = [smile[m.start() + 1] + ":" + smile[m.end() - 3:m.end() - 1]
                    for m in re.finditer(pat, smile)]
    return deduplicate_list(current_marks)

SYNTHON_VALID_COMBINATIONS = {'C:10': ['N:20', 'O:20', 'C:20', 'c:20', 'n:20', 'S:20'],
                              'c:10': ['N:20', 'O:20', 'C:20', 'c:20', 'n:20', 'S:20'],
                              'c:20': ['N:11', 'C:10', 'c:10'], 
                              'C:20': ['C:10', 'c:10'],
                              'c:21': ['N:20', 'O:20', 'n:20'], 
                              'C:21': ['N:20', 'n:20'],
                              'N:20': ['C:10', 'c:10', 'C:21', 'c:21', 'S:10'], 
                              'N:11': ['c:20'],
                              'n:20': ['C:10', 'c:10', 'C:21', 'c:21'], 
                              'O:20': ['C:10', 'c:10', 'c:21'],
                              'S:20': ['C:10', 'c:10'], 
                              'S:10': ['N:20'], 
                              'C:30': ['C:40', 'N:40'],
                              'C:40': ['C:30'], 
                              'C:50': ['C:50'], 
                              'C:70': ['C:60', 'c:60'],
                              'c:60':['C:70'], 
                              'C:60': ['C:70'], 
                              'N:40': ['C:30'] }

def add_reconstruction_atoms(smile): # synthon reconstruction string
    
    'augments synthon annotations (ie c:10) with dummy atoms for fusion'
    labels = [10, 20, 30, 40, 50, 60, 70, 21, 11] # annotation numbers
    atomsForMarking = [23, 74, 72, 104, 105, 106, 107, 108, 109] # dummy atoms
    atomsForMarkingForDoubleBonds = [72, 104, 105]
    
    mol = to_mol(smile)
    mol = Chem.AddHs(mol)
    
    for atom in mol.GetAtoms():
        if atom.GetAtomMapNum() != 0:
            repl = atomsForMarking[labels.index(atom.GetAtomMapNum())]
            replCount = 0
            for neighbor in atom.GetNeighbors():
                if neighbor.GetAtomicNum() == 1:
                    mol.GetAtomWithIdx(neighbor.GetIdx()).SetAtomicNum(repl)
                    replCount += 1
                    if repl not in atomsForMarkingForDoubleBonds and replCount == 1:
                        break
                    elif replCount == 2:
                        break
                        
    mol = Chem.RemoveHs(mol)
    return to_smile(mol)

def remove_reconstruction_atoms(smile):
    'removes dummy atoms for fusion'
    atomsForMarking = set([23, 74, 72, 104, 105, 106, 107, 108, 109])
    mol = to_mol(smile)
    
    for atom in mol.GetAtoms():
        if atom.GetAtomicNum() in atomsForMarking:
            atom.SetAtomicNum(1)
            
    mol = Chem.AddHs(mol)
    mol = Chem.RemoveHs(mol)
            
    return to_smile(mol)


def parse_reaction(reaction):
    return {
        "name": reaction.get("name"),
        "SMARTS": reaction.get("SMARTS"),
        "Labels": reaction.get("Labels"),
        "ReconstructionReaction": reaction.get("ReconstructionReaction")
    }

def parse_reaction_group(group):
    return {
        "name": group.get("name"),
        "reactions": [parse_reaction(r) for r in group]
    }

def parse_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    available_reactions = root.find("AvailableReactions")
    
    result = {
        "AvailableReactions": [
            parse_reaction_group(group)
            for group in available_reactions
            if group.tag.startswith("R")
        ],
        "AvailableModesOfFragmentation": [
            mode.text.strip()
            for mode in root.find("AvailableModesOfFragmentation")
            if mode.text and mode.text.strip()
        ]
    }
    
    return result

def get_synthon_data(smile):
    synthon_data_list = []
    
    if ':' in smile:
        synthons = [smile]
    else:
        synthons = smile_to_synthon(smile)[0]
    
    synthons = [Synthon(smile, i) for i in synthons]
    return synthons

def get_synthon_pairs(synthon_list1, synthon_list2):
    output = []
    for s1 in synthon_list1:
        for s2 in synthon_list2:
            if s1.is_compatible(s2):
                output.append((s1, s2))
    
    output = deduplicate_list(output, key_func = lambda x: x[0].synthon_smile+x[1].synthon_smile)
    return output

class Synthon():
    def __init__(self, smile, synthon):
        self.smile = smile
        self.synthon_smile = synthon
        self.recon_smile = add_reconstruction_atoms(synthon)
        self.recon_mol = to_mol(self.recon_smile)
        self.marks = set(get_synthon_marks(synthon))
        self.compatible_marks = set([item for sublist in 
                                      [SYNTHON_VALID_COMBINATIONS.get(i, []) 
                                       for i in self.marks] for item in sublist])
        
    def is_compatible(self, synthon):
        overlaps = self.compatible_marks.intersection(synthon.marks)
        return bool(overlaps)
    
    def __repr__(self):
        return self.synthon_smile
    
class SynthonReaction():
    def __init__(self, reaction_config):
        self.reaction_config = reaction_config
        self.reaction = smarts_to_rxn(reaction_config['ReconstructionReaction'])
        # self.reaction = AllChem.ReactionFromSmarts(reaction_config['ReconstructionReaction'])
        self.reaction.Initialize()
        self.reactant_mols = list(self.reaction.GetReactants())
        
    def react(self, reactant1, reactant2):
        products = self.reaction.RunReactants((reactant1, reactant2))
        products = get_unique_products(products, False)
        return products
    
class SynthonReactionClass():
    def __init__(self, class_config):
        self.name = class_config['name']
        self.reactions = [SynthonReaction(i) for i in class_config['reactions']]
        
    def react(self, synthon1, synthon2):        
        reactants = [synthon1.recon_mol, synthon2.recon_mol]
        
        output = []
        for reaction in self.reactions:
            products = []
            for reactant_order in [reactants, reactants[::-1]]:
                products += reaction.react(*reactant_order)
            
            output += products
            
        if len(output)>0:
            date_print(f"Reaction Class {self.name} produced {len(output)} products")
                
        return output
    
class SynthonReactionUniverse():
    def __init__(self, xml_filename):
        self.setup(xml_filename)
        
    def setup(self, xml_filename):
        reactions = parse_xml(xml_filename)['AvailableReactions'][:-1]
        self.reactions = [SynthonReactionClass(i) for i in reactions]
        self.reaction_dict = {i.name:i for i in self.reactions}
        
    def react_pair(self, reactant1, reactant2, reaction_names=None):
        if reaction_names is None:
            reaction_names = [i.name for i in self.reactions]
        
        date_print(f"Starting synt-on reaction with {len(reaction_names)} reaction classes")
        synthons1 = get_synthon_data(reactant1)
        synthons2 = get_synthon_data(reactant2)
        
        synthon_pairs = get_synthon_pairs(synthons1, synthons2)
        
        terminal_products = set()
        synthon_products = set()
        
        for reaction_name in reaction_names:
            reaction_class = self.reaction_dict[reaction_name]
            for pair in synthon_pairs:
                products = reaction_class.react(*pair)
                for product in products:
                    if ':' in product:
                        synthon_products.update([product])
                    else:
                        terminal_products.update([product])
        
        date_print(f"Synt-on reaction with {len(reaction_names)} reaction classes produced " \
                   f"{len(synthon_products)} synthon products and {len(terminal_products)} " \
                   "terminal products")
        
        return list(synthon_products), list(terminal_products)
    
SYNTON_REACTIONS = SynthonReactionUniverse('synt_on/config/Setup.xml')
