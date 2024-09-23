from rdkit import Chem
from rdkit.Chem import rdMolDescriptors, Descriptors, QED
from rdkit.Chem import AllChem
from rdkit.Contrib.SA_Score import sascorer
from rdkit.Chem.FilterCatalog import FilterCatalog, FilterCatalogParams
from rdkit.Chem.Lipinski import RotatableBondSmarts

def find_bond_groups(mol):
    """
    Find groups of contiguous rotatable bonds and return them sorted by decreasing size

    https://www.rdkit.org/docs/Cookbook.html
    """
    rot_atom_pairs = mol.GetSubstructMatches(RotatableBondSmarts)
    rot_bond_set = set([mol.GetBondBetweenAtoms(*ap).GetIdx() for ap in rot_atom_pairs])
    rot_bond_groups = []
    while (rot_bond_set):
        i = rot_bond_set.pop()
        connected_bond_set = set([i])
        stack = [i]
        while (stack):
            i = stack.pop()
            b = mol.GetBondWithIdx(i)
            bonds = []
            for a in (b.GetBeginAtom(), b.GetEndAtom()):
                bonds.extend([b.GetIdx() for b in a.GetBonds() if (
                    (b.GetIdx() in rot_bond_set) and (not (b.GetIdx() in connected_bond_set)))])
            connected_bond_set.update(bonds)
            stack.extend(bonds)
        rot_bond_set.difference_update(connected_bond_set)
        rot_bond_groups.append(tuple(connected_bond_set))
    return tuple(sorted(rot_bond_groups, reverse = True, key = lambda x: len(x)))

def max_ring_size(mol):
    'size of largest ring'
    ring_info = mol.GetRingInfo()
    return max((len(r) for r in ring_info.AtomRings()), default=0)

def min_ring_size(mol):
    'size of smallest ring'
    ring_info = mol.GetRingInfo()
    return min((len(r) for r in ring_info.AtomRings()), default=0)

def loose_rotbond(mol):
    'number of rotatable bonds, includes things like amides and esters'
    return rdMolDescriptors.CalcNumRotatableBonds(mol, False)

def rot_chain_length(mol):
    'Length of longest contiguous rotatable bond chain'
    output = find_bond_groups(mol)
    output = len(output[0]) if output else 0
    return output

def num_compounds(mol):
    'number of molecules in mol'
    smile = Chem.MolToSmiles(mol)
    return smile.count('.')+1

def num_dummies(mol):
    smile = Chem.MolToSmiles(mol)
    n_attachments = smile.count('*')
    return n_attachments

FILTER_CATALOGUES = {
    'PAINS' : FilterCatalog(FilterCatalogParams.FilterCatalogs.PAINS),
    'PAINS_A' : FilterCatalog(FilterCatalogParams.FilterCatalogs.PAINS_A),
    'PAINS_B' : FilterCatalog(FilterCatalogParams.FilterCatalogs.PAINS_B),
    'PAINS_C' : FilterCatalog(FilterCatalogParams.FilterCatalogs.PAINS_C),
    'BRENK' : FilterCatalog(FilterCatalogParams.FilterCatalogs.BRENK),
    'NIH' : FilterCatalog(FilterCatalogParams.FilterCatalogs.NIH),
    'ZINC' : FilterCatalog(FilterCatalogParams.FilterCatalogs.ZINC),
}

PROP_FUNCS = {
    'Number of Compounds' : num_compounds,
    'Number of Dummy Atoms' : num_dummies,
    'TPSA' : rdMolDescriptors.CalcTPSA,
    'LogP' : Descriptors.MolLogP,
    
    'Molecular Weight' : rdMolDescriptors.CalcExactMolWt,
    'Heavy Atom Count' : rdMolDescriptors.CalcNumHeavyAtoms,
    'Atom Count' : rdMolDescriptors.CalcNumAtoms,
    'Heteroatom Count' : rdMolDescriptors.CalcNumHeteroatoms,
    'Spiro Atom Count' : rdMolDescriptors.CalcNumSpiroAtoms,
    'Bridgehead Atom Count' : rdMolDescriptors.CalcNumBridgeheadAtoms,
    'Stereocenter Count' : rdMolDescriptors.CalcNumAtomStereoCenters,
    
    'Hydrogen Bond Donors' : rdMolDescriptors.CalcNumHBD,
    'Hydrogen Bond Acceptors' : rdMolDescriptors.CalcNumHBA,
    
    'Formal Charge' : Chem.rdmolops.GetFormalCharge,
    
    'Rotatable Bonds' : rdMolDescriptors.CalcNumRotatableBonds,
    'Loose Rotatable Bonds' : loose_rotbond,
    'Rotatable Chain Length' : rot_chain_length,
    
    'Max Ring Size' : max_ring_size,
    'Min Ring Size' : min_ring_size,
    
    'Ring Count' : rdMolDescriptors.CalcNumRings,
    'Ring Count (Aromatic)' : rdMolDescriptors.CalcNumAromaticRings,
    'Ring Count (Saturated)' : rdMolDescriptors.CalcNumSaturatedRings,
    'Ring Count (Aliphatic)' : rdMolDescriptors.CalcNumAliphaticRings,
    
    'Heterocycle Count' : rdMolDescriptors.CalcNumHeterocycles,
    'Heterocycle Count (Aromatic)' : rdMolDescriptors.CalcNumAromaticHeterocycles,
    'Heterocycle Count (Saturated)' : rdMolDescriptors.CalcNumSaturatedHeterocycles,
    'Heterocycle Count (Aliphatic)' : rdMolDescriptors.CalcNumAliphaticHeterocycles,
    
    'Carbocycles (Aromatic)' : rdMolDescriptors.CalcNumAromaticCarbocycles,
    'Carbocycles (Saturated)' : rdMolDescriptors.CalcNumSaturatedCarbocycles,
    'Carbocycles (Aliphatic)' : rdMolDescriptors.CalcNumAliphaticCarbocycles,
    
    'Amide Bond Count' : rdMolDescriptors.CalcNumAmideBonds,
    'Fraction SP3' : rdMolDescriptors.CalcFractionCSP3,
    'QED' : QED.qed,
    'SA Score' : sascorer.calculateScore,
    'Molar Refractivity' : Descriptors.MolMR,
    'Radical Count' : Descriptors.NumRadicalElectrons,
}