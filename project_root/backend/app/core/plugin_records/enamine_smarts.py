import os 

REAL_REACTIONS = [
    {
        'id' : 22,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[OH1][C:4]([*:5])=[O:6]',
        ],
        'product' : '[*:5][C:4](=[O:6])[N:2]([*:1])[*:3]'
    },
    {
        'id' : 11,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[OH1][C:4]([*:5])=[O:6]',
        ],
        'product' : '[*:5][C:4](=[O:6])[N:2]([*:1])[*:3]'
    },
    {
        'id' : 527,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[OH1][C:4]([*:5])=[O:6]',
        ],
        'product' : '[*:5][C:4](=[O:6])[N:2]([*:1])[*:3]'
    },
    {
        'id' : 240690,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[OH1][C:4]([*:5])=[O:6]',
        ],
        'product' : '[*:5][C:4](=[O:6])[N:2]([*:1])[*:3]'
    },
    {
        'id' : 2430,
        'reactants' : [
            '[*:1][N:2]([H])[H:3]',
            '[*:4][N:5]([H])[*:6]',
        ],
        'product' : 'O=C([N:2]([*:1])[H:3])[N:5]([*:4])[*:6]'
    },
    {
        'id' : 2708,
        'reactants' : [
            '[*:1][N:2]([H])[H:3]',
            '[*:4][N:5]([H])[H:6]',
        ],
        'product' : 'O=C([N:2]([*:1])[H:3])[N:5]([*:4])[H:6]'
    },
    {
        'id' : 2230,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[F,Cl,Br,I][*:4]',
        ],
        'product' : '[*:1][N:2]([*:3])[*:4]'
    },
    {
        'id' : 2718,
        'reactants' : [
            '[*:1][N:2]([H])[H:3]',
            '[*:4][N:5]([H])[H:6]',
        ],
        'product' : 'O=C(C(=O)[N:2]([*:1])[H:3])[N:5]([*:4])[H:6]'
    },
    {
        'id' : 40,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[O:4]=[S:5](=[O:6])([F,Cl,Br,I])[*:7]',
        ],
        'product' : '[O:4]=[S:5](=[O:6])([*:7])[N:2]([*:1])[*:3]'
    },
    {
        'id' : 27,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[F,Cl,Br,I][*:4]',
        ],
        'product' : '[*:1][N:2]([*:3])[*:4]'
    },
    {
        'id' : 271948,
        'reactants' : [
            '[*:1][N:2]([H])[*:3]',
            '[*:4][N:5]([H])[H:6]',
        ],
        'product' : 'O=C(C(=O)[N:2]([*:1])[*:3])[N:5]([*:4])[H:6]'
    },
    {
        'id' : 1458,
        'reactants' : [
            '[OH1:1][C:2]([*:3])=[O:4]',
            '[F,Cl,Br,I][*:5]',
        ],
        'product' : '[O:4]=[C:2]([*:3])[O:1][*:5]'
    },
]

for reaction in REAL_REACTIONS:
    reaction['smarts'] = f"{reaction['reactants'][0]}.{reaction['reactants'][1]}>>{reaction['product']}"

ENAMINE_CREATE = [
    {
        "name": f"Enamine Reaction {reaction['id']}",
        "type": "assembly",
        "plugin_class": "internal_rdkit",
        "execution_type": "queue",
        "group_key": "rdkit_plugin",
        "timeout": 600,
        "max_concurrency": int(os.environ.get('RDKIT_CONCURRENCY', 64)),
        "max_retries": 3,
        "num_parents": 2,
        "config": {
            "single_stage_reactions": [{'smarts' : reaction['smarts'], 'requires_hs' : True}],
            "multi_stage_reactions": []
        }
    }
    for reaction in REAL_REACTIONS
]

ENAMINE_CREATE += [
    {
        "name": f"Enamine Reaction All",
        "type": "assembly",
        "plugin_class": "internal_rdkit",
        "execution_type": "queue",
        "group_key": "rdkit_plugin",
        "timeout": 600,
        "max_concurrency": int(os.environ.get('RDKIT_CONCURRENCY', 64)),
        "max_retries": 3,
        "num_parents": 2,
        "config": {
            "single_stage_reactions": [{'smarts' : reaction['smarts'], 'requires_hs' : True}
                                       for reaction in REAL_REACTIONS],
            "multi_stage_reactions": []
        }
    }
]

