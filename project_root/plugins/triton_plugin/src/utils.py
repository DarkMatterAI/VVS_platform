import numpy as np 

def parse_size(size_kwargs, sizes, prefix, expected=1):
    parsed = []
    for size in sizes:
        if size_kwargs.get(f"{prefix}_{size}") is not None:
            parsed.append(size)
    assert len(parsed)==expected, f"expected exactly {expected} {prefix} size, found {len(parsed)}, {parsed}"
    return parsed[0]

def parse_sequence_batch(sequence):
    sequence_batch = np.char.decode(sequence.astype("bytes"), "utf-8")
    inputs = []
    for sequence_item in sequence_batch:
        inputs.append(sequence_item.item())
    return inputs 