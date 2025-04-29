from typing import List 

def _chunk(seq: List, n: int) -> List[List]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]