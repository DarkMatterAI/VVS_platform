from datetime import datetime 
from collections import OrderedDict

def date_print(s):
    print(f"{str(datetime.now())} - RDKit Plugin Worker: {s}")
    
def deduplicate_list(input, key_func=None):
    'deduplicates list while maintaining order'
    if key_func:
        od = OrderedDict()
        for item in input:
            od[key_func(item)] = item
        output = list(od.values())
    else:
        output = list(OrderedDict.fromkeys(input))
    
    return output