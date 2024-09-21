from datetime import datetime 

def date_print(s):
    print(f"{str(datetime.now())} - RDKit Plugin Worker: {s}")
    