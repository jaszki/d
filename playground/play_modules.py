import os
import sys

if __name__ == "__main__":
    from src.misc.misc_modules import testf
    print(f"TEST:\n{testf()}\n\n")
    
    import src.misc.misc_modules
    print("DIR:\n")
    for d in dir(src.misc.misc_modules):
        print(f"{d}: {globals().get(d, "Not found")}")
    print(f"\nTEST:\n{src.misc.misc_modules.testf()}\n\n")
    
    from src.misc import misc_modules
    print("DIR:\n")
    for d in dir(misc_modules):
        print(f"{d}: {globals().get(d, "Not found")}")
    print(f"\nTEST:\n{misc_modules.testf()}")