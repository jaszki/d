import os
import sys

from src.test import test

if __name__ == "__main__":
    for d in dir():
        print(f"{d}: {globals().get(d, "Var not found")}")
    test()

