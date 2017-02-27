#!/usr/bin/env python3
from sympy.ntheory import factorint
import multiprocessing as mp
from parallel_processor import server
from parallel_processor import client

if __name__ == '__main__':
    sp = mp.Process(target=server)
    cp = mp.Process(target=client)
    sp.start()
    cp.start()
