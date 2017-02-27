#!/usr/bin/env python3
from sympy.ntheory import factorint

import multiprocessing as mp
#from multiprocess_generator import factorize_naive
from multiprocess_generator import server
from multiprocess_generator import client


if __name__ == '__main__':
    sp = mp.Process(target=server)
    cp = mp.Process(target=client)
    sp.start()
    cp.start()
