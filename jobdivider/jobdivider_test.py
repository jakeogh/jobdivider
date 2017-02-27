#!/usr/bin/env python3
from sympy.ntheory import factorint
import multiprocessing as mp
#from jobdivider import server
#from jobdivider import client
from jobdivider import jobdivider_test

#def jobdivider_test():
#    sp = mp.Process(target=server)
#    cp = mp.Process(target=client)
#    sp.start()
#    cp.start()

if __name__ == '__main__':
    jobdivider_test()
