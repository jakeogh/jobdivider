#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Example of "distributed computing".
Originally from:
   http://eli.thegreenplace.net/2012/01/24/Distributed-computing-in-python-with-multiprocessing
Modified by:
   https://github.com/Dan77111/TPS/blob/master/concurrency/on-net/multi-syncmanager.py
Then by:
   https://github.com/jakeogh/jobdivider

License: PUBLIC DOMAIN

This program takes a queue of jobs and distributes it over over N processes,
each running a specified function on the job and returning the results.

The built-in example calculates the factors of a number of integers by
feeding a shared Queue to N processes, possibly on different machines.

IPC is over IP proxied/synchronized by multiprocessing.managers

Two Queue objects are passed to each worker process:
job_q:      a queue of numbers to factor
res_q:   a queue to return factors and job stats to the server

'''

import os
import time
import queue
import multiprocessing as mp
from multiprocessing.managers import SyncManager
from multiprocessing import AuthenticationError
from sympy.ntheory import factorint
from kcl.printops import cprint
import pprint
import click
from .jobdivider_test import jobdivider_test

DEFAULT_START = 999999999999
DEFAULT_COUNT = 1000
DEFAULT_PROCESSES = 4
# benchmarks on: i7-2620M CPU @ 2.70GHz
# naive: 17.72s +- 1.00s
# sympy: 2.19s  +- 0.01s

DEFAULT_PORT = 5555
DEFAULT_IP = '127.0.0.1'
DEFAULT_AUTH = '98sdf..xwXiia39'
DEFAULT_FUNCTION = factorint

@click.group()
def cli():
    pass

def make_nums(base, count):
    ''' Return list of N odd numbers '''
    return [base + i * 2 for i in range(count)]

def factorize_naive(n):
    factors = []
    p = 2
    while True:
        if n == 1:
            return factors
        if n % p == 0:
            factors.append(p)
            n = n / p
        elif p * p >= n:            # n is prime now
            factors.append(int(n))  #not sure why that's a float
            return factors
        elif p > 2:                 # advance in steps of 2 over odd numbers
            p += 2
        else:                       # If p == 2, get to 3
            p += 1
    assert False, "unreachable"

def factorize_worker(job_q, res_q, function):
    worker_start = time.time()
    process_id = os.getpid()
    cprint('process id:', process_id)
    while True:
        try:
            job = job_q.get_nowait()
            out_dict = {}
            for n in job:
                job_start = time.time()
                out_dict[n] = {'result':function(n)}
                job_time = time.time() - job_start
                out_dict[n]['time'] = round(job_time, 5)
                out_dict[n]['pid'] = process_id
            res_q.put(out_dict)
        except queue.Empty:
            return

def mp_factorizer(job_q, res_q, proc_count, function):
    '''Create proc_count processes running factorize_worker() using the same 2 queues.'''
    print("proc_count:", proc_count)
    pp = [mp.Process(target=factorize_worker, args=(job_q, res_q, function)) for i in range(proc_count)]
    for p in pp: p.start()
    for p in pp: p.join()

def make_server_manager(ip, port, auth):
    '''
    Manager a process listening on port accepting connections from clients
    Clients run two .register() methods to get access to the shared Queues
    '''
    job_q = queue.Queue()
    res_q = queue.Queue()
    class JobQueueManager(SyncManager):
        pass
    JobQueueManager.register('get_job_q', callable=lambda: job_q)
    JobQueueManager.register('get_res_q', callable=lambda: res_q)
    return JobQueueManager(address=(ip, port), authkey=auth)

def runserver_manager(ip, port, auth, base, count):
    man = make_server_manager(ip=ip, port=port, auth=auth)
    man.start()
    print("Server pid: %d" % os.getpid())
    print("Server port: %s" % port)
    print("Server auth: %s" % auth)
    job_q = man.get_job_q()
    res_q = man.get_res_q()

    nums = make_nums(base, count)
    chunksize = 43
    for i in range(0, len(nums), chunksize):
        job_q.put(nums[i:i + chunksize])

    # count results until all expected results are in.
    res_count = 0
    res_dict = {}
    while res_count < count:
        out_dict = res_q.get()
        res_dict.update(out_dict)
        res_count += len(out_dict)

    # Sleep before shutting down the server to give clients time to realize
    # the job queue is empty and exit in an orderly way.
    time.sleep(1)
    man.shutdown()
    return res_dict

def make_client_manager(ip, port, auth):
    '''
    Creates manager for client. Manager connects to server on the
    given address and exposes the get_job_q and get_res_q methods for
    accessing the shared queues from the server. Returns a manager object.
    '''
    class ServerQueueManager(SyncManager):
        pass
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_res_q')
    manager = ServerQueueManager(address=(ip, port), authkey=auth)
    try:
        manager.connect()
    except AuthenticationError:
        print("ERROR: Incorrect auth key:", auth)
        quit(1)
    print('Client connected to %s:%s' % (ip, port))
    return manager

def client(ip=DEFAULT_IP, port=DEFAULT_PORT, auth=DEFAULT_AUTH, processes=DEFAULT_PROCESSES, function=DEFAULT_FUNCTION):
    auth = auth.encode('ascii')
    '''
    Client creates a client_manager from which obtains the two proxies to the Queues
    Then runs mp_factorizer to execute processes that factorize
    '''
    while True:
        try:
            man = make_client_manager(ip=ip, port=port, auth=auth)
            break
        except ConnectionRefusedError:
            time.sleep(0.1)

    job_q = man.get_job_q()
    res_q = man.get_res_q()
    mp_factorizer(job_q, res_q, processes, function)

def server(ip=DEFAULT_IP, port=DEFAULT_PORT, auth=DEFAULT_AUTH, base=DEFAULT_START, count=DEFAULT_COUNT):
    auth = auth.encode('ascii')
    start = time.time() # not correct unless client is programatically started
    d = runserver_manager(ip=ip, port=port, auth=auth, base=base, count=count)
    passed = time.time() - start
    pprint.pprint(d)
    print("total time:", passed)
    print("results:", len(d.keys()))

@cli.command()
@click.option('--ip', is_flag=False, required=False, default=DEFAULT_IP, help='Server IP.')
@click.option('--port', is_flag=False, required=False, default=DEFAULT_PORT, type=int, help='Server port.')
@click.option('--auth', is_flag=False, required=False, default=DEFAULT_AUTH, type=str, help='Server key.')
@click.option('--processes', is_flag=False, required=False, default=DEFAULT_PROCESSES, type=int, help='processes to spawn.')
def runclient(ip=DEFAULT_IP, port=DEFAULT_PORT, auth=DEFAULT_AUTH, processes=DEFAULT_PROCESSES, function=DEFAULT_FUNCTION):
    client(ip=ip, port=port, auth=auth, processes=processes)

@cli.command()
@click.option('--ip', is_flag=False, required=False, default=DEFAULT_IP, help='Server IP.')
@click.option('--port', is_flag=False, required=False, default=DEFAULT_PORT, type=int, help='Server port.')
@click.option('--auth', is_flag=False, required=False, default=DEFAULT_AUTH, type=str, help='Server key.')
@click.option('--base', is_flag=False, required=False, default=DEFAULT_START, type=int, help='Smallest number to factorize.')
@click.option('--count', is_flag=False, required=False, default=DEFAULT_COUNT, type=int, help='Number of numbers to factorize.')
def runserver(ip=DEFAULT_IP, port=DEFAULT_PORT, auth=DEFAULT_AUTH, base=DEFAULT_START, count=DEFAULT_COUNT):
    server(ip=ip, port=port, auth=auth, base=base, count=count)

def jobdivider_test():
    sp = mp.Process(target=server)
    cp = mp.Process(target=client)
    sp.start()
    cp.start()

@cli.command()
def test():
   jobdivider_test()

if __name__ == '__main__':
    cli()
