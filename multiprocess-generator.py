#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
 Example of "distributed computing". Adapted by:
 http://eli.thegreenplace.net/2012/01/24/Distributed-computing-in-python-with-multiprocessing
 further adapted by: https://github.com/Dan77111/TPS/blob/master/concurrency/on-net/multi-syncmanager.py
 PUBLIC DOMAIN

 This program calculates the factors of a number of integers by feeding a
 shared Queue to N processes, possibly on different machines.
 IPC is over IP proxied/synchronized by multiprocessing.managers

 Two Queue objects are passed to each worker process:
 job_q: a queue of numbers to factor
 result_q: a queue to return factors and job stats to the server
'''
import os
import time
import queue
import multiprocessing as mp
from multiprocessing.managers import SyncManager
from multiprocessing import AuthenticationError
from sympy.ntheory import factorint
import click

@click.group()
def cli():
    pass

def make_nums(base, count):
    ''' Return list of N odd numbers '''
    return [base + i * 2 for i in range(count)]

def factorize_sympy(n):
    start = time.time()
    process_id = os.getpid()
    factors = sorted([key for key, value in factorint(n).items() for _ in range(value)])
    end = time.time()
    jobtime = str(end-start)[0:5]
    return {'factors':factors, 'pid':process_id, 'jobtime':str(end-start)[0:5]}

def factorize_naive(n):
    start = time.time()
    process_id = os.getpid()
    factors = []
    p = 2
    while True:
        if n == 1:
            end = time.time()
            return {'factors':factors, 'pid':process_id, 'jobtime':str(end-start)[0:5]}
        if n % p == 0:
            factors.append(p)
            n = n / p
        elif p * p >= n:         # n is prime now
            factors.append(n)
            end = time.time()
            return {'factors':factors, 'pid':process_id, 'jobtime':str(end-start)[0:5]}
        elif p > 2: # Advance in steps of 2 over odd numbers
            p += 2
        else:       # If p == 2, get to 3
            p += 1
    assert False, "unreachable"

def factorizer_worker(job_q, res_q):
    process_id = os.getpid()
    print('process id:', process_id)
    while True:
        try:
            job = job_q.get_nowait()
            #out_dict = {n: factorize_naive(n) for n in job}
            out_dict = {n: factorize_sympy(n) for n in job}
            res_q.put(out_dict)
        except queue.Empty:
            return

def mp_factorizer(job_q, res_q, proc_count):
    '''Create proc_count processes running factorize_worker() using the same 2 queues.'''
    print("proc_count:", proc_count)
    pp = [mp.Process(target=factorizer_worker, args=(job_q, res_q)) for i in range(proc_count)]
    for p in pp: p.start()
    for p in pp: p.join()

def make_server_manager(ip, port, authkey):
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
    return JobQueueManager(address=(ip, port), authkey=authkey)

def runserver_manager(ip, port, authkey, base, count):
    man = make_server_manager(ip=ip, port=port, authkey=authkey)
    man.start()
    print("Server pid: %d port: %s authkey: '%s'." % (os.getpid(), port, authkey))
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

def make_client_manager(ip, port, authkey):
    '''
    Creates manager for client. Manager connects to server on the
    given address and exposes the get_job_q and get_res_q methods for
    accessing the shared queues from the server. Returns a manager object.
    '''
    class ServerQueueManager(SyncManager):
        pass
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_res_q')
    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    try:
        manager.connect()
    except AuthenticationError:
        print("ERROR: Incorrect auth key:", authkey)
        quit(1)
    print('Client connected to %s:%s' % (ip, port))
    return manager

@cli.command()
@click.option('--ip', is_flag=False, required=False, default='127.0.0.1', help='Server IP.')
@click.option('--port', is_flag=False, required=False, default=5555, type=int, help='Server port.')
@click.option('--authkey', is_flag=False, required=False, default=b'98sdf..xwXiia39', type=bytes, help='Server key.')
@click.option('--processes', is_flag=False, required=True, type=int, help='Client processes to spawn.')
def client(ip, port, authkey, processes):
    '''
    Client creates a client_manager from which obtains the two proxies to the Queues
    Then runs mp_factorizer to execute processes that factorize
    '''
    man = make_client_manager(ip=ip, port=port, authkey=authkey)
    job_q = man.get_job_q()
    res_q = man.get_res_q()
    mp_factorizer(job_q, res_q, processes)

@cli.command()
@click.option('--ip', is_flag=False, required=False, default='127.0.0.1', help='Server IP.')
@click.option('--port', is_flag=False, required=False, default=5555, type=int, help='Server port.')
@click.option('--authkey', is_flag=False, required=False, default=b'98sdf..xwXiia39', type=bytes, help='Server key.')
@click.option('--base', is_flag=False, required=True, type=int, help='Smallest number to factorize.')
@click.option('--count', is_flag=False, required=True, type=int, help='Number of numbers to factorize.')
def server(ip, port, authkey, base, count):
    print("Server on port %d with key '%s'" % (port, authkey))
    print("Factorizing %d odd numbers starting from %d" % (count, base))
    start = time.time() # not reliable because the client has gotta be manually started
    d = runserver_manager(ip=ip, port=port, authkey=authkey, base=base, count=count)
    passed = time.time() - start
    for k in sorted(d):
        pid = d[k]['pid']
        factors = d[k]['factors']
        jobtime = d[k]['jobtime']
        print(pid, k, jobtime, factors)
    print("Factorized %d numbers in %.2f seconds." % (count, passed))

if __name__ == '__main__':
    cli()
