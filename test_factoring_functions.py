#!/usr/bin/env python3

from sympy.ntheory import factorint
import multiprocessing as mp
from multiprocess_generator import factorize_naive
from sympy.ntheory import factorint
from multiprocess_generator import server
from multiprocess_generator import client
import click

@click.group()
def cli():
    pass

def launch_server_and_clients(function):
    print(type(function))
    sp = mp.Process(target=server)
    cp = mp.Process(target=client, kwargs={'function':function})
    sp.start()
    cp.start()

@cli.command()
def naive_wrapped():
    launch_server_and_clients(function=factorize_naive)

#@cli.command()
#def sympy_wrapped():
#    launch_server_and_clients(function=factorize_sympy)

@cli.command()
def sympy():
    launch_server_and_clients(function=factorint)

if __name__ == '__main__':
    cli()
