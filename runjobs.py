from sleekpubsub.pubsub import Pubsub
from sleekpubsub.cli import CLInterface
import cProfile
import time
import math
import sys

def runjobs(listen=False):
    p = Pubsub()
    job_channel = p.job("jobtest")
    x = 0
    while True:
        id, query = job_channel.get()
        job_channel.finish(id, query)
        x %= 50
        if x == 0:
            sys.stdout.write('.')
            sys.stdout.flush()
        x += 1

runjobs()
