from thoonk import Pubsub
import cProfile
import time
import math
import sys

def runjobs(listen=False):
    p = Pubsub()
    job_channel = p.job("jobtest")
    x = 0
    starttime = time.time()
    while True:
        id, query = job_channel.get()
        #if x%2:
        job_channel.finish(id, query)
        x += 1
        if time.time() > starttime + 1.0:
            print "%d/sec" % x
            x = 0
            starttime = time.time()


runjobs()
