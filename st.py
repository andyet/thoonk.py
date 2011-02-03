#!/usr/bin/python
from sleekpubsub.pubsub import Pubsub
from sleekpubsub.cli import CLInterface
import cProfile
import time


def testspeed(total=40000):
    p = Pubsub()
    n = p.leaf("speed")
    start = time.time()
    for x in range(1,total):
        n.publish(x, x)
    tt = time.time() - start
    print tt, total / tt

cProfile.run('testspeed()')
#testspeed()
