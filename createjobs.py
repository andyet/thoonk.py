from sleekpubsub.pubsub import Pubsub
from sleekpubsub.cli import CLInterface
import cProfile
import time


def testspeed(total=1):
    p = Pubsub(listen=False)
    j = p.job("jobtest")
    start = time.time()
    for x in range(1,total):
        j.publish(x)
    tt = time.time() - start
    print tt, total / tt

testspeed(total=40000)
