from thoonk.pubsub import Pubsub
from thoonk.cli import CLInterface
import cProfile
import time


def testspeed(total=1):
    p = Pubsub(listen=False)
    j = p.job("jobtest")
    start = time.time()
    for x in range(1,total+1):
        j.publish(x)
    tt = time.time() - start
    print tt, total / tt

testspeed(total=40000)
