from sleekpubsub.pubsub import Pubsub
from sleekpubsub.cli import CLInterface
import cProfile
import time


def testspeed(total=40000):
    p = Pubsub()
    p.create_node("speed", {})
    start = time.time()
    for x in range(1,total):
        p.publish("speed", x, x)
    tt = time.time() - start
    print tt, total / tt

cProfile.run('testspeed()')
