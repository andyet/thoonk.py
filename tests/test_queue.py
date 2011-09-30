import thoonk
from thoonk.feeds import Queue
import unittest
from ConfigParser import ConfigParser


class TestQueue(unittest.TestCase):

    def setUp(self):
        conf = ConfigParser()
        conf.read('test.cfg')
        if conf.sections() == ['Test']:
            self.ps = thoonk.Thoonk(host=conf.get('Test', 'host'),
                                    port=conf.getint('Test', 'port'),
                                    db=conf.getint('Test', 'db'))
            self.ps.redis.flushdb()
        else:
            print 'No test configuration found in test.cfg'
            exit()

    def test_basic_queue(self):
        """Test basic QUEUE publish and retrieve."""
        q = self.ps.queue("testqueue")
        self.assertEqual(q.__class__, Queue)
        q.put("10")
        q.put("20")
        q.put("30")
        q.put("40")
        r = []
        for x in range(0,4):
            r.append(q.get(timeout=2))
        self.assertEqual(r, ["10", "20", "30", "40"], "Queue results did not match publish.")

suite = unittest.TestLoader().loadTestsFromTestCase(TestQueue)

