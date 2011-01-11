import sleekpubsub
import unittest

class TestQueue(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.ps = sleekpubsub.Pubsub(db=10)
        self.ps.redis.flushdb()

    def test_basic_queue(self):
        """Test basic QUEUE publish and retrieve."""
        q = self.ps.queue("testqueue")
        q.put("10")
        q.put("20")
        q.put("30")
        q.put("40")
        r = []
        for x in range(0,4):
            r.append(q.get(timeout=2))
        self.assertEqual(r, ["10", "20", "30", "40"], "Queue results did not match publish.")

suite = unittest.TestLoader().loadTestsFromTestCase(TestQueue)
            
