import sleekpubsub
import unittest

class TestLeaf(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.ps = sleekpubsub.Pubsub(db=10)
        self.ps.redis.flushdb()

    def test_10_basic_leaf(self):
        """Test basic LEAF publish and retrieve."""
        l = self.ps.leaf("testleaf")
        l.publish("hi", id='1')
        l.publish("bye", id='2')
        l.publish("thanks", id='3')
        l.publish("you're welcome", id='4')
        r = l.get_items()
        self.assertEqual(r, ['4', '3', '2','1'], "Queue results did not match publish.")

    def test_20_basic_leaf_items(self):
        """Test items match completely."""
        l = self.ps.leaf("testleaf")
        r = l.get_items()
        self.assertEqual(r, ['4', '3', '2','1'], "Queue results did not match publish.")
        c = {}
        for id in r:
            c[id] = l.get_item(id)
        self.assertEqual(c, {'1': 'hi', '3': 'thanks', '2': 'bye', '4': "you're welcome"}, "Queue items did not match publish.")
    
    def test_30_basic_leaf_retract(self):
        """Testing item retract items match."""
        l = self.ps.leaf("testleaf")
        l.retract('3')
        r = l.get_items()
        self.assertEqual(r, ['4', '2','1'], "Queue results did not match publish.")
        c = {}
        for id in r:
            c[id] = l.get_item(id)
        self.assertEqual(c, {'1': 'hi', '2': 'bye', '4': "you're welcome"}, "Queue items did not match publish.")


suite = unittest.TestLoader().loadTestsFromTestCase(TestLeaf)
