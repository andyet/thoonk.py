import thoonk
import unittest

class TestLeaf(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.ps = thoonk.Pubsub(db=10)
        self.ps.redis.flushdb()

    def test_05_basic_retract(self):
        """Test adding and retracting an item."""
        l = self.ps.feed("testfeed")
        l.publish('foo', id='1')
        r = l.get_ids()
        v = l.get_all()
        self.assertEqual(r, ['1'], "Feed results did not match publish: %s." % r)
        self.assertEqual(v, {'1': 'foo'}, "Feed contents did not match publish: %s." % r)
        l.retract('1')
        r = l.get_ids()
        v = l.get_all()
        self.assertEqual(r, [], "Feed results did not match: %s." % r)
        self.assertEqual(v, {}, "Feed contents did not match: %s." % r)

    def test_10_basic_feed(self):
        """Test basic LEAF publish and retrieve."""
        l = self.ps.feed("testfeed")
        l.publish("hi", id='1')
        l.publish("bye", id='2')
        l.publish("thanks", id='3')
        l.publish("you're welcome", id='4')
        r = l.get_ids()
        self.assertEqual(r, ['1', '2', '3', '4'], "Queue results did not match publish: %s." % r)

    def test_20_basic_feed_items(self):
        """Test items match completely."""
        l = self.ps.feed("testfeed")
        r = l.get_ids()
        self.assertEqual(r, ['1', '2', '3', '4'], "Queue results did not match publish: %s" % r)
        c = {}
        for id in r:
            c[id] = l.get_item(id)
        self.assertEqual(c, {'1': 'hi', '3': 'thanks', '2': 'bye', '4': "you're welcome"}, "Queue items did not match publish: %s" % c)

    def test_30_basic_feed_retract(self):
        """Testing item retract items match."""
        l = self.ps.feed("testfeed")
        l.retract('3')
        r = l.get_ids()
        self.assertEqual(r, ['1', '2','4'], "Queue results did not match publish: %s" % r)
        c = {}
        for id in r:
            c[id] = l.get_item(id)
        self.assertEqual(c, {'1': 'hi', '2': 'bye', '4': "you're welcome"}, "Queue items did not match publish: %s" % c)

    def test_40_create_delete(self):
        """Testing feed delete"""
        ps = thoonk.Pubsub(db=10)
        ps.redis.flushdb()
        l = ps.feed("test2")
        l.delete_feed()


suite = unittest.TestLoader().loadTestsFromTestCase(TestLeaf)
