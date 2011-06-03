import thoonk
import unittest

class TestLeaf(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.ps = thoonk.Pubsub(db=10)
        self.ps.redis.flushdb()

    def test_10_basic_list(self):
        """Test basic list publish and retrieve."""
        l = self.ps.list("testfeed")
        l.publish("hi")
        l.publish("bye")
        l.publish("thanks")
        l.publish("you're welcome")
        r = l.get_ids()
        v = l.get_items()
        items = {'1': 'hi',
                 '2': 'bye',
                 '3': 'thanks',
                 '4': "you're welcome"}
        self.assertEqual(r, ['1', '2', '3', '4'], "List results did not match publish: %s." % r)
        self.assertEqual(v, items, "List items don't match: %s" % v)

    def test_20_list_before(self):
        """Test addding an item before another item"""
        l = self.ps.list("testfeed")
        l.publish_before('3', 'foo')
        r = l.get_ids()
        self.assertEqual(r, ['1', '2', '5', '3', '4'], "List results did not match: %s." % r)

    def test_30_list_after(self):
        """Test adding an item after another item"""
        l = self.ps.list("testfeed")
        l.publish_after('3', 'foo')
        r = l.get_ids()
        self.assertEqual(r, ['1', '2', '5', '3', '6', '4'], "List results did not match: %s." % r)

    def test_40_list_prepend(self):
        """Test addding an item to the front of the list"""
        l = self.ps.list("testfeed")
        l.prepend('bar')
        r = l.get_ids()
        self.assertEqual(r, ['7', '1', '2', '5', '3', '6', '4'],
                "List results don't match: %s" % r)

    def test_50_list_edit(self):
        """Test editing an item in a list"""
        l = self.ps.list("testfeed")
        l.edit('6', 'bar')
        r = l.get_ids()
        v = l.get_item('6')
        vs = l.get_items()
        items = {'1': 'hi',
                 '2': 'bye',
                 '3': 'thanks',
                 '4': "you're welcome",
                 '5': 'foo',
                 '6': 'bar',
                 '7': 'bar'}
        self.assertEqual(r, ['7', '1', '2', '5', '3', '6', '4'],
                "List results don't match: %s" % r)
        self.assertEqual(v, 'bar', "Items don't match: %s" % v)
        self.assertEqual(vs, items, "List items don't match: %s" % vs)

    def test_60_list_retract(self):
        """Test retracting an item from a list"""
        l = self.ps.list("testfeed")
        l.retract('3')
        r = l.get_ids()
        self.assertEqual(r, ['7', '1', '2', '5', '6', '4'],
                "List results don't match: %s" % r)


suite = unittest.TestLoader().loadTestsFromTestCase(TestLeaf)
