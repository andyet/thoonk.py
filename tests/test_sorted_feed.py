import thoonk
import unittest
from ConfigParser import ConfigParser


class TestLeaf(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

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

    def test_10_basic_sorted_feed(self):
        """Test basic sorted feed publish and retrieve."""
        l = self.ps.sorted_feed("testfeed")
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
        self.assertEqual(r, ['1', '2', '3', '4'], "Sorted feed results did not match publish: %s." % r)
        self.assertEqual(v, items, "Sorted feed items don't match: %s" % v)

    def test_20_sorted_feed_before(self):
        """Test addding an item before another item"""
        l = self.ps.sorted_feed("testfeed")
        l.publish_before('3', 'foo')
        r = l.get_ids()
        self.assertEqual(r, ['1', '2', '5', '3', '4'], "Sorted feed results did not match: %s." % r)

    def test_30_sorted_feed_after(self):
        """Test adding an item after another item"""
        l = self.ps.sorted_feed("testfeed")
        l.publish_after('3', 'foo')
        r = l.get_ids()
        self.assertEqual(r, ['1', '2', '5', '3', '6', '4'], "Sorted feed results did not match: %s." % r)

    def test_40_sorted_feed_prepend(self):
        """Test addding an item to the front of the sorted feed"""
        l = self.ps.sorted_feed("testfeed")
        l.prepend('bar')
        r = l.get_ids()
        self.assertEqual(r, ['7', '1', '2', '5', '3', '6', '4'],
                "Sorted feed results don't match: %s" % r)

    def test_50_sorted_feed_edit(self):
        """Test editing an item in a sorted feed"""
        l = self.ps.sorted_feed("testfeed")
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
                "Sorted feed results don't match: %s" % r)
        self.assertEqual(v, 'bar', "Items don't match: %s" % v)
        self.assertEqual(vs, items, "Sorted feed items don't match: %s" % vs)

    def test_60_sorted_feed_retract(self):
        """Test retracting an item from a sorted feed"""
        l = self.ps.sorted_feed("testfeed")
        l.retract('3')
        r = l.get_ids()
        self.assertEqual(r, ['7', '1', '2', '5', '6', '4'],
                "Sorted feed results don't match: %s" % r)


suite = unittest.TestLoader().loadTestsFromTestCase(TestLeaf)
