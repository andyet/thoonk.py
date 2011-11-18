import thoonk
from thoonk.feeds import SortedFeed
import unittest
from ConfigParser import ConfigParser


class TestLeaf(unittest.TestCase):
    
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
    
    def test_10_basic_sorted_feed(self):
        """Test basic sorted feed publish and retrieve."""
        l = self.ps.sorted_feed("sortedfeed")
        self.assertEqual(l.__class__, SortedFeed)
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
        l = self.ps.sorted_feed("sortedfeed")
        l.publish("hi")
        l.publish("bye")
        l.publish_before('2', 'foo')
        r = l.get_ids()
        self.assertEqual(r, ['1', '3', '2'], "Sorted feed results did not match: %s." % r)

    def test_30_sorted_feed_after(self):
        """Test adding an item after another item"""
        l = self.ps.sorted_feed("sortedfeed")
        l.publish("hi")
        l.publish("bye")
        l.publish_after('1', 'foo')
        r = l.get_ids()
        self.assertEqual(r, ['1', '3', '2'], "Sorted feed results did not match: %s." % r)

    def test_40_sorted_feed_prepend(self):
        """Test addding an item to the front of the sorted feed"""
        l = self.ps.sorted_feed("sortedfeed")
        l.publish("hi")
        l.publish("bye")
        l.prepend('bar')
        r = l.get_ids()
        self.assertEqual(r, ['3', '1', '2'],
                "Sorted feed results don't match: %s" % r)

    def test_50_sorted_feed_edit(self):
        """Test editing an item in a sorted feed"""
        l = self.ps.sorted_feed("sortedfeed")
        l.publish("hi")
        l.publish("bye")
        l.edit('1', 'bar')
        r = l.get_ids()
        v = l.get_item('1')
        vs = l.get_items()
        items = {'1': 'bar',
                 '2': 'bye'}
        self.assertEqual(r, ['1', '2'],
                "Sorted feed results don't match: %s" % r)
        self.assertEqual(v, 'bar', "Items don't match: %s" % v)
        self.assertEqual(vs, items, "Sorted feed items don't match: %s" % vs)

    def test_60_sorted_feed_retract(self):
        """Test retracting an item from a sorted feed"""
        l = self.ps.sorted_feed("sortedfeed")
        l.publish("hi")
        l.publish("bye")
        l.publish("thanks")
        l.publish("you're welcome")
        l.retract('3')
        r = l.get_ids()
        self.assertEqual(r, ['1', '2', '4'],
                "Sorted feed results don't match: %s" % r)

    def test_70_sorted_feed_move_first(self):
        """Test moving items around in the feed."""
        l = self.ps.sorted_feed('sortedfeed')
        l.publish("hi")
        l.publish("bye")
        l.publish("thanks")
        l.publish("you're welcome")
        l.move_first('4')
        r = l.get_ids()
        self.assertEqual(r, ['4', '1', '2', '3'],
                "Sorted feed results don't match: %s" % r)

    def test_71_sorted_feed_move_last(self):
        """Test moving items around in the feed."""
        l = self.ps.sorted_feed('sortedfeed')
        l.publish("hi")
        l.publish("bye")
        l.publish("thanks")
        l.publish("you're welcome")
        l.move_last('2')
        r = l.get_ids()
        self.assertEqual(r, ['1', '3', '4', '2'],
                "Sorted feed results don't match: %s" % r)


    def test_72_sorted_feed_move_before(self):
        """Test moving items around in the feed."""
        l = self.ps.sorted_feed('sortedfeed')
        l.publish("hi")
        l.publish("bye")
        l.publish("thanks")
        l.publish("you're welcome")
        l.move_before('1', '2')
        r = l.get_ids()
        self.assertEqual(r, ['2', '1', '3', '4'],
                "Sorted feed results don't match: %s" % r)

    def test_73_sorted_feed_move_after(self):
        """Test moving items around in the feed."""
        l = self.ps.sorted_feed('sortedfeed')
        l.publish("hi")
        l.publish("bye")
        l.publish("thanks")
        l.publish("you're welcome")
        l.move_after('1', '4')
        r = l.get_ids()
        self.assertEqual(r, ['1', '4', '2', '3'],
                "Sorted feed results don't match: %s" % r)


suite = unittest.TestLoader().loadTestsFromTestCase(TestLeaf)
