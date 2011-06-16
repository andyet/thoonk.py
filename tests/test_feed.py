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
        l = self.ps.feed("test2")
        l.delete_feed()

    def test_50_max_length(self):
        """Test feeds with a max length"""
        feed = self.ps.feed('testfeed2', {'max_length': 5})
        feed.publish('item-1', id='1')
        feed.publish('item-2', id='2')
        feed.publish('item-3', id='3')
        feed.publish('item-4', id='4')
        feed.publish('item-5', id='5')
        items = feed.get_all()
        expected = {
            '1': 'item-1',
            '2': 'item-2',
            '3': 'item-3',
            '4': 'item-4',
            '5': 'item-5'
        }
        self.assertEqual(expected, items,
                "Items don't match: %s" % items)

        feed2 = self.ps.feed('testfeed2')
        feed2.publish('item-6', id='6')
        items = feed2.get_all()
        del expected['1']
        expected['6'] = 'item-6'
        self.assertEqual(expected, items,
                "Maxed items don't match: %s" % items)


suite = unittest.TestLoader().loadTestsFromTestCase(TestLeaf)
