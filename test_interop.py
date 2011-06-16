import sys
import thoonk
import time
import unittest
from optparse import OptionParser


CONFIG = {}


class TestInteropDriver(unittest.TestCase):

    """
    """

    def setUp(self):
        self.thoonk = thoonk.Thoonk(CONFIG['host'], CONFIG['port'], CONFIG['db'])
        self.driver_queue= self.thoonk.queue('interop-testing-driver')
        self.follower_queue = self.thoonk.queue('interop-testing-follower')

    def wait(self):
        self.driver_queue.get()

    def proceed(self):
        self.follower_queue.put('token')

    def test_10_feeds(self):
        """Test interop for Feeds -- 1"""
        feed = self.thoonk.feed('interop-test-feed')
        feed.publish('test item', id='1')
        self.proceed()

    def test_11_feeds(self):
        """Test interop for Feeds -- 2"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        feed.publish('edited item', id='1')
        self.proceed()

    def test_12_feeds(self):
        """Test interop for Feeds -- 3"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        feed.retract('1')
        self.proceed()

    def test_13_feeds(self):
        """Test interop for Feeds -- 4"""
        feed = self.thoonk.feed('interop-test-feed')
        feed.config = {'max_length': 5}
        self.wait()
        feed.publish('item-1', id='1')
        time.sleep(0.1)
        feed.publish('item-2', id='2')
        time.sleep(0.1)
        feed.publish('item-3', id='3')
        time.sleep(0.1)
        feed.publish('item-4', id='4')
        time.sleep(0.1)
        feed.publish('item-5', id='5')
        time.sleep(0.1)
        self.proceed()

    def test_14_feeds(self):
        """Test interop for Feeds -- 5"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        feed.publish('item-6', id='6')
        time.sleep(.1)
        self.proceed()

    def test_15_feeds(self):
        """Test interop for Feeds -- 6"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        feed.publish('edited item-4', id='4')
        self.proceed()

    def test_20_lists(self):
        """Test interop for Lists"""
        pass

    def test_30_queues(self):
        """Test interop for Queues"""
        pass

    def test_40_jobs(self):
        """Test interop for Jobs"""
        pass


class TestInteropFollower(unittest.TestCase):

    """
    """

    def setUp(self):
        self.thoonk = thoonk.Thoonk(CONFIG['host'], CONFIG['port'], CONFIG['db'])
        self.driver_queue = self.thoonk.queue('interop-testing-driver')
        self.follower_queue = self.thoonk.queue('interop-testing-follower')

    def tearDown(self):
        self.proceed()

    def proceed(self):
        self.driver_queue.put('token')

    def wait(self):
        self.follower_queue.get()

    def test_10_feeds(self):
        """Test interop for Feeds -- 1"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        items = feed.get_all()
        self.assertEqual({'1': 'test item'}, items,
                "Items don't match: %s" % items)

    def test_11_feeds(self):
        """Test interop for Feeds -- 2"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        items = feed.get_all()
        self.assertEqual({'1': 'edited item'}, items,
                "Items don't match: %s" % items)

    def test_12_feeds(self):
        """Test interop for Feeds -- 3"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        items = feed.get_all()
        self.assertEqual({}, items,
                "Items were not retracted: %s" % items)

    def test_13_feeds(self):
        """Test interop for Feeds -- 4"""
        feed = self.thoonk.feed('interop-test-feed', {'max_length':5})
        self.wait()
        items = feed.get_all()
        ids = feed.get_ids()
        expected = {
            '1': 'item-1',
            '2': 'item-2',
            '3': 'item-3',
            '4': 'item-4',
            '5': 'item-5'
        }
        self.assertEqual(expected, items,
                "Items don't match: %s" % items)
        self.assertEqual(['1','2','3','4','5'], ids,
                "Items not in order: %s" % ids)

    def test_14_feeds(self):
        """Test interop for Feeds -- 5"""
        feed = self.thoonk.feed('interop-test-feed', {'max_length':5})
        self.wait()
        items = feed.get_all()
        ids = feed.get_ids()
        expected = {
            '2': 'item-2',
            '3': 'item-3',
            '4': 'item-4',
            '5': 'item-5',
            '6': 'item-6'
        }
        self.assertEqual(expected, items,
                "Items don't match: %s" % items)
        self.assertEqual(['2','3','4','5','6'], ids,
                "Items not in order: %s" % ids)

    def test_15_feeds(self):
        """Test interop for Feeds -- 6"""
        feed = self.thoonk.feed('interop-test-feed')
        self.wait()
        items = feed.get_all()
        ids = feed.get_ids()
        expected = {
            '3': 'item-3',
            '4': 'edited item-4',
            '5': 'item-5',
            '6': 'item-6'
        }
        self.assertEqual(expected, items,
                "Items don't match: %s" % items)
        self.assertEqual(['3','5','6','4'], ids,
                "Items not in order: %s" % ids)

    def test_20_lists(self):
        """Test interop for Lists"""
        pass

    def test_30_queues(self):
        """Test interop for Queues"""
        pass

    def test_40_jobs(self):
        """Test interop for Jobs"""
        pass


if __name__ == '__main__':
    optp = OptionParser()
    optp.add_option('-s', '--server', help='set Redis host',
                    dest='server', default='localhost')
    optp.add_option('-p', '--port', help='set Redis host',
                    dest='port', default=6379)
    optp.add_option('-d', '--db', help='set Redis db',
                    dest='db', default=10)

    opts, args = optp.parse_args()

    CONFIG['host'] = opts.server
    CONFIG['port'] = opts.port
    CONFIG['db'] = opts.db

    if args[0] == 'driver':
        t = thoonk.Thoonk(opts.server, opts.port, opts.db)
        t.redis.flushdb()
        test_class = TestInteropDriver
    else:
        test_class = TestInteropFollower

    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    unittest.TextTestRunner(verbosity=2).run(suite)
