import sys
import redis
import thoonk
import unittest
import threading

import ConfigParser

from . import ThoonkTest


class FeedTest(ThoonkTest):

    def setUp(self):
        self.reset_redis()
        self.start_thoonk()
        self.thoonk.register_type(thoonk.Feed)

    def test_publish_no_id(self):
        """Test publishing to a feed without an ID"""

        events = []
        ready = threading.Event()
        def on_publish(*args):
            events.append(args[1])
            ready.set()

        feed = self.thoonk.create('feed', 'test_publish_no_id')
        feed.on('publish', on_publish)
        item_id = feed.publish('data')

        ready.wait(1)
        self.assertEqual(events, ['data'])

        retrieved = feed.get_item(item_id)

        self.assertEqual(retrieved, 'data')

    def test_publish_id(self):
        """Test publishing to a feed with an ID"""

        events = []
        ready1 = threading.Event()
        ready2 = threading.Event()

        def on_publish(*args):
            events.append(args)
            ready1.set()

        def on_publishid(*args):
            events.append(args)
            ready2.set()

        feed = self.thoonk.create('feed', 'test_publish_id')
        feed.on('publish', on_publish)
        feed.on('publishid:testid', on_publishid)
        item_id = feed.publish('data_with_id', 'testid')

        ready1.wait(1)
        ready2.wait(1)

        self.assertEqual(events, [('testid', 'data_with_id'),
                                  ('testid', 'data_with_id')])

        retrieved = feed.get_item(item_id)

        self.assertEqual(retrieved, 'data_with_id')

    def test_get_all(self):
        """Test retrieving all feed items"""

        feed = self.thoonk.create('feed', 'test_get_all')
        feed.publish('foo', '1')
        feed.publish('bar', '2')
        feed.publish('baz', '3')
        feed.publish({'a': 42}, '4')

        items = feed.get_all()

        self.assertEqual(items, {
            '1': 'foo',
            '2': 'bar',
            '3': 'baz',
            '4': {'a': 42}
        })

        ids = feed.get_ids()
        self.assertEqual(ids, set(['1', '2', '3', '4']))

    def test_retract(self):
        """Test retracting items"""

        events = []
        ready = threading.Event()
        def on_retract(*args):
            events.append(args)
            ready.set()

        feed = self.thoonk.create('feed', 'test_retract')
        feed.on('retract', on_retract)

        feed.publish('foo', '1')
        feed.publish('bar', '2')
        feed.retract('1')

        item = feed.get_item('1')
        items = feed.get_all()
        ids = feed.get_ids()

        ready.wait(1)

        self.assertEqual(item, None)
        self.assertEqual(items, {'2': 'bar'})
        self.assertEqual(ids, set(['2']))
        self.assertEqual(events, [('1', )])

    def test_edit(self):
        """Test editing an item"""

        events = []
        ready1 = threading.Event()
        ready2 = threading.Event()

        def on_edit(*args):
            events.append(args)
            ready1.set()

        def on_editid(*args):
            events.append(args)
            ready2.set()

        feed = self.thoonk.create('feed', 'test_edit')
        feed.on('edit', on_edit)
        feed.on('editid:testid', on_editid)

        feed.publish('old data', 'testid')
        feed.publish('new data', 'testid')

        ready1.wait(1)
        ready2.wait(1)

        self.assertEqual(events, [('testid', 'new data'),
                                  ('testid', 'new data')])

        retrieved = feed.get_item('testid')

        self.assertEqual(retrieved, 'new data')

    def test_max_length(self):
        """Test feed with max_length config"""

        events = []
        ready = threading.Event()
        def on_retract(*args):
            events.append(args)
            ready.set()

        feed = self.thoonk.create('feed', 'test_max_length')
        feed.config({'max_length': 2})
        feed.on('retract', on_retract)

        feed.publish('1', '1')
        feed.publish('2', '2')
        feed.publish('3', '3')

        ready.wait(1)

        self.assertEqual(events, [('1',)])
        self.assertEqual(feed.get_ids(), set(['2', '3']))


suite = unittest.TestLoader().loadTestsFromTestCase(FeedTest)
