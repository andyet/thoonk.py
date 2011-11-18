import thoonk
from thoonk.feeds import Feed, Job
import unittest
import time
import redis
from ConfigParser import ConfigParser
import threading

class TestNotice(unittest.TestCase):

    def setUp(self):
        conf = ConfigParser()
        conf.read('test.cfg')
        if conf.sections() == ['Test']:
            redis.Redis(host=conf.get('Test', 'host'),
                        port=conf.getint('Test', 'port'),
                        db=conf.getint('Test', 'db')).flushdb()
            self.ps = thoonk.Thoonk(host=conf.get('Test', 'host'),
                                    port=conf.getint('Test', 'port'),
                                    db=conf.getint('Test', 'db'),
                                    listen=True)
        else:
            print 'No test configuration found in test.cfg'
            exit()

    def tearDown(self):
        self.ps.close()

    "claimed, cancelled, stalled, finished"
    def test_01_feed_notices(self):
        """Test for create, publish, edit, retract and delete notices from feeds"""
        
        """Feed Create Event"""
        create_event = threading.Event()
        def create_handler(feed):
            self.assertEqual(feed, "test_notices")
            create_event.set()
        
        self.ps.register_handler("create", create_handler)
        l = self.ps.feed("test_notices")
        create_event.wait(1)
        self.assertTrue(create_event.isSet(), "Create notice not received")
        self.ps.remove_handler("create", create_handler)
        
        """Feed Publish Event"""
        publish_event = threading.Event()
        ids = [None, None]
        
        def received_handler(feed, item, id):
            self.assertEqual(feed, "test_notices")
            ids[1] = id
            publish_event.set()
        
        self.ps.register_handler('publish', received_handler)
        ids[0] = l.publish('a')
        publish_event.wait(1)

        self.assertTrue(publish_event.isSet(), "Publish notice not received")
        self.assertEqual(ids[1], ids[0])
        self.ps.remove_handler('publish', received_handler)
        
        """Feed Edit Event """
        edit_event = threading.Event()
        def edit_handler(feed, item, id):
            self.assertEqual(feed, "test_notices")
            ids[1] = id
            edit_event.set()

        self.ps.register_handler('edit', edit_handler)
        l.publish('b', id=ids[0])
        edit_event.wait(1)

        self.assertTrue(edit_event.isSet(), "Edit notice not received")
        self.assertEqual(ids[1], ids[0])
        self.ps.remove_handler('edit', edit_handler)
        
        """Feed Retract Event"""
        retract_event = threading.Event()
        def retract_handler(feed, id):
            self.assertEqual(feed, "test_notices")
            ids[1] = id
            retract_event.set()

        self.ps.register_handler('retract', retract_handler)
        l.retract(ids[0])
        retract_event.wait(1)

        self.assertTrue(retract_event.isSet(), "Retract notice not received")
        self.assertEqual(ids[1], ids[0])
        self.ps.remove_handler('retract', retract_handler)
        
        """Feed Delete Event"""
        delete_event = threading.Event()
        def delete_handler(feed):
            self.assertEqual(feed, "test_notices")
            delete_event.set()
        
        self.ps.register_handler("delete", delete_handler)
        l.delete_feed()
        delete_event.wait(1)
        self.assertTrue(delete_event.isSet(), "Delete notice not received")
        self.ps.remove_handler("delete", delete_handler)
    
    
    def skiptest_10_job_notices(self):
        notices_received = [False]
        ids = [None, None]
        
        def publish_handler(feed, item, id):
            self.assertEqual(feed, "testjob")
            ids[-1] = id
            notices_received[-1] = "publish"

        def claimed_handler(feed, id):
            self.assertEqual(feed, "testjob")
            ids[-1] = id
            notices_received[-1] = "claimed"
        
        def cancelled_handler(feed, id):
            self.assertEqual(feed, "testjob")
            ids[-1] = id
            notices_received[-1] = "cancelled"

        def stalled_handler(feed, id):
            self.assertEqual(feed, "testjob")
            ids[-1] = id
            notices_received[-1] = "stalled"
        
        def retried_handler(feed, id):
            self.assertEqual(feed, "testjob")
            ids[-1] = id
            notices_received[-1] = "retried"
        
        def finished_handler(feed, id, result):
            self.assertEqual(feed, "testjob")
            ids[-1] = id
            notices_received[-1] = "finished"
        
        def do_wait():
            i = 0
            while not notices_received[-1] and i < 2:
                i += 1
                time.sleep(0.2)
        
        self.ps.register_handler('publish_notice', publish_handler)
        self.ps.register_handler('claimed_notice', claimed_handler)
        self.ps.register_handler('cancelled_notice', cancelled_handler)
        self.ps.register_handler('stalled_notice', stalled_handler)
        self.ps.register_handler('retried_notice', retried_handler)
        self.ps.register_handler('finished_notice', finished_handler)
        
        j = self.ps.job("testjob")
        self.assertEqual(j.__class__, Job)
        self.assertFalse(notices_received[0])
        
        # create the job
        ids[0] = j.put('b')
        do_wait()
        self.assertEqual(notices_received[0], "publish", "Notice not received")
        self.assertEqual(ids[0], ids[-1])
        
        notices_received.append(False); ids.append(None);
        # claim the job
        id, job, cancelled = j.get()
        self.assertEqual(job, 'b')
        self.assertEqual(cancelled, 0)
        self.assertEqual(ids[0], id)
        do_wait()
        self.assertEqual(notices_received[-1], "claimed", "Claimed notice not received")
        self.assertEqual(ids[0], ids[-1])
        
        notices_received.append(False); ids.append(None);
        # cancel the job
        j.cancel(id)
        do_wait()
        self.assertEqual(notices_received[-1], "cancelled", "Cancelled notice not received")
        self.assertEqual(ids[0], ids[-1])
        
        notices_received.append(False); ids.append(None);
        # get the job again
        id, job, cancelled = j.get()
        self.assertEqual(job, 'b')
        self.assertEqual(cancelled, 1)
        self.assertEqual(ids[0], id)
        do_wait()
        self.assertEqual(notices_received[-1], "claimed", "Claimed notice not received")
        self.assertEqual(ids[0], ids[-1])
        
        notices_received.append(False); ids.append(None);
        # stall the job
        j.stall(id)
        do_wait()
        self.assertEqual(notices_received[-1], "stalled", "Stalled notice not received")
        self.assertEqual(ids[0], ids[-1])
        
        notices_received.append(False); ids.append(None);
        # retry the job
        j.retry(id)
        do_wait()
        self.assertEqual(notices_received[-1], "retried", "Retried notice not received")
        self.assertEqual(ids[0], ids[-1])
        
        notices_received.append(False); ids.append(None);
        # get the job again
        id, job, cancelled = j.get()
        self.assertEqual(job, 'b')
        self.assertEqual(cancelled, 0)
        self.assertEqual(ids[0], id)
        do_wait()
        self.assertEqual(notices_received[-1], "claimed", "Claimed notice not received")
        self.assertEqual(ids[0], ids[-1])
        
        notices_received.append(False); ids.append(None);
        # finish the job
        j.finish(id)
        do_wait()
        self.assertEqual(notices_received[-1], "finished", "Finished notice not received")
        self.assertEqual(ids[0], ids[-1])

        self.ps.remove_handler('publish_notice', publish_handler)
        self.ps.remove_handler('claimed_notice', claimed_handler)
        self.ps.remove_handler('cancelled_notice', cancelled_handler)
        self.ps.remove_handler('stalled_notice', stalled_handler)
        self.ps.remove_handler('retried_notice', retried_handler)
        self.ps.remove_handler('finished_notice', finished_handler)
        
suite = unittest.TestLoader().loadTestsFromTestCase(TestNotice)
