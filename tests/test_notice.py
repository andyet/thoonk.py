import thoonk
import unittest
import time
from ConfigParser import ConfigParser

class TestNotice(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        conf = ConfigParser()
        conf.read('test.cfg')
        if conf.sections() == ['Test']:
            self.ps = thoonk.Thoonk(host=conf.get('Test', 'host'),
                                    port=conf.getint('Test', 'port'),
                                    db=conf.getint('Test', 'db'),
                                    listen=True)
            self.ps.redis.flushdb()
        else:
            print 'No test configuration found in test.cfg'
            exit()


    def tearDown(self):
        self.ps.close()

    "claimed, cancelled, stalled, finished"

    def test_05_publish_notice(self):
        notice_received = [False]
        ids = [None, None]
        
        def received_handler(feed, item, id):
            self.assertEqual(feed, "testfeed")
            ids[1] = id
            notice_received[0] = True
        
        self.ps.register_handler('publish_notice', received_handler)
        
        j = self.ps.feed("testfeed")
        
        self.assertFalse(notice_received[0])
        
        #publisher
        ids[0] = j.publish('a')
        
        # block while waiting for notice
        i = 0
        while not notice_received[0] and i < 3:
            i += 1
            time.sleep(1)

        self.assertEqual(ids[0], ids[1])
        
        self.assertTrue(notice_received[0], "Notice not received")
        
        self.ps.remove_handler('publish_notice', received_handler)

    def test_10_job_notices(self):
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
                time.sleep(1)
        
        self.ps.register_handler('publish_notice', publish_handler)
        self.ps.register_handler('claimed_notice', claimed_handler)
        self.ps.register_handler('cancelled_notice', cancelled_handler)
        self.ps.register_handler('stalled_notice', stalled_handler)
        self.ps.register_handler('retried_notice', retried_handler)
        self.ps.register_handler('finished_notice', finished_handler)
        
        j = self.ps.job("testjob")
        
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
