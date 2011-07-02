import thoonk
import unittest
import math
from ConfigParser import ConfigParser


class TestJob(unittest.TestCase):

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


    def setUp(self):
        self.ps = thoonk.Pubsub(db=10, listen=True)
        self.ps.redis.flushdb()

    def tearDown(self):
        self.ps.close()

    def test_10_basic_job(self):
        """JOB publish, retrieve, finish, get result"""
        #publisher
        testjob = self.ps.job("testjob")
        id = testjob.put(9.0)

        #worker
        testjobworker = self.ps.job("testjob")
        id_worker, query_worker, cancelled = testjobworker.get(timeout=3)
        result_worker = math.sqrt(float(query_worker))
        testjobworker.finish(id_worker, result_worker, True)

        #publisher gets result
        query_publisher, result_publisher = testjob.get_result(id, 1)
        self.assertEqual(float(result_worker), float(result_publisher), "Job results did not match publish.")
        self.assertEqual(testjob.get_ids(), [])

    def test_20_cancel_job(self):
        """Test cancelling a job"""
        j = self.ps.job("testjob")
        #publisher
        id = j.put(9.0)
        #worker claims
        id, query, cancelled = j.get()
        self.assertEqual(cancelled, 0)
        #publisher or worker cancels
        j.cancel(id)
        id2, query2, cancelled2 = j.get()
        self.assertEqual(cancelled2, 1)
        self.assertEqual(id, id2)
        #cancel the work again
        j.cancel(id)
        # check the cancelled increment again
        id3, query3, cancelled3 = j.get()
        self.assertEqual(cancelled3, 2)
        self.assertEqual(id, id3)
        #cleanup -- remove the job from the queue
        j.retract(id)
        self.assertEqual(j.get_ids(), [])

    def test_30_no_job(self):
        j = self.ps.job("testjob")
        self.assertRaises(Exception, j.get, timeout=1)
        
suite = unittest.TestLoader().loadTestsFromTestCase(TestJob)

