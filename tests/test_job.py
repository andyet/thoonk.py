import sleekpubsub
import unittest
import math

class TestJob(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

    def setUp(self):
        self.ps = sleekpubsub.Pubsub(db=10, listen=True)
        self.ps.redis.flushdb()

    def tearDown(self):
        self.ps.close()

    def test_10_basic_job(self):
        """JOB publish, retrieve, finish, get result"""
        #publisher
        testjob = self.ps.job("testjob")
        id, job_response = testjob.put(9.0)
        
        #worker
        testjobworker = self.ps.job("testjob")
        id_worker, query_worker = testjobworker.get()
        result_worker = math.sqrt(float(query_worker))
        testjobworker.finish(id_worker, result_worker)
        
        #publisher gets result
        query_publisher, result_publisher = job_response.get()
        self.assertEqual(float(result_worker), float(result_publisher), "Job results did not match publish.")
        self.assertEqual(testjob.get_items(), [])

    def test_20_cancel_job(self):
        j = self.ps.job("testjob")
        #publisher
        id, q = j.put(9.0)
        #worker claims
        id, query = j.get()
        #publisher or worker cancels
        j.cancel(id)
        id2, query2 = j.get()
        self.assertEqual(id, id2)
        #cancel the work again
        j.cancel(id)
        #cleanup -- remove the job from the queue
        j.retract(id)
        #assert empty
        self.assertEqual(j.get_items(), [])


suite = unittest.TestLoader().loadTestsFromTestCase(TestJob)
            
