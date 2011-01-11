import sleekpubsub
import unittest
import math

class TestJob(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.ps = sleekpubsub.Pubsub(db=10, listen=True)
        self.ps.redis.flushdb()
    #def setUp(self):
    #    self.ps = sleekpubsub.Pubsub(listen=True, db=10)

    def test_10_basic_job(self):
        """JOB publish, retrieve, finish, get result"""
        j = self.ps.job("testjob")
        #publisher
        id, q = j.put(9.0)
        #worker
        id, query = j.get()
        result1 = math.sqrt(float(query))
        j.finish(id, result1)
        #publisher gets result
        query2, result2 = q.get()
        self.assertEqual(float(result1), float(result2), "Job results did not match publish.")
        self.assertEqual(j.get_items(), [])

    def test_20_cancel_job(self):
        j = self.ps.job("testjob")
        #publisher
        id, q = j.put(9.0)
        #worker claims
        id, query = j.get()
        #publisher cancels
        j.cancel(id)
        id2, query2 = j.get()
        self.assertEqual(id, id2)
        j.cancel(id)
        j.retract(id)


suite = unittest.TestLoader().loadTestsFromTestCase(TestJob)
            
