import sys
import redis
import thoonk
import unittest

import ConfigParser


class ThoonkTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ThoonkTest, self).__init__(*args, **kwargs)
        self.config = ConfigParser.SafeConfigParser()
        self.config.read('test.cfg')

        try:
            lock = self.config.getboolean('Test', 'lock') == True
        except ConfigParser.NoOptionError:
            lock = False
            pass

        if lock:
            print('WARNING: Tests disabled. Remove the lock=True option' + \
                  ' from test.cfg to enable tests')
            sys.exit()
            return

        self.reset_redis()

    def setUp(self):
        self.reset_redis()
        self.start_thoonk()

    def tearDown(self):
        self.stop_thoonk()
        self.reset_redis()

    def reset_redis(self):
        host = self.config.get('Test', 'host')
        port = self.config.getint('Test', 'port')
        db = self.config.getint('Test', 'db')

        client = redis.Redis(host=host, port=port, db=db)
        client.flushdb()
        client.connection_pool.disconnect()

    def start_thoonk(self):
        host = self.config.get('Test', 'host')
        port = self.config.getint('Test', 'port')
        db = self.config.getint('Test', 'db')

        self.thoonk = thoonk.Thoonk(host=host, port=port, db=db)

    def stop_thoonk(self):
        self.thoonk.quit()
