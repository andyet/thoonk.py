import uuid
import time
import json

from thoonk import ThoonkObject


class Job(ThoonkObject):

    TYPE = 'job'
    SCRIPT_DIR = './scripts/job/'
    SUBSCRIBABLES = ['finish']

    def __init__(self, name, thoonk):
        super(Job, self).__init__(name, thoonk)

    def cancel(self, id):
        return self.run_script('cancel', [self.name, id])

    def config(self, data):
        return self.run_script('config', [self.name, json.tostring(data)])

    def create(self, config):
        return self.run_script('create', [self.name, json.tostring(config)])

    def finish(self, id, result=None):
        return self.run_script('finish', [self.name, id, result])

    def get(self, timeout=0):
        id = self.thoonk.redis.brpop('job.ids:%s' % self.name, timeout)
        return self.run_script('get', [self.name, id, int(time.time() * 1000)])

    def publish(self, item, id=None, priority=False, finished=None):
        if not id:
            id = uuid.uuid()
        if finished is not None:
            self.on('finished:%s' % id, finished)
        return self.run_script('publish', [self.name, id, item, priority])

    def retract(self, id):
        return self.run_script('retract', [self.name, id])

    def retry(self, id):
        return self.run_script('retry', [self.name, id, int(time.time() * 1000)])

    def stall(self, id):
        return self.run_script('stall', [self.name, id])

    def handle_event(self, channel, msg):
        obj = channel.split(':')
        etype = obj[0].split('.')
        name = etype[2]

        if name == 'finish':
            msg = msg.split('\00')
            if len(msg) == 1:
                self.emit('finished', msg[0])
                self.emit('finished:%s' % msg[0])
            else:
                self.emit('finished', msg[0], result=msg[1])
                self.emit('finished:%s' % msg[0], result=msg[1])
