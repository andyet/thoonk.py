import uuid
import json
import time

from thoonk import ThoonkObject


class Feed(ThoonkObject):

    TYPE = 'feed'
    PACKAGE = 'thoonk'
    SCRIPT_DIR = 'scripts/feed/'
    SUBSCRIBABLES = ['publish', 'edit', 'retract']

    def __init__(self, name, thoonk):
        super(Feed, self).__init__(name, thoonk)
        self.run_script('create', [json.dumps({})])

    def config(self, config):
        return self.run_script('config', [json.dumps(config)])

    def get_ids(self):
        return set(self.run_script('getids', []))

    def get_item(self, id=None):
        data = self.run_script('get', [id])
        if data is None:
            return None
        return json.loads(data)

    def get_all(self):
        result = self.run_script('getall', [])
        items = {}
        if result:
            data = json.loads(result)
            for item in data:
                items[item['id']] = json.loads(item['item'])
        return items

    def publish(self, item, id=None):
        if not id:
            id = uuid.uuid4()
        return self.run_script('publish', [id, json.dumps(item), time.time() * 1000])

    def retract(self, id):
        result = self.run_script('retract', [id])
        return True if result else False

    def length(self):
        return self.run_script('length', [])

    def has_id(self, id):
        result = self.run_script('hasid', [id])
        return True if result else False

    def handle_event(self, channel, msg):
        obj = channel.split(':')
        etype = obj[0].split('.')
        name = etype[2]

        if name == 'publish':
            msg = msg.split('\x00')
            data = json.loads(msg[1])
            self.emit('publish', msg[0], data)
            self.emit('publishid:%s' % msg[0], msg[0], data)
        elif name == 'edit':
            msg = msg.split('\x00')
            data = json.loads(msg[1])
            self.emit('edit', msg[0], data)
            self.emit('editid:%s' % msg[0], msg[0], data)
        elif name == 'retract':
            self.emit('retract', msg)
            self.emit('retract:%s' % msg, msg)
