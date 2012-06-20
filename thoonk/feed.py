import uuid

from thoonk import ThoonkObject


class Feed(ThoonkObject):

    TYPE = 'feed'
    SCRIPT_DIR = './scripts/feed/'
    SUBSCRIBABLES = ['publish', 'edit', 'retract']

    def __init__(self, name, thoonk):
        super(Feed, self).__init__(name, thoonk)

    def get_ids(self):
        pass

    def get_item(self, id=None):
        pass

    def get_all(self):
        items = self.run_script('getall', [self.name])
        result = {}
        for item in items:
            result[item[0]] = item[1]
        return result

    def publish(self, item, id=None):
        if not id:
            id = uuid.uuid4()
        return self.run_script('publish', [id, item, ''])

    def retract(self, id):
        pass

    def handle_event(self, channel, msg):
        obj = channel.split(':')
        etype = obj[0].split('.')
        name = etype[2]

        if name == 'publish':
            msg = msg.split('\x00')
            self.emit('publish', msg[0], msg[1])
            self.emit('publishid:%s' % msg[0], msg[0], msg[1])
