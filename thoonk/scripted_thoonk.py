import redis
import uuid
import os
import glob
import threading


from events import EventEmitter


class ThoonkObject(EventEmitter):

    OBJ_TYPE = ''
    SCRIPT_DIR = ''

    def __init__(self, name, thoonk, subscribe=True):
        super(ThoonkObject, self).__init__()
        self.name = name
        self.thoonk = thoonk
        self.subscribables = []
        self.subscription_inited = False
        if subscribe:
            self.init_subscribe()

    def _build_event(self, event_type):
        return 'event.%s.%s:%s' % (self.OBJ_TYPE, event_type, self.name)

    def handle_event(self, channel, msg):
        pass

    def init_subscribe(self):
        if self.name not in self.thoonk.subscriptions:
            self.thoonk.subscriptions[self.name] = self.subscribables
            if not self.subscription_inited:
                for subscribable in self.subscribables:
                    event_name = self._build_event(subscribable)
                    self.thoonk.on(event_name, self.handle_event)

            self.subscription_inited = True

    def run_script(self, name, args):
        return self.thoonk._run_script(self.OBJ_TYPE, name, self.name, args)


class Thoonk(EventEmitter):

    def __init__(self):
        super(Thoonk, self).__init__()
        self.scripts = {}
        self.shas = {}
        self.instance = uuid.uuid4()
        self.subscriptions = {}
        self.objects = {}
        self.redis = redis.StrictRedis()
        self.lredis = self.redis.pubsub()

        self.lredis.psubscribe('event.*')

        self._thread = threading.Thread(
                name='ThoonkListener', 
                target=self._listen)
        self._thread.daemon = True
        self._thread.start()

    def quit(self):
        self.redis.connection_pool.disconnect()
        self.lredis.connection_pool.disconnect()

    def register_type(self, name, obj):
        self.objects[name] = obj(name, self)
        self.scripts[obj.OBJ_TYPE] = {}

        for filename in glob.glob(os.path.join(obj.SCRIPT_DIR, '*.lua')):
            basename = os.path.basename(filename)
            if basename.endswith('.lua'):
                verb = basename[0:-4]
                script = open(filename).read()
                self.scripts[obj.OBJ_TYPE][verb] = script
                sha = self.redis.execute_command('SCRIPT', 'LOAD', script)
                if obj.OBJ_TYPE not in self.shas:
                    self.shas[obj.OBJ_TYPE] = {}
                self.shas[obj.OBJ_TYPE][verb] = sha
                self.emit('loaded.%s' % name)

    def _run_script(self, obj_type, script, feed, args):
        eargs = ['0', feed]
        eargs.extend(args)
        sha = self.shas[obj_type][script]
        return self.redis.execute_command('EVALSHA', sha, *eargs)

    def _listen(self):
        r = self.lredis.parse_response()

        for event in self.lredis.listen():
            print event['channel']
            self.emit(event['channel'], event['channel'], event['data'])


class Feed(ThoonkObject):
    OBJ_TYPE = 'feed'
    SCRIPT_DIR = './scripts/feed/'

    def __init__(self, name, thoonk):
        super(Feed, self).__init__(name, thoonk)
        self.subscribables = ['publish', 'edit', 'retract']

    def publish(self, item, id=None):
        if not id:
            id = uuid.uuid4()
        return self.run_script('publish', [id, item, ''])

    def get_all(self):
        items = self.run_script('getall', [self.name]) 
        result = {}
        for item in items:
            result[item[0]] = item[1]
        return result

    def handle_event(self, channel, msg):
        obj = channel.split(':')
        etype = obj[0].split('.')
        name = etype[2]

        if name == 'publish':
            msg = msg.split('\x00')
            self.emit('publish', msg[0], msg[1])
            self.emit('publishid:%s' % msg[0], msg[0], msg[1])
