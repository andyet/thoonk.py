import glob
import os
import redis
import threading
import uuid


class EventEmitter(object):

    def __init__(self):
        """
        Initializes the EE.
        """
        self._events = { 'new_listener': [] }

    def on(self, event, f=None):
        """Return a function that takes an event listener callback."""

        def _on(f):
            # Creating a new event array if necessary
            try:
                self._events[event]
            except KeyError:
                self._events[event] = []

            #fire 'new_listener' *before* adding the new listener!
            self.emit('new_listener')

            # Add the necessary function
            self._events[event].append(f)

        if (f==None):
            return _on
        else:
            return _on(f)

    def emit(self, event, *args, **kwargs):
        """Emit `event`, passing the arguments to each attached function."""

        # Pass the args to each function in the events dict
        for fxn in self._events.get(event, []):
            fxn(*args, **kwargs)

    def once(self, event, f=None):
        def _once(f):
            def g(*args, **kwargs):
                f(*args, **kwargs)
                self.remove_listener(event, g)
            return g

        if (f==None):
            return lambda f: self.on(event, _once(f))
        else:
            self.on(event, _once(f))

    def remove_listener(self, event, function):
        """Remove the function attached to `event`."""
        self._events[event].remove(function)

    def remove_all_listeners(self, event):
        """Remove all listeners attached to `event`."""
        self._events[event] = []

    def listeners(self, event):
        return self._events[event]


class ThoonkObject(EventEmitter):

    TYPE = ''
    SCRIPT_DIR = ''
    SUBSCRIBABLES = []

    def __init__(self, name, thoonk, subscribe=True):
        super(ThoonkObject, self).__init__()
        self.name = name
        self.thoonk = thoonk
        self.subscription_inited = False
        if subscribe:
            self.init_subscribe()

    def _build_event(self, event_type):
        return 'event.%s.%s:%s' % (self.TYPE, event_type, self.name)

    def handle_event(self, channel, msg):
        pass

    def init_subscribe(self):
        if self.name not in self.thoonk.subscriptions:
            self.thoonk.subscriptions[self.name] = self.SUBSCRIBABLES
            if not self.subscription_inited:
                for subscribable in self.SUBSCRIBABLES:
                    event_name = self._build_event(subscribable)
                    self.thoonk.on(event_name, self.handle_event)

            self.subscription_inited = True

    def run_script(self, name, args):
        return self.thoonk._run_script(self.TYPE, name, self.name, args)


class Thoonk(EventEmitter):

    def __init__(self, host=None, port=None, db=None, listen=None):
        super(Thoonk, self).__init__()
        self.scripts = {}
        self.objects = {}
        self.shas = {}
        self.instance = uuid.uuid4()
        self.subscriptions = {}
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

    def register_type(self, obj):
        self.objects[obj.TYPE] = obj
        self.scripts[obj.TYPE] = {}

        for filename in glob.glob(os.path.join(obj.SCRIPT_DIR, '*.lua')):
            basename = os.path.basename(filename)
            if basename.endswith('.lua'):
                verb = basename[0:-4]
                script = open(filename).read()
                self.scripts[obj.TYPE][verb] = script
                sha = self.redis.execute_command('SCRIPT', 'LOAD', script)
                if obj.TYPE not in self.shas:
                    self.shas[obj.TYPE] = {}
                self.shas[obj.TYPE][verb] = sha
                self.emit('loaded.%s' % obj.TYPE)

    def create(self, obj_type, name):
        obj_class = self.objects.get(obj_type, None)
        if obj_class is None:
            raise ValueError('Unknown thoonk object type: %s' % obj_type)
        return obj_classs(name, self)

    def _run_script(self, obj_type, script, feed, args):
        eargs = ['0', feed]
        eargs.extend(args)
        sha = self.shas[obj_type][script]
        return self.redis.execute_command('EVALSHA', sha, *eargs)

    def _listen(self):
        #r = self.lredis.parse_response()

        for event in self.lredis.listen():
            print event
            self.emit(event['channel'], event['channel'], event['data'])
