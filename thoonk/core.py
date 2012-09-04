import glob
import os
import redis
import threading
import uuid

from pkg_resources import resource_listdir, resource_string


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
    PACKAGE = 'thoonk'
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
        result = self.thoonk._run_script(self.TYPE, name, self.name, args)
        if result[0]:
            raise RuntimeError(result[0])
        if len(result) == 2:
            return result[1]
        return result[1:]

    @classmethod
    def get_scripts(self):
        scripts = {}
        for name in resource_listdir(self.PACKAGE, self.SCRIPT_DIR):
            if name.endswith('.lua'):
                script = resource_string(self.PACKAGE, '%s/%s' % (self.SCRIPT_DIR, name))
                if script:
                    scripts[name[0:-4]] = script
        return scripts


class Thoonk(EventEmitter):

    def __init__(self, host='localhost', port=6379, db=0):
        super(Thoonk, self).__init__()
        self.scripts = {}
        self.objects = {}
        self.shas = {}
        self.instance = uuid.uuid4()
        self.subscriptions = {}
        self.redis = redis.StrictRedis(host=host, port=port, db=db)
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
        self.scripts[obj.TYPE] = obj.get_scripts()
        self.shas[obj.TYPE] = {}

        for name, script in self.scripts[obj.TYPE].items():
            sha = self.redis.execute_command('SCRIPT', 'LOAD', script)
            self.shas[obj.TYPE][name] = sha

        self.emit('loaded.%s' % obj.TYPE)

    def create(self, obj_type, name):
        obj_class = self.objects.get(obj_type, None)
        if obj_class is None:
            raise ValueError('Unknown thoonk object type: %s' % obj_type)
        return obj_class(name, self)

    def _run_script(self, obj_type, script, feed, args):
        eargs = ['0', feed]
        eargs.extend(args)
        sha = self.shas[obj_type][script]
        return self.redis.execute_command('EVALSHA', sha, *eargs)

    def _listen(self):
        try:
            for event in self.lredis.listen():
                self.emit(event['channel'], event['channel'], event['data'])
        except AttributeError:
            # It's possible to get this exception after calling quit()
            pass
