import Queue
from pubsub import ACL, Interface, NodeExists
import threading
import traceback

class CLInterface(Interface):
    name = "CLI"

    def start(self):
        self.acl = ACL()
        self.thread = threading.Thread(target=self.listen)
        #self.thread.setDaemon()
        self.thread.start()

    def listen(self):
        while True:
            try:
                cmd = raw_input(">")
                if cmd == 'quit':
                    break
                if len(cmd.split()) > 1:
                    args = cmd.split()[1:]
                else:
                    args = []
                cmd = cmd.split()[0]
                if hasattr(self, "cmd_%s" % cmd):
                    getattr(self, "cmd_%s" % cmd)(*args)
                else:
                    print "Command %s not found." % cmd
            except EOFError:
               break
            except:
                traceback.print_exc()

    def cmd_create(self, *args):
        try:
            self.pubsub.create_node(args[0], {})
        except NodeExists:
            print "Node already exists"

    def cmd_publish(self, *args):
        self.pubsub.publish(args[0], " ".join(args[1:]))

    def cmd_delete(self, *args):
        self.pubsub.delete_node(args[0])

    def cmd_retract(self, *args):
        self.pubsub.retract(*args)

    def publish_notice(self, node, item, id):
        print "publish: %s[%s]: %s" % (node, id, item)

    def retract_notice(self, node, id):
        print "retract: %s[%s]" % (node, id)


