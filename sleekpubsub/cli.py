import Queue
from pubsub import ACL, Interface, NodeExists
import threading
import traceback
import sys

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
                elif cmd.strip() == "":
                    continue
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

    def cmd_nodes(self, *args):
        print self.pubsub.get_nodes()

    def cmd_items(self, *args):
        print self.pubsub.get_items(args[0])

    def cmd_item(self, *args):
        if len(args) == 1:
            args.append(None)
        print self.pubsub.get_item(args[0], args[1])

    def cmd_retract(self, *args):
        print self.pubsub.retract(args[0], args[1])

    def cmd_getconfig(self, *args):
        print self.pubsub.nodeconfig[args[0]]

    def cmd_setconfig(self, *args):
        node = args[0]
        config = ' '.join(args[1:])
        self.pubsub.nodeconfig[node] = config
        print "Ok."

    def publish_notice(self, node, item, id):
        print "publish: %s[%s]: %s" % (node, id, item)

    def retract_notice(self, node, id):
        print "retract: %s[%s]" % (node, id)

    def create_notice(self, node):
        print "created: %s" % node

    def delete_notice(self, node):
        print "deleted: %s" % node

