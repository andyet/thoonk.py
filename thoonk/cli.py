import Queue
from pubsub import ACL, Interface
import threading
import traceback
import sys
from exceptions import FeedExists

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
            self.pubsub.create_feed(args[0], {})
        except FeedExists:
            print "Feed already exists"

    def cmd_publish(self, *args):
        self.pubsub[args[0]].publish(" ".join(args[1:]))

    def cmd_delete(self, *args):
        self.pubsub[args[0]].delete_feed()

    def cmd_retract(self, *args):
        self.pubsub[args[0]].retract(args[1])

    def cmd_feeds(self, *args):
        print self.pubsub.get_feeds()

    def cmd_items(self, *args):
        print self.pubsub[args[0]].get_items()

    def cmd_item(self, *args):
        if len(args) == 1:
            args.append(None)
        print self.pubsub[args[0]].get_item(args[1])

    def cmd_retract(self, *args):
        print self.pubsub[args[0]].retract(args[1])

    def cmd_getconfig(self, *args):
        print self.pubsub[args[0]].config

    def cmd_setconfig(self, *args):
        feed = args[0]
        config = ' '.join(args[1:])
        self.pubsub[feed].config = config
        print "Ok."

    def publish_notice(self, feed, item, id):
        print "publish: %s[%s]: %s" % (feed, id, item)

    def retract_notice(self, feed, id):
        print "retract: %s[%s]" % (feed, id)

    def create_notice(self, feed):
        print "created: %s" % feed

    def delete_notice(self, feed):
        print "deleted: %s" % feed

    def finish_notice(self, feed, id, item, result):
        print "finished: %s[%s]: %s -> %s" % (feed, id, item, result)


