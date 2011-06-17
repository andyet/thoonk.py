"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import Queue
import cmd
import threading
import traceback
import sys

import thoonk
from thoonk import Thoonk
from thoonk.exceptions import FeedExists


class CLInterface(cmd.Cmd):

    def __init__(self, host='localhost', port=6379, db=0):
        cmd.Cmd.__init__(self)
        self.thoonk = Thoonk(host, port, db)
        self.lthoonk = Thoonk(host, port, db)
        self.intro = 'Thoonk.py v%s Client' % thoonk.__version__
        self.prompt = '>>> '
        self.lthoonk.register_handler('publish_notice', self.publish_notice)
        self.lthoonk.register_handler('retract_notice', self.retract_notice)
        self.lthoonk.register_handler('create_notice', self.create_notice)
        self.lthoonk.register_handler('delete_notice', self.delete_notice)

    def start(self):
        self.thread = threading.Thread(target=self.lthoonk.listen)
        self.thread.daemon = True
        self.thread.start()
        self.lthoonk.listen_ready.wait()
        self.cmdloop()

    def parseline(self, line):
        line = cmd.Cmd.parseline(self, line)
        if line[0] != 'help':
            return (line[0], line[1].split(' '), line[2])
        return line

    def do_EOF(self, line):
        return True

    def do_quit(self, line):
        return True

    def help_quit(self):
        print 'Quit'

    def do_create(self, args):
        try:
            self.thoonk.create_feed(args[0], {})
        except FeedExists:
            print "Feed already exists"

    def help_create(self):
        print 'create [feed name]'
        print 'Create a new feed with the given name'

    def do_publish(self, args):
        self.thoonk[args[0]].publish(" ".join(args[1:]))

    def help_publish(self):
        print 'publish [item contents]'
        print 'Publish a string to the feed (may include spaces)'

    def do_delete(self, args):
        self.thoonk[args[0]].delete_feed()

    def help_delete(self):
        print 'delete [feed name]'
        print 'Delete the given feed'

    def do_retract(self, args):
        self.thoonk[args[0]].retract(args[1])

    def help_retract(self):
        print 'retract [feed name] [item id]'
        print 'Remove an item from a feed.'

    def do_feeds(self, args):
        print self.thoonk.get_feeds()

    def help_feeds(self):
        print 'List existing feeds'

    def do_items(self, args):
        print self.thoonk[args[0]].get_all()

    def help_items(self):
        print 'items [feed name]'
        print 'List items in a given feed.'

    def do_item(self, args):
        if len(args) == 1:
            args.append(None)
        print self.thoonk[args[0]].get_item(args[1])

    def help_item(self):
        print 'item [feed name] [id]'
        print 'Print the contents of a feed item'

    def do_getconfig(self, args):
        print self.thoonk[args[0]].config

    def help_getconfig(self):
        print 'getconfig [feed name]'
        print 'Show the JSON configuration for a feed'

    def do_setconfig(self, args):
        feed = args[0]
        config = ' '.join(args[1:])
        self.thoonk[feed].config = config
        print "Ok."

    def help_setconfig(self):
        print 'setconfig [feed name] [config]'
        print 'Set the configuration for a feed'

    def publish_notice(self, feed, item, id):
        print "\npublish: %s[%s]: %s" % (feed, id, item)

    def retract_notice(self, feed, id):
        print "\nretract: %s[%s]" % (feed, id)

    def create_notice(self, feed):
        print "\ncreated: %s" % feed

    def delete_notice(self, feed):
        print "\ndeleted: %s" % feed

    def finish_notice(self, feed, id, item, result):
        print "\nfinished: %s[%s]: %s -> %s" % (feed, id, item, result)


if __name__ == '__main__':
    CLInterface().start()
