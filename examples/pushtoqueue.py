import sleekpubsub
ps = sleekpubsub.Pubsub()
q = ps.pyqueue('testpyqueue')

q.put("this is a string")
q.put(set(("this", "is", "a", "set")))
q.put(3.14)
q.put(4)
q.put(u'unicode string')

ps.close()
