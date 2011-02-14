import sleekpubsub
ps = sleekpubsub.Pubsub()
q = ps.pyqueue('testpyqueue')
while True:
    value = q.get()
    print type(value), ":", value
ps.close()
