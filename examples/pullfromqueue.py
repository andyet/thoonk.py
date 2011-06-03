import thoonk
ps = thoonk.Pubsub()
q = ps.pyqueue('testpyqueue')
while True:
    value = q.get()
    print value
    print type(value), ":", value
ps.close()
