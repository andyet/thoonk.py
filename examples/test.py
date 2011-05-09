from thoonk import Pubsub

p = Pubsub()
n = p.feed("test")
id = n.publish("hayyyyyy", id='crap')
print id, n.get_item(id)
q = p.queue("queue_test")
q.publish("whatever")
q.publish("shit ")
q.publish("whatever")
q.publish("whatever")
q.publish("whatever")
while True:
    try:
        print q.get(timeout=1)
    except q.Empty:
        break
print "done"

