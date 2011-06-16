# Thoonk #

Thoonk is a persistent (and fast!) system for push feeds, queues, and jobs which
leverages Redis. Thoonk.py is the Python implementation of Thoonk, and is
interoperable with other versions of Thoonk (currently Thoonk.js for node.js).

# Feed Types #

## Feed ##

The core of Thoonk is the feed. A feed is a subject that you can publish items
to (string, binary, json, xml, whatever), each with a unique id (assigned or
generated). Other apps and services may subscribe to your feeds and recieve
new/update/retract notices on your feeds. Each feed persists published items
that can later be queried. Feeds may also be configured for various behaviors,
such as max number of items, default serializer, friendly title, etc.

Feeds are useful for clustering applications, delivering data from different
sources to the end user, bridged peering APIs (pubsub hubbub, XMPP Pubsub,
maintaining ATOM and RSS files, etc), persisting, application state,
passing messages between users, taking input from and serving multiple APIs
simultaneously, and generally persisting and pushing data around.

## Queue ##

Queues are stored and interacted with in similar ways to feeds, except instead
of publishes being broadcast, clients may do a "blocking get" to claim an item,
ensuring that they're the only one to get it. When an item is delivered, it is
deleted from the queue.

Queues are useful for direct message passing.

## Sorted Feed ##

Sorted feeds are unbounded, manually ordered collections of items. Sorted feeds
behave similarly to plain feeds except that items may be edited in place or
inserted in arbitrary order.

## Job ##

Jobs are like Queues in that one client claims an item, but that client is also
required to report that the item is finished or cancel execution. Failure to to
finish the job in a configured amount of time or canceling the job results in
the item being reintroduced to the available list. Unlike queues, job items are
not deleted until they are finished.

Jobs are useful for distributing load, ensuring a task is completed regardless
of outages, and keeping long running tasks away from synchronous interfaces.

# Installation #

In theory, if I publish the package correctly, then this should work.

    pip install thoonk

## Requirements ##

Thoonk requires the redis-py package. I strongly recommend installing
the hiredis package as well, since it should significantly increase performance.

Since the redis package is undergoing refactoring with backwards incompatible
changes, be sure to use version 2.2.4.

    pip install redis=2.2.4
    pip install hiredis

## Running the Tests ##

After checking out a copy of the source code, you can run

    python testall.py

from the main directory.

# Using Thoonk #

## Initializing ##

    import thoonk
    pubsub = thoonk.Thoonk(host, port, db)

## Creating a Feed ##

    thoonk.create_feed(feed_name, {"max_length": 50})

OR create an object referencing the feed, creating it if it doesn't exist,
reconfiguring it if you specify a configuration.

    test_feed = thoonk.feed(feed_name)

The same is true for queues, jobs, and lists:
    
    test_queue = thoonk.queue('queue name')
    test_job = thoonk.job('job channel name')
    test_list = thoonk.list('list name')

## Configuring a Feed ##

    thoonk.set_config(feed_name, json_config)

### Supported Configuration Options ###

* type: feed/queue/job
* max\_length: maximum number of items to keep in a feed

## Using a Feed ##

    feed = thoonk.feed('test_feed')

### Publishing to a Feed ###

Publishing to a feed adds an item to the end of the feed, sorted by publish time.

    feed.publish('item contents', id='optional id')

Editing an existing item in the feed can be done by publishing with the same ID as
the item to replace. The edited version will be moved to the end of the feed.

Since IDs are unique within the feed, it is possible to use a feed as a basic set
structure by only working the IDs and not with the item contents.

### Retracting an Item ###

Removing an item is done through retraction, which simply requires the ID of the
item to remove.

    feed.retract('item id')

### Retrieving a List of Item IDs ###

Retrieving all of the IDs in the feed provides the order in which items appear.
   
    item_ids = feed.get_ids()

### Retrieve a Dictionary of All Items ###

Retrieving a dictionary of all items, keyed by item ID is doable using:

    items = feed.get_all()

#### Iterating over items in published order ####

    items = feed.get_all()
    for id in feed.get_ids():
        do_stuff_with(items[id])

### Retrieving a Specific Item ###

A single item may be retrieved from the feed if its ID is known.

    item = feed.get_item('item id')

## Using a Queue ##

    queue = thoonk.queue('queue_feed')

### Publishing To a Queue ###

    queue.put('item')
    queue.put('priority item', priority=queue.HIGH)

### Popping a Queue ###

    item = queue.get()
    timed_item = queue.get(timeout=5)

## Using a Job Feed ##

A job feed is a queue of individual jobs; there is no inherent relationship between jobs from the
same job feed.

    job = thoonk.job('job_feed')

### Publishing a Job ###

Creating new jobs is done by putting them in the job queue. New items are placed at the end of the
queue, but it is possible to insert high priority jobs at the front.

    job.put('job contents')
    job.put('priority job', priority=job.HIGH)

### Claiming a Job ###

Workers may pull jobs from the queue by claiming them using the `get()` method. The default behaviour
is to block indefinitely while waiting for a job, but a timeout value in seconds may be supplied
instead.

    data = job.get()
    timed_data = job.get(timeout=5)

### Cancelling a Job Claim ###

Cancelling a job is done by a worker who has claimed the job. Cancellation relinquishes the claim to
the job and puts it back in the queue to be given to another worker.

    job.cancel('job id')

### Stalling a Job ###

Stalling a job removes it from the queue to prevent it from executing, but does not completely
delete it. A stalled job is effectively paused, waiting for whatever issue that required the
stall to be resolved.

    job.stall('job id')

### Retrying a Stalled Job ###

Once a job jas been stalled, it can be retried once the issue requiring stalling has been
resolved.

    job.retry('job id')

### Retracting a Job ###

Retracting a job completely removes it from the queue, preventing it from being executed.

    job.retract('job id')

### Finishing a Job ###

Finishing a job can be done in three ways. The first is as a simple acknowledgment that the task
has been completed.

    job.finish('job id')

The second is when there is result data that should be returned to the job owner. The `result=True`
parameter is used since it is possible for `None` to be an actual job result.

    job.finish('job id', 'result contents', result=True)

It may also be desired to only keep job results around for a short period of time. In which case,
a timeout parameter may be added.

    job.finish('job id', 'result contents', result=True, timeout=5)

### Check Job Results ###

Checking the result of a job can require knowing what the original job request actually was.
Thus, the `get_result` method will return both values.

    query, result = job.get_result('job id', timeout=5)

# The Future of Thoonk #

# Writing Your Own Implementation or Peer #
