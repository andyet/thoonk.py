from sleekpubsub.pubsub import Pubsub
from sleekpubsub.cli import CLInterface

p = Pubsub(listen=True, db=10)
i = CLInterface()
p.register_interface(i)
