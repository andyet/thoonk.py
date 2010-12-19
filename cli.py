from sleekpubsub.pubsub import Pubsub
from sleekpubsub.cli import CLInterface

p = Pubsub()
i = CLInterface()
p.register_interface(i)
