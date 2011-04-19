from thoonk.pubsub import Pubsub
from thoonk.cli import CLInterface

p = Pubsub(listen=True, db=10)
i = CLInterface()
p.register_interface(i)
