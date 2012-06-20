import uuid

from thoonk import ThoonkObject


class Deck(ThoonkObject):

    TYPE = 'deck'
    SCRIPT_DIR = './scripts/deck/'
    SUBSCRIBABLES = ['publish', 'edit', 'retract']

    def __init__(self, name, thoonk):
        super(Deck, self).__init__(name, thoonk)
