import collections
from random import choice,shuffle

Card=collections.namedtuple('Card',['rank','suit'])

class Deck:
    ranks=[str(r) for r in range(2,11)]+list('JQKA')
    suits='♠ ♦ ♣ ♥'.split()

    def __init__(self):
        self._cards=[Card(rank,suit) for suit in self.suits for rank in self.ranks]

    def __len__(self):
        return len(self._cards)
    
    def __getitem__(self,position):
        return self._cards[position]

    def __setitem__(self,position,value):
        self._cards[position]=value

deck=Deck()

print(deck[1])
shuffle(deck)
print(deck[1])

