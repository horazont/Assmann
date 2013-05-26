#!/usr/bin/python3

import MarkovChain

chain = MarkovChain.MarkovChain(2)
chain.learn('choochoooshoe')
print(chain.states)
print()
for v in chain.states.V:
    print(v, list(chain.states.get_edges_at(v)))
