#!/usr/bin/python3

import MarkovChain

chain = MarkovChain.MarkovChain(8)
chain.learn('choochoooshoe')
print("Done with learning.")
for i in range(20):
    try:
        print(next(chain.emit()), end='')
    except StopIteration:
        break

print()
