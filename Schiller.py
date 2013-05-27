#!/usr/bin/python3

import sys
import pickle

import MarkovChain

print("Ich bin nicht Schiller!")

order = sys.argv[1]
chars = sys.argv[2]
dumpname = 'schiller-{}.chain'.format(order)

try:
    with open(dumpname, 'rb') as f:
        chain = pickle.load(f)
        print("Reading from {}".format(dumpname))
except OSError:
    def source():
        with open('1._Akt') as f:
            for line in f:
                for char in line:
                    yield char

    print("Learning.")
    chain = MarkovChain.MarkovChain(order)
    chain.learn(source())
    print("Done learning!")
    with open(dumpname, 'wb') as f:
        pickle.dump(chain, f)

for i in range(int(chars)):
    try:
        print(next(chain.emit()), end='')
    except StopIteration:
        break

print()
