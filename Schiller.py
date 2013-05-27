#!/usr/bin/python3

import sys
import pickle

import MarkovChain

print("Ich bin nicht Schiller!")

infile = sys.argv[1]
order = sys.argv[2]
chars = sys.argv[3]
dumpname = '{}-{}.chain'.format(infile,order)

try:
    with open(dumpname, 'rb') as f:
        chain = pickle.load(f)
        print("Reading from {}".format(dumpname))
except OSError:
    def source():
        with open(infile) as f:
            for line in f:
                yield from line

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
