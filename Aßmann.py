#!/usr/bin/python3

import re
import sys
import pickle

import MarkovChain

pattern = """(\w+|\s+|,|\.|\?|!|"|'|\[|\]|\(|\)|\n)"""

infile = sys.argv[1]
order = sys.argv[2]
chars = sys.argv[3]
dumpname = '{}-{}-assmann.chain'.format(infile,order)

try:
    with open(dumpname, 'rb') as f:
        chain = pickle.load(f)
        print("Reading from {}".format(dumpname))
except OSError:
    def source():
        with open(infile) as f:
            yield from (m.group(0) for m in re.finditer(pattern, f.read()))

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
