#!/usr/bin/python3

import bisect
import random
import itertools
import collections
import numpy as np

def weighted_choice(choices, weights):
    cum = np.add.accumulate(weights)
    rand = random.random() * cum[-1]
    return choices[bisect.bisect(cum, rand)]

class MarkovSource:
    """A Markov source of order n.
    """

    def __init__(self, xs, init_state, ps, order):
        self.order = order
        self.time = 0;
        self.xs = xs
        if not isinstance(ps, np.ndarray):
            raise TypeError("ps Argument must be an instance of numpy.ndarray")

        self.init_state = init_state
        self.state = init_state.copy()
        self.ps = ps
        self.last = collections.deque(['' for i in range(order)])

    def emit(self):
        if self.time == 0:
            state = self.init_state
        else:
            state = np.dot(self.state, self.ps)
        
        out = weighted_choice(self.xs, state)

        self.last.pop()
        self.last.append(out)
        self.last.rotate(1)

        print('t = ', self.time, ' state:', state)
        self.state = state
        self.time += 1
        
        yield out
