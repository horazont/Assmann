#!/usr/bin/python3

import bisect
import random
import itertools
import collections
import copy

import numpy as np

from Graph import DirectedWeightedGraph

def weighted_choice(choices, weights):
    cum = np.add.accumulate(weights)
    rand = random.random() * cum[-1]
    return choices[bisect.bisect(cum, rand)]


class CharacterSet(list):
    def __pow__(self, other):
        return type(self)(itertools.product(self, repeat=other))


class MarkovChain:
    """A Markov source of order n.
    """

    def __init__(self, order, debug=False):
        self.order = int(order)
        self.time = 0
        self.states = DirectedWeightedGraph()
        self.state = ()
        self.learn_state = ()

    def set_random_state(self):
        self.state = random.choice(list(self.states.V))

    def next_state(self):
        self.time += 1
        cands = list(self.states.get_edges_at(self.state))
        if len(cands) > 0:
            edge = weighted_choice(cands, [c[1] for c in cands])
            self.state, _ = edge
        else:
            self.state = None

    def emit(self):
        while True:
            self.next_state()

            if self.state is None:
                raise StopIteration

            yield self.state[-1]

    def add_transition(self, src, dst):
        """Add a state transition into the state graph.

        This will add edges with weight 1 or increase the weight if the edge
        already exists.
        """
        self.states.add_vertex(src)
        self.states.add_vertex(dst)
        self.states.add_edge(src, dst, 1)

    def learn(self, source):
        """Build a markov model from an iterable input source.
        """
        state = collections.deque(self.learn_state)
        for i in source:
            oldstate = tuple(state)

            if len(oldstate) == self.order:
                state.popleft()

            state.append(i)

            # FIXME how to handle the start case?
            self.add_transition(oldstate, tuple(state))

        self.learn_state = tuple(state)

