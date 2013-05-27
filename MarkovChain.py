#!/usr/bin/python3

import bisect
import random
import itertools
import collections
import copy

import numpy as np

from Graph import DirectedWeightedGraph
from Graph import DirectedWeightedEdge

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
        self.order = order
        self.time = 0
        self.states = DirectedWeightedGraph()
        self.state = ()
        self.learn_state = ()

    def set_random_state(self):
        self.state = random.choice(self.states.V)

    def next_state(self):
        self.time += 1
        cands = list(self.states.get_edges_at(self.state))
        if len(cands) > 0:
            self.state = weighted_choice(cands, [c.weight for c in cands]).dst
        else:
            self.state = None

    def emit(self):
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

        existing = self.states.find_edge(src, dst)
        if existing is None:
            self.states.add_edge(DirectedWeightedEdge(src, dst, 1))
        else:
            existing.weight += 1
        
    def learn(self, source):
        """Build a markov model from an iterable input source.
        """
        state = collections.deque(self.learn_state)
        for i in source:
            newstate = copy.deepcopy(state)

            if len(state) == self.order:
                newstate.popleft()

            newstate.append(i)

            # FIXME how to handle the start case?
            self.add_transition(tuple(state), tuple(newstate))
            state = newstate

        self.learn_state = tuple(state)

