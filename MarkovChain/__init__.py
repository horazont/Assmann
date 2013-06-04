#!/usr/bin/python3

import bisect
import random
import itertools
import collections
import copy

import numpy as np

from MarkovChain.Graph import AbstractMarkovGraph
from MarkovChain.NativeGraph import NativeMarkovGraph

try:
    from MarkovChain.SQLGraph import SQLMarkovGraph
except ImportError:
    pass

def weighted_choice(choices, weights):
    cum = np.add.accumulate(weights)
    rand = random.random() * cum[-1]
    return choices[bisect.bisect(cum, rand)]


class MarkovChain:
    """A Markov source of order n.
    """

    def __init__(self, graph, debug=False):
        self.time = 0
        self.graph = graph
        self.state = ()
        self.learn_state = (None,) * graph.order

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, value):
        if not isinstance(value, AbstractMarkovGraph):
            raise TypeError("graph must implement AbstractMarkovGraph "
                            "interface. Got: {}".format(value))
        self._graph = value

    def set_random_state(self):
        self.state = self.graph.get_random_state()

    def next_state(self):
        self.time += 1
        cands = list(self.graph.get_weighted_transitions(self.state))
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

    def learn(self, source):
        """Build a markov model from an iterable input source.
        """
        state = collections.deque(self.learn_state)
        for i in source:
            oldstate = tuple(state)

            if len(oldstate) == self.graph.order:
                state.popleft()

            state.append(i)

            # FIXME how to handle the start case?
            self.graph.add_transition(oldstate, tuple(state))

        self.learn_state = tuple(state)

    def __iadd__(self, other):
        # FIXME: handle time and current state etc.
        self.graph += other.graph
        return self
