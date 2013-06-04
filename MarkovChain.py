#!/usr/bin/python3

import bisect
import random
import itertools
import collections
import copy
import abc

import numpy as np

from Graph import DirectedWeightedGraph

def weighted_choice(choices, weights):
    cum = np.add.accumulate(weights)
    rand = random.random() * cum[-1]
    return choices[bisect.bisect(cum, rand)]

class AbstractMarkovGraph:
    @abc.abstractmethod
    def add_transition(self, src, dst):
        """
        Add a state transition into the state graph.

        This will add edges with weight 1 or increase the weight if the edge
        already exists.
        """

    @abc.abstractmethod
    def get_weighted_transitions(self, src):
        pass

    @abc.abstractmethod
    def get_random_state(self, random_choice=None):
        pass

    @abc.abstractmethod
    def __iadd__(self, other):
        pass

class NativeMarkovGraph(AbstractMarkovGraph, DirectedWeightedGraph):
    def add_transition(self, src, dst):
        self.add_vertex(src)
        self.add_vertex(dst)
        self.add_edge(src, dst, 1)

    def get_weighted_transitions(self, src):
        return self.get_edges_at(src)

    def get_random_state(self, random_choice=None):
        random_choice = random_choice or random.choice
        return random_choice(list(self.V))

class MarkovChain:
    """A Markov source of order n.
    """

    def __init__(self, order, debug=False, graph=None):
        self.order = int(order)
        self.time = 0
        self.graph = graph or NativeMarkovGraph()
        self.state = ()
        self.learn_state = ()

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

            if len(oldstate) == self.order:
                state.popleft()

            state.append(i)

            # FIXME how to handle the start case?
            self.graph.add_transition(oldstate, tuple(state))

        self.learn_state = tuple(state)

    def __iadd__(self, other):
        if self.order != other.order:
            raise ValueError(
                "Cannot merge chains of different order ({} != {})".format(
                    self.order,
                    other.order
                ))

        # FIXME: handle time and current state etc.
        self.graph += other.graph
        return self
