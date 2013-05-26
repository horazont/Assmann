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


class CharacterSet(list):
    def __pow__(self, other):
        return type(self)(itertools.product(self, repeat=other))


def shift_append(S, c):
     S.pop()
     S.append(c)
     S.rotate(1)
     return S

class MarkovChain:
    """A Markov source of order n.
    """

    def __init__(self, xs, markov_mat, init_probs, order, debug=False):
        self.alphabet = xs
        self.order = order
        self.states = collections.deque(
                      CharacterSet(self.alphabet)**self.order)
        self.markov_mat = np.array(markov_mat)
        self.init_probs = np.array(init_probs)

        self.time = 0

        if not self._valid_markov_matrix(markov_mat):
            raise TypeError("Invalid markov matrix supplied")

        self.markov_mat = markov_mat
        self.cur_state = collections.deque(['' for i in range(order)])

    def emit(self):

        
        yield out

    def add(self):
        pass

    def build(self):
        pass

    def _valid_markov_matrix(self, mat):
        if not isinstance(np.ndarray, mat):
            return False

        if mat.shape[0] != mat.shape[1]:
            return False

    def _debug(self, msg):
        if self.debug:
            print(msg)

