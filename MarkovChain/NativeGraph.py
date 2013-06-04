import random

import Graph

import MarkovChain.Graph

class NativeMarkovGraph(MarkovChain.Graph.AbstractMarkovGraph,
                        Graph.DirectedWeightedGraph):
    """
    Uses DirectedWeightedGraph from Graph to implement a markov graph.
    """

    def add_transition(self, src, dst):
        self.add_vertex(src)
        self.add_vertex(dst)
        self.add_edge(src, dst, 1)

    def get_weighted_transitions(self, src):
        return self.get_edges_at(src)

    def get_random_state(self, random_choice=None):
        random_choice = random_choice or random.choice
        return random_choice(list(self.V))
