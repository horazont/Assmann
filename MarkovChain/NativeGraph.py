import random
import urllib.request
import urllib.parse
import pickle

import Graph

import MarkovChain.Graph

def path_or_die(url):
    parsed = urllib.parse.urlparse(url,
                                   scheme="file",
                                   allow_fragments=False)
    if parsed.scheme != "file":
        raise ValueError("A file:// URL is required.")
    if parsed.netloc != "":
        raise ValueError("file:// URL must not have a non-empty netloc "
                         "(did you forget the third /?)")
    return parsed.path


class NativeMarkovGraph(Graph.DirectedWeightedGraph,
                        MarkovChain.Graph.AbstractMarkovGraph):
    """
    Uses DirectedWeightedGraph from Graph to implement a markov graph.
    """

    def __init__(self, order):
        super().__init__()
        assert order > 0
        self._order = order
        self._url = None

    def add_transition(self, src, dst):
        if len(src) != len(dst) or len(src) != self._order:
            raise ValueError("src ({}) and/or dst ({}) doesn't adhere "
                             "to given order of {}".format(
                                src,
                                dst,
                                self._order))
        self.add_vertex(src)
        self.add_vertex(dst)
        self.add_edge(src, dst, 1)

    def get_weighted_transitions(self, src):
        return self.get_edges_at(src)

    def get_random_state(self, random_choice=None):
        random_choice = random_choice or random.choice
        return random_choice(list(self.V))

    def __iadd__(self, other):
        if self.order != other.order:
            raise ValueError(
                "Cannot merge chains of different order ({} != {})".format(
                    self.order,
                    other.order
                ))

        return super().__iadd__(self, other)

    @classmethod
    def open(cls, url):
        try:
            path = path_or_die(url)
        except ValueError:
            with urllib.request.urlopen(url) as f:
                obj = pickle.load(f)
        else:
            with open(path, "rb") as f:
                obj = pickle.load(f)

        if not isinstance(obj, cls):
            raise TypeError(
                "Unexpected type came out of the pickle! {}".format(
                    type(obj)))
        obj._url = url
        return obj

    def flush(self, url=None):
        if url is None:
            url = self._url

        path = path_or_die(url)
        with open(path, "wb") as f:
            pickle.dump(self, f)
        if url is not None:
            self._url = url

    @property
    def order(self):
        return self._order
