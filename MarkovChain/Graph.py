import abc

class AbstractMarkovGraph(metaclass=abc.ABCMeta):
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

    @abc.abstractclassmethod
    def open(cls, url):
        pass

    @abc.abstractmethod
    def flush(self):
        pass
