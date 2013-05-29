#!/usr/bin/python3

class DirectedWeightedGraph:
    def __init__(self, V=set(), E=dict()):
        self.V = set(V)
        # extra cast here to allow input such as
        #    [((src, dst), weight), ...]
        self.E = dict()
        for (src, dst), weight in dict(E).items():
            self.add_edge(src, dst, weight)

    def add_vertex(self, v):
        self.V.add(v)

    def del_vertex(self, v):
        self.V.remove(v)
        # copy the key view to a list, otherwise
        # modifying the dict during the iteration
        # would RuntimeError
        for k in list(self.E.keys()):
            (src, dst) = k
            if src == v or dst == v:
                del self.E[k]

    def add_vertices(self, *args):
        for arg in args:
            self.add_vertex(arg)

    def add_edge(self, src, dst, weight):
        src_dict = self.E.setdefault(src, dict())
        if dst in src_dict:
            src_dict[dst] += weight
        else:
            src_dict[dst] = weight

    def add_edges(self, *args):
        collections.deque(map(self.add_edge, args), 0)

    def del_edge(self, src, dst):
        src_dict = self.E.setdefault(src, dict())
        try:
            del src_dict[dst]
        finally:
            if not src_dict:
                del self.E[src]

    def get_edges_at(self, v):
        return self.E.get(v, dict()).items()

    def get_weight(self, src, dst):
        return self.E.get(src, dict()).get(dst, None)

    def adjacency_matrix(self, index_map):
        """Build the adjacency matrix representing the graph.

        Since vertices can be any kind of object, an index map must be
        supplied, mapping vertices to matrix indeces (integers, starting at 0).
        """

        import numpy

        n = len(self.V)
        mat = numpy.zeros((n, n))
        for v in sorted(self.V):
            for dst, weight in self.get_edges_at(v):
                mat[index_map[v],index_map[dst]] = weight

        return mat

    def __iadd__(self, other):
        """
        Merge *other* graph into this one.
        """

        self.V.union(other.V)
        for src, dsts in other.E.items():
            my_dict = self.E.setdefault(src, {})
            for dst, weight in dsts.items():
                if dst in my_dict:
                    my_dict[dst] += weight
                else:
                    my_dict[dst] = weight
        return self

    def __str__(self):
        return '{}(V = {}, E = {})'.format(type(self).__name__, str(self.V),
                                           str(self.E))

    def __repr__(self):
        return '{}(V = {}, E = {})'.format(type(self).__name__, repr(self.V),
                                           repr(self.E))
