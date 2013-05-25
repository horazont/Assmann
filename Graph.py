#!/usr/bin/python3

class DirectedWeightedEdge:
    def __init__(self, src, dst, weight):
        self.src = src
        self.dst = dst
        self.weight = weight

    def __eq__(self, other):
        return self.dst == other.dst and self.src == other.src

    # maintain hashability
    def __hash__(self):
        return super().__hash__()

    def __str__(self):
        return '({}, {}, {})'.format(repr(self.src), repr(self.dst), 
                                     repr(self.weight))

    def __repr__(self):
        return '{}({}, {}, {})'.format(type(self).__name__, repr(self.src),
                                       repr(self.dst), repr(self.weight))

class DirectedWeightedGraph:
    def __init__(self, **kwargs):
        if 'V' not in kwargs and 'E' not in kwargs:
            self.V = set()
            self.E = set()
        else:
            try:
                self.V = kwargs['V']
                self.E = kwargs['E']
            except KeyError:
                ValueError("Both V and E arguments must be supplied")

    def add_vertex(self, v):
        self.V.add(v)

    def del_vertex(self, v):
        self.V.remove(v)
        self.E -= set(filter(lambda e: e.dst == v or e.src == v, self.E))

    def add_vertices(self, *args):
        for arg in args:
            self.add_vertex(arg)

    def add_edge(self, e):
        self.E.add(e)

    def add_edges(self, *args):
        for arg in args:
            self.add_edge(arg)

    def del_edge(self, e):
        self.E.remove(e)

    def get_edges_at(self, v):
        return filter(lambda vv: vv.src == v, self.E)

    def __str__(self):
        return '{}(V = {}, E = {})'.format(type(self).__name__, str(self.V),
                                           str(self.E))

    def __repr__(self):
        return '{}(V = {}, E = {})'.format(type(self).__name__, repr(self.V),
                                           repr(self.E))
