import itertools
import random
import functools

from sqlalchemy import *
import sqlalchemy.sql as sql
import sqlalchemy.exc
import sqlalchemy.ext as ext
from sqlalchemy.sql.expression import Insert
from sqlalchemy.ext.compiler import compiles

import MarkovChain.Graph

class InsertOrUpdateEdge(Insert):
    def __init__(self, edge_table, node_id_a, node_id_b):
        super().__init__(edge_table, values={
            "node_a": node_id_a,
            "node_b": node_id_b,
            "weight": 1
        })

@compiles(InsertOrUpdateEdge)
def compile_insert_or_update(insert, compiler, **kwargs):
    s = compiler.visit_insert(insert, **kwargs)
    return s + " ON DUPLICATE KEY UPDATE weight = weight + 1"

class SQLMarkovGraph(MarkovChain.Graph.AbstractMarkovGraph):
    def __init__(self, order, url,
            engine_and_conn=(None, None),
            max_word_length=127,
            debug=False):
        super().__init__()
        assert order > 0
        self._order = order
        if engine_and_conn != (None, None):
            if url is not None:
                raise ValueError("Cannot pass non-None url if engine "
                                 "is given.")
            self._engine, self._conn = engine_and_conn
            if self._conn is None:
                self._conn = self._engine.connect()
        else:
            self._engine, self._conn = self._engine_and_conn_for_url(
                url,
                echo=debug
                )
        self._metadata = MetaData()
        self._node_column_names = ['v'+str(i) for i in range(order)]

        order_from_database = self._order_from_db()
        if (    order_from_database is not None and
                order_from_database != order and
                order is not None):
            raise ValueError("Existing database conflicts: Order "
                             "differs ({} != {})".format(
                                order,
                                order_from_database
                             ))

        if order is None:
            self._order = order_from_database

        args = [Column(
                    col,
                    String(max_word_length),
                    nullable=True)
                for col in self._node_column_names]
        args.append(
            UniqueConstraint(*self._node_column_names)
        )

        self._nodes = Table(
            'nodes', self._metadata,
            Column('node_id', Integer, primary_key=True),
            *args
        )

        self._edges = Table(
            'edges', self._metadata,
            Column('node_a',
                   None,
                   ForeignKey('nodes.node_id'),
                   nullable=False,
                   primary_key=True),
            Column('node_b',
                   None,
                   ForeignKey('nodes.node_id'),
                   nullable=False,
                   primary_key=True),
            Column('weight',
                   Integer,
                   nullable=False)
        )

        self._metadata.create_all(self._engine)

        self._node_columns = [
            getattr(self._nodes.c, col)
            for col in self._node_column_names
        ]

        self._fast_get_node_id = \
            functools.lru_cache()(lambda x: self._get_node_id(x))

        self._buffer = []

    @staticmethod
    def _engine_and_conn_for_url(url, echo=False):
        engine = create_engine(url, echo=echo)
        conn = engine.connect()
        return engine, conn

    def _order_from_db(self):
        try:
            tbl = Table("nodes",
                        MetaData(),
                        autoload=True,
                        autoload_with=self._engine)
        except sqlalchemy.exc.NoSuchTableError:
            return None
        else:
            return len(list(filter(lambda x: x.name.startswith("v"),
                                   tbl.columns)))

    def _node_where(self, node):
        iterable = iter(zip(self._node_columns, node))
        col, val = next(iterable)
        clause = col == val
        for col, val in iterable:
            clause &= col == val
        return clause

    def _get_node_id(self, node):
        result = self._conn.execute(sql.select(
            [self._nodes.c.node_id],
            whereclause=self._node_where(node))).fetchone()
        if result is not None:
            return result[0]
        else:
            return None

    def _has_link(self, node_id_a, node_id_b):
        return self._conn.execute(sql.select(
            [self._edges.c.weight],
            whereclause=\
                (self._edges.c.node_a == node_id_a) &
                (self._edges.c.node_b == node_id_b))).fetchone() is not None

    def _add_node(self, node):
        self._fast_get_node_id.cache_clear()
        statement = self._nodes.insert().values(
            **dict(zip(self._node_column_names, node))
        ) #.returning(self._nodes.c.node_id)

        return self._conn.execute(statement).lastrowid

    def _increase_weight(self, node_id_a, node_id_b):
        self._conn.execute(self._edges.update().
            where((self._edges.c.node_a == node_id_a) &
                  (self._edges.c.node_b == node_id_b)).
            values(self._edges.c.weight + 1))

    def _add_link(self, node_id_a, node_id_b):
        self._conn.execute(self._edges.insert().
            values(node_a=node_id_a,
                   node_b=node_id_b,
                   weight=1))

    def add_transition(self, src, dst):
        node_a = self._fast_get_node_id(src)
        if node_a is None:
            node_a = self._add_node(src)

        node_b = self._fast_get_node_id(dst)
        if node_b is None:
            node_b = self._add_node(dst)

        statement = InsertOrUpdateEdge(self._edges, node_a, node_b)

        self._conn.execute(statement)

    def _recombine_node(self, items):
        node = tuple(itertools.islice(items, 0, self._order))
        weight = items[self._order]
        return node, weight

    def get_weighted_transitions(self, src):
        return map(
            self._recombine_node,
            self._conn.execute(
                select(
                    self._node_columns + [self._edges.c.weight],
                    from_obj=[
                        self._edges.join(
                            self._nodes,
                            self._nodes.c.node_id == self._edges.c.node_b
                        )
                    ],
                    whereclause=self._edges.c.node_a == self._fast_get_node_id(src)
                )
            )
            )

    def __iadd__(self, other):
        return NotImplemented

    def get_random_state(self, random_choice=None):
        if random_choice is None:
            # we'll try to offload the random choice to the DB server
            count = self._conn.execute(
                        select([func.count()]).
                            from_table(self._nodes)
                        ).fetchone()[0]
            node = tuple(self._conn.execute(
                             self._nodes.select(self._node_columns).
                                         offset(random.randint(count))
                             ).fetchone())
            return node
        else:
            # okay, *this* will be horribly slow
            nodes = list(map(tuple, self._conn.execute(
                self._nodes.select()
                )))
            return random_choice(nodes)

    @property
    def order(self):
        return self._order

    @classmethod
    def open(cls, url, debug=False):
        return cls(None,
            url,
            debug=debug)

    def flush(self):
        pass

