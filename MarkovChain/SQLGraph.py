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
    def __init__(self, edge_table):
        dct = {}
        for col in edge_table.columns:
            dct[col.name] = ''
        dct["weight"] = 1

        super().__init__(edge_table, values=dct)

    def set_nodes(self, node_a, node_b):
        dct = {}
        for i, va, vb in zip(itertools.count(), node_a, node_b):
            if va is None:
                va = ''
            if vb is None:
                vb = ''
            dct["v"+str(i)+"a"] = va
            dct["v"+str(i)+"b"] = vb
        return self.values(**dct)

@compiles(InsertOrUpdateEdge)
def compile_insert_or_update(insert, compiler, **kwargs):
    s = compiler.visit_insert(insert, **kwargs)
    return s + " ON DUPLICATE KEY UPDATE weight = weight + 1"


class CollectEdges:
    def __init__(self, conn, edge_table, count=1024):
        self._max_count = count
        self._edges = []
        self._conn = conn
        self._insert = InsertOrUpdateEdge(edge_table)

    def add(self, node_a, node_b):
        dct = {}
        for i, va, vb in zip(itertools.count(), node_a, node_b):
            if va is None:
                va = ''
            if vb is None:
                vb = ''
            dct["v"+str(i)+"a"] = va
            dct["v"+str(i)+"b"] = vb
        dct["weight"] = 1
        self._edges.append(dct)

        if len(self._edges) >= self._max_count:
            self.flush()

    def flush(self):
        if len(self._edges) == 0:
            return
        self._conn.execute(
            self._insert,
            self._edges)
        self._edges = []

class SQLMarkovGraph(MarkovChain.Graph.AbstractMarkovGraph):
    def __init__(self, order, url,
            engine_and_conn=(None, None),
            max_word_length=63,
            debug=False):
        super().__init__()
        if order is not None and order <= 0:
            raise ValueError("order must be a positive integer.")
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
            if order_from_database is None:
                raise ValueError("Could not get order from database and"
                                 " no order was given.")
            self._order = order_from_database
            order = self._order

        self._src_column_names = ['v'+str(i)+"a" for i in range(order)]
        self._dst_column_names = ['v'+str(i)+"b" for i in range(order)]

        args = [Column(
                    col,
                    Unicode(max_word_length),
                    nullable=True,
                    primary_key=True)
                for col in self._src_column_names + self._dst_column_names]

        self._edges = Table(
            'edges', self._metadata,
            Column('weight',
                   Integer,
                   nullable=False),
            *args
        )

        self._src_columns = [getattr(self._edges.c, col)
                             for col in self._src_column_names]
        self._dst_columns = [getattr(self._edges.c, col)
                             for col in self._dst_column_names]

        self._metadata.create_all(self._engine)
        self._collect = CollectEdges(self._conn, self._edges)

    @staticmethod
    def _engine_and_conn_for_url(url, echo=False):
        engine = create_engine(url, echo=echo)
        conn = engine.connect()
        return engine, conn

    def _order_from_db(self):
        try:
            tbl = Table("edges",
                        MetaData(),
                        autoload=True,
                        autoload_with=self._engine)
        except sqlalchemy.exc.NoSuchTableError:
            return None
        else:
            return len(list(filter(lambda x: x.name.startswith("v") and x.name.endswith("a"),
                                   tbl.columns)))

    def add_transition(self, src, dst):
        self._collect.add(src, dst)

    def _recombine_node(self, items):
        node = self._recreate_node(itertools.islice(items, 0, self._order))
        weight = items[self._order]
        return node, weight

    @staticmethod
    def _where_node(columns, node):
        iterator = iter(zip(columns, node))
        col, val = next(iterator)
        clause = col == val
        for col, val in iterator:
            clause &= col == val
        return clause

    @staticmethod
    def _recreate_node(row):
        return tuple(x.decode("utf-8") for x in row)

    def get_weighted_transitions(self, src):
        self._collect.flush()
        return map(
            self._recombine_node,
            self._conn.execute(
                select(self._dst_columns + [self._edges.c.weight])
                .where(self._where_node(self._src_columns, src))
            )
            )

    def __iadd__(self, other):
        return NotImplemented

    def get_random_state(self, random_choice=None):
        if random_choice is None:
            # we'll try to offload the random choice to the DB server
            count = self._conn.execute(
                        select([func.count()])
                            .select_from(self._edges)
                            .group_by(*self._src_columns)
                        )
            count = len(list(count))
            node = tuple(self._conn.execute(
                select(self._src_columns)
                    .group_by(*self._src_columns)
                    .offset(random.randint(0, count))
                ).fetchone())
            return node
        else:
            # okay, *this* will be horribly slow
            nodes = list(self._conn.execute(
                select(self._src_columns).
                group_by(*self._src_columns)
                ))
            return self._recreate_node(random_choice(nodes))

    @property
    def order(self):
        return self._order

    @classmethod
    def open(cls, url, debug=False):
        return cls(None,
            url,
            debug=debug)

    def flush(self, url=None):
        self._collect.flush()

