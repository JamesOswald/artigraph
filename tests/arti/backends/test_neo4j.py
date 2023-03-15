from pytest import fixture

from arti import Graph, producer
from arti.backends.neo4j import Neo4jBackend
from tests.arti.dummies import div


@producer()
def div(a: int, b: int) -> int:
    return a // b


@fixture()
def neo4j_backend() -> Neo4jBackend:
    return Neo4jBackend(
        host="localhost",
        port=7687,
        username="neo4j",
        password="artigraph",
        database="artigraph",
    )


@fixture
def graph(neo4j_backend: Neo4jBackend) -> Graph:
    with Graph(name="Test", backend=neo4j_backend) as g:
        g.artifacts.a = 6
        g.artifacts.b = 10
        g.artifacts.namespace.z = 15
        g.artifacts.c = div(a=g.artifacts.a, b=g.artifacts.b)
    return g


def test_write_snapshot(graph: Graph):
    graph.snapshot()


def test_write_build(graph: Graph):
    graph.build()
