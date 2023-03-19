import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from arti import Artifact, Graph, producer
from arti.backends.neo4j import Neo4jBackend
from arti.formats.json import JSON
from arti.partitions import Int64Key
from arti.storage.local import LocalFile
from arti.types import Collection, Int64, Struct
from tests.arti.dummies import DummyFormat

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@producer()
def div(a: int, b: int) -> int:
    return a // b


@pytest.fixture()
def neo4j_backend() -> Neo4jBackend:
    return Neo4jBackend(
        host="127.0.0.1",
        port=7687,
        username="neo4j",
        password="artigraph",
        database="artigraph",
    )


@pytest.fixture
def graph(neo4j_backend: Neo4jBackend) -> Graph:
    with Graph(name="test", backend=neo4j_backend) as g:
        g.artifacts.a = 6
        g.artifacts.b = 10
        g.artifacts.namespace.z = 15
        g.artifacts.c = div(a=g.artifacts.a, b=g.artifacts.b)
    return g


@pytest.fixture
def graph_with_nested(request: pytest.FixtureRequest, neo4j_backend: Neo4jBackend) -> Graph:
    data_dir = FIXTURES_DIR / request.node.name.replace("[", "").replace("]", "")
    data_dir.mkdir()
    with Graph(name="test-nested", backend=neo4j_backend) as g:
        g.artifacts.d = Artifact(
            type=Collection(
                element=Struct(fields={"id": Int64(), "value": Int64()}), partition_by=("id",)
            ),
            format=JSON(),
            storage=LocalFile(path=str(data_dir / "{id.key}.json")),
        )

    g.write([{"value": 1}], artifact=g.artifacts.d, keys={"id": Int64Key(key=1)})
    g.write([{"value": 2}], artifact=g.artifacts.d, keys={"id": Int64Key(key=2)})

    try:
        yield g
    finally:
        shutil.rmtree(data_dir)


def test_connect(neo4j_backend: Neo4jBackend):
    with neo4j_backend.connect() as connection:
        connection.run_cypher("MATCH (n) RETURN n")


def test_write_model(neo4j_backend: Neo4jBackend, graph: Graph):
    with neo4j_backend.connect() as connection:
        connection.write_model(graph.artifacts.a)


def test_write_model_with_nested(neo4j_backend: Neo4jBackend, graph_with_nested: Graph):
    with neo4j_backend.connect() as connection:
        connection.write_model(graph_with_nested.artifacts.d)


def test_read_model(neo4j_backend: Neo4jBackend, graph: Graph):
    with neo4j_backend.connect() as connection:
        artifact, _ = connection.read_model(
            node_type="Artifact", fingerprint=int(graph.artifacts.a.fingerprint.key)
        )
        assert artifact.fingerprint == graph.artifacts.a.fingerprint
        assert artifact == graph.artifacts.a


def test_read_models(neo4j_backend: Neo4jBackend, graph: Graph, graph_with_nested: Graph):
    with neo4j_backend.connect() as connection:
        artifacts, _ = connection.read_models(
            node_types=["Artifact", "Artifact"],
            fingerprints=[
                int(graph.artifacts.a.fingerprint.key),
                int(graph_with_nested.artifacts.d.fingerprint.key),
            ],
        )


def test_read_model_with_nested(neo4j_backend: Neo4jBackend, graph_with_nested: Graph):
    with neo4j_backend.connect() as connection:
        artifact, _ = connection.read_model(
            node_type="Artifact", fingerprint=int(graph_with_nested.artifacts.d.fingerprint.key)
        )
        assert artifact.fingerprint == graph_with_nested.artifacts.d.fingerprint
        assert artifact == graph_with_nested.artifacts.d

def test_write_snapshot(graph: Graph):
    graph.snapshot()


def test_write_build(graph: Graph):
    graph.build()
