from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, ClassVar, TypeVar

import py2neo
from pydantic import BaseModel

from arti import Artifact, Backend, Connection, Fingerprint, InputFingerprints, StoragePartitions
from arti.internal.models import Model
from arti.internal.utils import frozendict

_Model = TypeVar("_Model", bound=Model)


class Node(BaseModel):
    model: ClassVar[_Model]
    fingerprint: int
    class_name: str


class Neo4jConnection(Connection):
    def __init__(self, neo_graph: py2neo.Graph):
        self.neo_graph = neo_graph

    def run_cypher(self, query: str, **kwargs: Any) -> Any:
        return self.neo_graph.run(query, **kwargs)

    def get_model_class_name(self, model: Model) -> str:
        return ".".join([type(model).__module__, type(model).__qualname__])

    def connect_nodes(self, left_node: Node, right_node: Node) -> None:
        with self.driver.session(database=self.database) as session:
            session.run(
                "MATCH (a), (b) WHERE a.id = $left_id AND b.id = $right_id CREATE (a)-[:CONNECTED_TO]->(b)",
                left_id=left_node.fingerprint,
                right_id=right_node.fingerprint,
            )

    def write_model(self, model: _Model) -> list[Any] | list[dict[str, Any]]:
        if model is not None and not isinstance(model, Fingerprint):
            existing = self.neo_graph.run(
                f"match (n {{fingerprint: {int(model.fingerprint.key)}}}) return n"
            ).data()
            if existing:
                return existing[0]["n"]["fingerprint"]
            else:
                props = {
                    "class_name": self.get_model_class_name(model),
                    "fingerprint": int(model.fingerprint.key),
                }
                nodes_to_connect = {}
                for key, value in model._iter():
                    if isinstance(value, Fingerprint):
                        props[key] = int(value.fingerprint.key)
                    elif isinstance(
                        value, frozendict
                    ):  # TODO deal with partition keys / assosiation proxies
                        props[key] = json.dumps(dict(value))
                    elif isinstance(value, Model):
                        nodes_to_connect[key] = self.write_model(value)
                    else:
                        props[key] = value

                labels = [model.__class__.__name__] + [
                    base.__name__ for base in model.__class__.__bases__
                ]
                query = f"CREATE (n:{':'.join(labels)} {{{', '.join([f'{key}: ${key}' for key in props.keys()])}}}) return n"
                result = self.neo_graph.run(query, **props).data()
                for key, value in nodes_to_connect.items():
                    # TODO filter to node type and figure out cartesian product
                    query = f"""
                    match (n), (rn) where n.fingerprint = {props['fingerprint']}
                    and rn.fingerprint = {value}
                    create (n)-[r:{key}]->(rn) return r"""
                    self.neo_graph.run(query)

                return result[0]["n"]["fingerprint"]
        return None

    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = ...
    ) -> StoragePartitions:
        return super().read_artifact_partitions(artifact, input_fingerprints)

    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        self.write_model(artifact)
        [self.write_model(partition) for partition in partitions]

    def write_graph_partitions(
        self,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        # TODO need GraphSnaphot model
        pass

    def read_graph_partitions(
        self, graph_name: str, graph_snapshot_id: Fingerprint, artifact_key: str, artifact: Artifact
    ) -> StoragePartitions:
        #  match
        #     (n:GraphSnapshotNode {{fingerprint: {int(graph_snapshot_id.key)}}})
        #     -[artifacts {{name: '{artifact_key}'}}]
        #     ->(ArtifactNode)
        #     -[r:storage]
        #     ->(StorageNode)
        #     <-[storage]
        #     -(spn:StoragePartitionNode)
        #     return spn
        # """
        return super().read_graph_partitions(graph_name, graph_snapshot_id, artifact_key, artifact)

    def read_graph_tag(self, graph_name: str, tag: str) -> Fingerprint:
        return super().read_graph_tag(graph_name, tag)

    def write_graph_tag(
        self, graph_name: str, graph_snapshot_id: Fingerprint, tag: str, overwrite: bool = False
    ) -> None:
        return super().write_graph_tag(graph_name, graph_snapshot_id, tag, overwrite)


class Neo4jBackend(Backend[Neo4jConnection]):
    host: str
    port: int
    username: str
    password: str
    database: str = "artigraph"

    @contextmanager
    def connect(self: Neo4jBackend) -> Iterator[Neo4jConnection]:
        uri = f"bolt://{self.host}:{self.port}"
        graph = py2neo.Graph(uri, auth=(self.username, self.password), name=self.database)
        try:
            yield Neo4jConnection(graph)
        finally:
            graph.service.connector.close()
