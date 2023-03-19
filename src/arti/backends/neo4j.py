from __future__ import annotations

import importlib
import json
from collections import defaultdict
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from graphlib import TopologicalSorter
from operator import attrgetter
from typing import Any, TypeVar

import py2neo

from arti import Artifact, Backend, Connection, GraphSnapshot, InputFingerprints, StoragePartitions
from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.internal.utils import _int, int64

_Model = TypeVar("_Model", bound=Model)


class Neo4jConnection(Connection):
    def __init__(self, neo_graph: py2neo.Graph, backend: Neo4jBackend):
        self.neo_graph = neo_graph
        self.backend = backend

    def run_cypher(self, query: str, **kwargs: Any) -> Any:
        return self.neo_graph.run(query, **kwargs)

    def get_model_class_name(self, model: Model) -> str:
        return f"{type(model).__module__}:{type(model).__qualname__}"

    def connect_nodes(
        self,
        left_fingerprint: int,
        right_fingerprint: int,
        relation_type: str,
        relationship_attrs: dict[str, Any] = {},
    ) -> None:
        relationship_expression = (
            f"[r:{relation_type} {{{', '.join([f'{key}: ${key}' for key in relationship_attrs.keys()])}}}]"
            if relationship_attrs
            else f"[r:{relation_type}]"
        )
        # TODO use node_type to build relationships
        return self.neo_graph.run(
            f"MATCH (a), (b) WHERE a.fingerprint = $left_fingerprint AND b.fingerprint = $right_fingerprint MERGE (a)-{relationship_expression}->(b) return r",
            left_fingerprint=left_fingerprint,
            right_fingerprint=right_fingerprint,
            **relationship_attrs,
        )

    def node_to_arti(
        self,
        node: py2neo.Node,
        relation_map: dict[tuple[py2neo.Node, str], py2neo.Relationship],
        node_model_map: dict[py2neo.Node, _Model],
    ) -> _Model:
        model_data = {}
        mod_name, cls_name = node["class_name"].split(":")
        module = importlib.import_module(mod_name)
        model = attrgetter(cls_name)(module)
        if "backend" in model.__fields__:
            model_data["backend"] = self.backend

        for key, value in model.__fields__.items():
            if issubclass(value.type_, Fingerprint):
                model_data[key] = Fingerprint.from_int64(int64(_int(node[key])))
            elif issubclass(value.type_, Model):
                relations = list(relation_map[(node, key)])
                if len(relations) == 1:
                    model_data[key] = node_model_map[relations[0].end_node]
                elif len(relations) > 1:
                    if dict(relations[0]):
                        fields = {}
                        for relation in relations:
                            # TODO is there a better way than relying on the "name" key
                            fields[relation["name"]] = node_model_map[relation.end_node]
                        model_data[key] = fields
                    else:
                        pass
            elif (
                "Mapping" in value._type_display()
            ):  # TODO gotta be a better way to determine if the field is a mapping
                model_data[key] = json.loads(node[key])
            else:
                model_data[key] = node[key]

        return model(**model_data)

    def read_model(
        self, node_type: str, fingerprint: int, node_model_map: dict[py2neo.Node, _Model] = None
    ) -> _Model:
        node_model_map = node_model_map if node_model_map is not None else {}
        data = self.neo_graph.run(
            f"match (n:{node_type} {{fingerprint: {fingerprint}}})-[r *1..]->() return r"
        ).data()

        if not data:
            raise ValueError(f"No node of type {node_type} with fingerprint {fingerprint} found!")

        relation_map = defaultdict(set)
        deps = defaultdict(set)
        for result in data:
            for relation in result["r"]:
                relation_map[(relation.start_node, relation.__class__.__name__)].add(relation)
                deps[relation.start_node].add(relation.end_node)

        for start_node in TopologicalSorter(deps).static_order():
            if start_node in node_model_map:
                continue
            model = self.node_to_arti(
                node=start_node, relation_map=relation_map, node_model_map=node_model_map
            )
            node_model_map[start_node] = model
        return model, node_model_map

    def read_models(
        self, node_types: list[str], fingerprints: list[int]
    ) -> tuple[list[_Model], dict[py2neo.Node, _Model]]:
        models = []
        node_model_map = {}
        for node_type, fingerprint in zip(node_types, fingerprints):
            model, model_map = self.read_model(node_type, fingerprint, node_model_map)
            node_model_map.update(model_map)
            models.append(model)
        return models, node_model_map

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
                    elif isinstance(value, Mapping):
                        for sub_key, sub_value in value.items():
                            if isinstance(sub_value, Model):
                                nodes_to_connect[key] = [
                                    (self.write_model(sub_value), {"name": sub_key})
                                ]
                            else:
                                props[key] = json.dumps(dict(value))
                                break
                        if not value:
                            props[key] = json.dumps(dict(value))
                    elif isinstance(value, Model):
                        nodes_to_connect[key] = [(self.write_model(value), {})]
                    else:
                        props[key] = value
                labels = [model.__class__.__name__] + [
                    base.__name__ for base in model.__class__.__bases__
                ]
                query = f"CREATE (n:{':'.join(labels)} {{{', '.join([f'{key}: ${key}' for key in props.keys()])}}}) return n"
                result = self.neo_graph.run(query, **props).data()
                for key, values in nodes_to_connect.items():
                    for fingerprint, relationship_attrs in values:
                        self.connect_nodes(
                            props["fingerprint"], fingerprint, key, relationship_attrs
                        )

                return result[0]["n"]["fingerprint"]
        return None

    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        where_query = " or ".join(
            [
                f"(sp.input_fingerprint = {int(fingerprint.key)} and sp.keys = '{json.dumps(dict(keys))}')"
                for keys, fingerprint in input_fingerprints.items()
            ]
        )
        query = f"""match (a:Artifact {{fingerprint: {int(artifact.fingerprint.key)}}})
        -[sar:storage]
        ->(s:Storage)
        <-[spr:storage]
        -(sp:StoragePartition)
        where {where_query} return sp
        """
        sp_fingerprints = self.neo_graph.run(query).data()
        partitions, _ = self.read_models(
            ["StoragePartition"] * len(sp_fingerprints), sp_fingerprints
        )
        return tuple(partitions)

    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        self.write_model(artifact)
        [self.write_model(partition) for partition in partitions]

    def write_snapshot_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        self.write_model(snapshot.graph)
        self.write_model(snapshot)
        for partition in partitions:
            self.write_model(partition)
            self.connect_nodes(
                int(snapshot.fingerprint.key),
                int(partition.fingerprint.key),
                relation_type="partition",
                relationship_attrs={"name": artifact_key},
            )
        self.connect_nodes(
            int(snapshot.graph.fingerprint.key),
            int(artifact.fingerprint.key),
            relation_type="artifact",
            relationship_attrs={"name": artifact_key},
        )
        self.connect_nodes(
            int(snapshot.fingerprint.key),
            int(artifact.fingerprint.key),
            relation_type="artifact",
            relationship_attrs={"name": artifact_key},
        )

    def read_snapshot_partitions(
        self, snapshot: GraphSnapshot, artifact_key: str, artifact: Artifact
    ) -> StoragePartitions:
        query = f"""match (gs:GraphSnapshot {{fingerprint: {int(snapshot.fingerprint.key)}}})
        -[ar:artifact {{name: '{artifact_key}'}}]
        ->(a:Artifact {{fingerprint: {int(artifact.fingerprint.key)}}})
        -[sar:storage]
        ->(s:Storage)
        <-[spr:storage]
        -(sp:StoragePartition)
        return sp.fingerprint
        """
        sp_fingerprints = self.neo_graph.run(query).to_series().tolist()
        partitions, _ = self.read_models(
            ["StoragePartition"] * len(sp_fingerprints), sp_fingerprints
        )
        return tuple(partitions)

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
            yield Neo4jConnection(graph, self)
        finally:
            graph.service.connector.close()
