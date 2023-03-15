from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Generic, TypeVar

from neomodel import (
    IntegerProperty,
    JSONProperty,
    One,
    RelationshipDefinition,
    RelationshipTo,
    StringProperty,
    StructuredNode,
    StructuredRel,
    config,
    db,
)

from arti import (
    Artifact,
    Backend,
    Fingerprint,
    Format,
    Graph,
    InputFingerprints,
    PartitionKey,
    Storage,
    StoragePartition,
    StoragePartitions,
    Type,
)
from arti.internal.models import Model

_Model = TypeVar("_Model", bound=Model)


### META ###
class Node(StructuredNode, Generic[_Model]):
    fingerprint = IntegerProperty(required=True, unique_index=True)
    class_name = StringProperty(required=True)

    @staticmethod
    def get_model_class_name(value: object) -> str:
        return ".".join([type(value).__module__, type(value).__qualname__])

    @classmethod
    @abstractmethod
    def dict_to_arti(self, data: dict[str, Any]) -> _Model:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def arti_to_dict(self, model: _Model) -> dict[str, Any]:
        raise NotImplementedError()


class ArtifactToGraphSnapshot(StructuredRel):
    name = StringProperty(required=True)


class StoragePartitionToPartitionKey(StructuredRel):
    name = StringProperty(required=True)


class TypeNode(Node[Type]):
    contents = JSONProperty()

    @classmethod
    def dict_to_arti(self, data: dict[str, Any]) -> Type:
        breakpoint()

    @classmethod
    def arti_to_dict(self, model: Type) -> dict[str, Any]:
        return {
            "fingerprint": model.fingerprint.key,
            "class_name": self.get_model_class_name(model),
            "contents": model.json(),
        }


class FormatNode(Node[Format]):
    contents = JSONProperty()
    type = RelationshipTo(TypeNode, "TYPE", cardinality=One)

    @classmethod
    def dict_to_arti(self, data: dict[str, Any]) -> Format:
        raise NotImplementedError()

    @classmethod
    def arti_to_dict(self, model: Format) -> dict[str, Any]:
        return {
            "fingerprint": model.fingerprint.key,
            "class_name": self.get_model_class_name(model),
            "contents": model.json(exclude={"type"}),
        }


class StorageNode(Node[Storage]):
    type = RelationshipTo(TypeNode, "TYPE", cardinality=One)
    format = RelationshipTo(FormatNode, "FORMAT", cardinality=One)
    contents = JSONProperty()

    @classmethod
    def dict_to_arti(self, data: dict[str, Any]) -> Storage:
        raise NotImplementedError()

    @classmethod
    def arti_to_dict(self, model: Storage) -> dict[str, Any]:
        return {
            "fingerprint": model.fingerprint.key,
            "class_name": self.get_model_class_name(model),
            "contents": model.json(exclude={"type", "format"}),
        }


class PartitionKeyNode(Node[PartitionKey]):
    contents = JSONProperty()

    @classmethod
    def dict_to_arti(self, data: dict[str, Any]) -> PartitionKey:
        raise NotImplementedError()

    @classmethod
    def arti_to_dict(self, model: PartitionKey) -> dict[str, Any]:
        return {
            "fingerprint": model.fingerprint.key,
            "class_name": self.get_model_class_name(model),
            "contents": model.json(),
        }


class StoragePartitionNode(Node[StoragePartition]):
    type = RelationshipTo(TypeNode, "TYPE", cardinality=One)
    format = RelationshipTo(FormatNode, "FORMAT", cardinality=One)
    storage = RelationshipTo(StorageNode, "STORAGE", cardinality=One)
    keys = RelationshipTo(PartitionKeyNode, "KEYS", model=StoragePartitionToPartitionKey)
    contents: JSONProperty()

    @classmethod
    def dict_to_arti(self, data: dict[str, Any]) -> StoragePartition:
        raise NotImplementedError()

    @classmethod
    def arti_to_dict(self, model: StoragePartition) -> dict[str, Any]:
        return {
            "fingerprint": model.fingerprint.key,
            "class_name": self.get_model_class_name(model),
            "contents": model.json(exclude={"type", "format"}),
        }


class ArtifactNode(Node[Artifact]):
    type = RelationshipTo(TypeNode, "TYPE", cardinality=One)
    format = RelationshipTo(FormatNode, "FORMAT", cardinality=One)
    storage = RelationshipTo(StorageNode, "STORAGE", cardinality=One)
    contents: JSONProperty()

    @classmethod
    def dict_to_arti(self, data: dict[str, Any]) -> Artifact:
        raise NotImplementedError()

    @classmethod
    def arti_to_dict(self, model: Artifact) -> dict[str, Any]:
        return {
            "fingerprint": model.fingerprint.key,
            "class_name": self.get_model_class_name(model),
            "contents": model.json(exclude={"type", "format", "storage"}),
        }


class GraphSnapshotNode(Node[Graph]):
    graph_name = StringProperty(required=True)
    snapshot_id = IntegerProperty(required=True)
    artifacts = RelationshipTo(ArtifactNode, "ARTIFACTS", model=ArtifactToGraphSnapshot)

    @classmethod
    def dict_to_arti(self, data: dict[str, Any]) -> Graph:
        raise NotImplementedError()

    @classmethod
    def arti_to_dict(self, model: Graph) -> dict[str, Any]:
        raise NotImplementedError()


class Neo4jBackend(Backend):
    host: str
    port: int
    username: str
    password: str
    database: str = "artigraph"

    @property
    def connection_string(self) -> str:
        return f"bolt://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @contextmanager
    def connect(self: Neo4jBackend) -> Iterator[Neo4jBackend]:
        # TODO: See if there's a way to avoid the global config / connection
        # TODO: Setup a transaction
        config.DATABASE_URL = self.connection_string
        try:
            yield self
        finally:
            config.DATABASE_URL = None  # TODO: Confirm this "disconnects"

    def try_connect(
        self, relationship: RelationshipDefinition, other_node: Node, *args: Any
    ) -> None:
        if not relationship.is_connected(other_node):
            relationship.connect(other_node, *args)

    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = ...
    ) -> StoragePartitions:
        nodes = StorageNode.nodes.filter(artifact.storage.fingerprint.key)
        breakpoint()
        print(nodes)

    def _get_or_create_artifact_node(
        self, artifact: Artifact
    ) -> tuple[TypeNode, FormatNode, StorageNode, ArtifactNode]:
        # TODO: Figure out recursive type deserialization
        type_node = TypeNode.get_or_create(TypeNode.arti_to_dict(artifact.type))[0]
        format_node = FormatNode.get_or_create(FormatNode.arti_to_dict(artifact.format))[0]
        storage_node = StorageNode.get_or_create(StorageNode.arti_to_dict(artifact.storage))[0]
        artifact_node = ArtifactNode.get_or_create(ArtifactNode.arti_to_dict(artifact))[0]
        self.try_connect(format_node.type, type_node)
        self.try_connect(storage_node.type, type_node)
        self.try_connect(storage_node.format, format_node)
        self.try_connect(artifact_node.type, type_node)
        self.try_connect(artifact_node.format, format_node)
        self.try_connect(artifact_node.storage, storage_node)
        return type_node, format_node, storage_node, artifact_node

    def _get_or_create_storage_partition_nodes(
        self, storage_node: StorageNode, partitions: StoragePartitions
    ) -> dict[StoragePartition, tuple[StoragePartitionNode, dict[str, PartitionKeyNode]]]:
        # TODO: Optimize this to reduce the # of round trips
        partition_node_map = {
            partition: (
                StoragePartitionNode.get_or_create(StoragePartitionNode.arti_to_dict(partition))[0],
                {
                    name: PartitionKeyNode.get_or_create(
                        PartitionKeyNode.arti_to_dict(partition_key)
                    )[0]
                    for name, partition_key in partition.keys.items()
                },
            )
            for partition in partitions
        }
        for storage_partition_node, partition_key_nodes in partition_node_map.values():
            self.try_connect(storage_partition_node.storage, storage_node)
            for name, partition_key_node in partition_key_nodes.items():
                self.try_connect(storage_partition_node.keys, partition_key_node, {"name": name})
        return partition_node_map

    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        _, _, storage_node, _ = self._get_or_create_artifact_node(artifact)
        self._get_or_create_storage_partition_nodes(storage_node, partitions)

    def write_graph_partitions(
        self,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        _, _, storage_node, artifact_node = self._get_or_create_artifact_node(artifact)
        self._get_or_create_storage_partition_nodes(storage_node, partitions)
        snapshot_node = GraphSnapshotNode.get_or_create(
            {
                # TODO: Refactor to pass in the Graph (or the graph fingerprint?)
                "snapshot_id": graph_snapshot_id.key,
                "fingerprint": graph_snapshot_id.key,  # TODO, graph and graph snapshot need separate fingerprints
                "graph_name": graph_name,
                "class_name": "GraphSnapshot",  # TODO, switch to GraphSnapshot / Graph
            }
        )[0]
        self.try_connect(snapshot_node.artifacts, artifact_node, {"name": artifact_key})

    def read_graph_partitions(
        self, graph_name: str, graph_snapshot_id: Fingerprint, artifact_key: str, artifact: Artifact
    ) -> StoragePartitions:
        query = f"""
        match
            (n:GraphSnapshotNode {{fingerprint: {int(graph_snapshot_id.key)}}})
            -[r:ARTIFACTS {{name: '{artifact_key}'}}]
            ->(arti:ArtifactNode)
            -[rs:STORAGE]
            ->(s:StorageNode {{fingerprint: {int(artifact.storage.fingerprint.key)}}})
            <-[rspn:STORAGE]
            -(spn:StoragePartitionNode)
        return spn, n
        """
        # TODO: Convert
        storage_partitions = db.cypher_query(query, resolve_objects=True)
        breakpoint()

    def read_graph_tag(self, graph_name: str, tag: str) -> Fingerprint:
        return super().read_graph_tag(graph_name, tag)

    def write_graph_tag(
        self, graph_name: str, graph_snapshot_id: Fingerprint, tag: str, overwrite: bool = False
    ) -> None:
        return super().write_graph_tag(graph_name, graph_snapshot_id, tag, overwrite)
