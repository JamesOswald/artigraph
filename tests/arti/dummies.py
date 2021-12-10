from __future__ import annotations

from typing import Annotated, Optional

from arti import (
    Annotation,
    Artifact,
    CompositeKey,
    CompositeKeyTypes,
    Fingerprint,
    Format,
    InputFingerprints,
    Producer,
    Statistic,
    Storage,
    StoragePartition,
    Type,
    TypeSystem,
    producer,
)
from arti.formats.json import JSON
from arti.types import Int32, Int64, Struct


class Num(Artifact):
    type: Type = Int64()
    format: Format = JSON()
    # Omit storage to require callers to set the instance in a separate tempdir.


@producer()
def div(a: Annotated[int, Num], b: Annotated[int, Num]) -> Annotated[int, Num]:
    return a // b


dummy_type_system = TypeSystem(key="dummy")


class DummyAnnotation(Annotation):
    pass


class DummyFormat(Format):
    type_system = dummy_type_system


class DummyPartition(StoragePartition):
    key: str = "test"

    def compute_content_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.key)


class DummyStorage(Storage[DummyPartition]):
    key: str = "test-{graph_name}-{path_tags}-{names}-{partition_key_spec}-{input_fingerprint}-{name}.{extension}"

    def discover_partitions(
        self, key_types: CompositeKeyTypes, input_fingerprints: Optional[InputFingerprints] = None
    ) -> tuple[DummyPartition, ...]:
        if key_types != CompositeKeyTypes():
            raise NotImplementedError()
        if input_fingerprints is not None and input_fingerprints != InputFingerprints():
            raise NotImplementedError()
        return (self.storage_partition_type(keys=CompositeKey(), key=self.key),)


# NOTE: when using a subclass of the original type hint, we must override[1].
#
# 1: https://github.com/samuelcolvin/pydantic/pull/3018


class DummyStatistic(Statistic):
    type: Int32 = Int32()
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A1(Artifact):
    type: Struct = Struct(fields={"a": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A2(Artifact):
    type: Struct = Struct(fields={"b": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A3(Artifact):
    type: Struct = Struct(fields={"c": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A4(Artifact):
    type: Struct = Struct(fields={"d": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class P1(Producer):
    a1: A1

    @staticmethod
    def build(a1: dict) -> Annotated[dict, A2]:  # type: ignore
        return {}


class P2(Producer):
    a2: A2

    @staticmethod
    def build(a2: dict) -> tuple[Annotated[dict, A3], Annotated[dict, A4]]:  # type: ignore
        return {}, {}


class P3(Producer):
    a1: A1
    a2: A2

    @staticmethod
    def build(a1: dict, a2: dict) -> tuple[Annotated[dict, A3], Annotated[dict, A4]]:  # type: ignore
        return {}, {}
