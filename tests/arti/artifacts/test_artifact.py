from typing import Any

import pytest

from arti.annotations import Annotation
from arti.artifacts import Artifact
from arti.formats import Format
from arti.partitions import CompositeKeyTypes, Int64Key
from arti.statistics import Statistic
from arti.storage import Storage
from arti.types import Int64, List, Struct, Type
from tests.arti.dummies import A1, A2, P1, P2, DummyFormat, DummyStatistic, DummyStorage


def test_cast() -> None:
    assert isinstance(Artifact.cast(A1()), A1)
    assert isinstance(Artifact.cast(P1(a1=A1())), A2)
    with pytest.raises(ValueError, match="P2 produces 2 Artifacts"):
        Artifact.cast(P2(a2=A2()))


@pytest.mark.xfail  # Artifact casting to python objects is not implemented yet.
def test_cast_todo() -> None:
    Artifact.cast(5)
    Artifact.cast("hi")
    Artifact.cast([1, 2, 3])


def test_Artifact_validation() -> None:
    class BadFormat(DummyFormat):
        def supports(self, type_: Type) -> None:
            raise ValueError("Format - Boo!")

    class BadStorage(DummyStorage):
        def supports(self, type_: Type, format: Format) -> None:
            raise ValueError("Storage - Boo!")

    with pytest.raises(ValueError, match="MissingTypeArtifact must set `type`") as exc:

        class MissingTypeArtifact(Artifact):
            format: DummyFormat = DummyFormat()
            storage: DummyStorage = DummyStorage()

    with pytest.raises(ValueError, match="Format - Boo!"):

        class BadFormatArtifact(Artifact):
            type: Int64 = Int64()
            format: BadFormat = BadFormat()
            storage: DummyStorage = DummyStorage()

        BadFormatArtifact()

    with pytest.raises(ValueError, match="Storage - Boo!"):

        class BadStorageArtifact(Artifact):
            type: Int64 = Int64()
            format: DummyFormat = DummyFormat()
            storage: BadStorage = BadStorage()

        BadStorageArtifact()

    with pytest.raises(ValueError, match="overriding `type` is not supported") as exc:

        class OverrideTypeArtifact(Artifact):
            type: Int64 = Int64()
            format: DummyFormat = DummyFormat()
            storage: DummyStorage = DummyStorage()

        OverrideTypeArtifact(type=Int64(nullable=True))
    # Confirm {Format,Storage}.support are not called
    with pytest.raises(AssertionError):
        exc.match("Format - Boo!")
    with pytest.raises(AssertionError):
        exc.match("Storage - Boo!")

    class GoodArtifact(Artifact):
        type: Type = Int64()
        format: Format = DummyFormat()
        storage: Storage[Any] = DummyStorage()

    GoodArtifact()


def test_instance_attr_merging() -> None:
    class Ann1(Annotation):
        x: int

    class Ann2(Annotation):
        y: int

    class Stat1(DummyStatistic):
        pass

    class Stat2(DummyStatistic):
        pass

    class MyArtifact(A1):
        annotations: tuple[Annotation, ...] = (Ann1(x=5),)
        statistics: tuple[Statistic, ...] = (Stat1(),)

    artifact = MyArtifact(annotations=[Ann2(y=10)], statistics=[Stat2()])
    assert tuple(type(a) for a in artifact.annotations) == (Ann1, Ann2)
    assert tuple(type(s) for s in artifact.statistics) == (Stat1, Stat2)


def test_Artifact_partition_key_types() -> None:
    class NonPartitioned(Artifact):
        type: List = List(element=Struct(fields={"a": Int64()}))

    assert NonPartitioned.partition_key_types == CompositeKeyTypes()
    assert not NonPartitioned.is_partitioned

    class Partitioned(Artifact):
        type: List = List(element=Struct(fields={"a": Int64()}), partition_by=("a",))

    assert Partitioned.partition_key_types == CompositeKeyTypes({"a": Int64Key})
    assert Partitioned.is_partitioned


def test_Artifact_storage_path_resolution() -> None:
    class S(DummyStorage):
        key = "test-{partition_key_spec}"

    class A(Artifact):
        type: Type = List(element=Struct(fields={"a": Int64()}), partition_by=("a",))
        format: Format = DummyFormat()
        storage: S

    assert A(storage=S()).storage.key == "test-a_key={a.key}"
