import re

import pytest

from arti.fingerprints import Fingerprint
from arti.partitions import CompositeKey, CompositeKeyTypes, Int64Key
from arti.storage import InputFingerprints
from arti.storage.literal import StringLiteral, StringLiteralPartition


def test_StringLiteral() -> None:
    literal = StringLiteral(id="test", value="test")
    partitions = literal.discover_partitions(key_types=CompositeKeyTypes())
    assert len(partitions) == 1
    partition = partitions[0]
    assert isinstance(partition, StringLiteralPartition)
    assert partition.value is not None
    assert partition.value == literal.value
    assert partition.compute_content_fingerprint() == Fingerprint.from_string(partition.value)
    # Confirm value=None returns no partitions
    assert StringLiteral(id="test").discover_partitions(key_types=CompositeKeyTypes()) == ()
    # Confirm keys/input_fingerprint validators don't error for empty values
    assert partition == StringLiteralPartition(
        id="test", input_fingerprint=Fingerprint.empty(), keys=CompositeKey(), value="test"
    )
    # Confirm empty value raises
    with pytest.raises(FileNotFoundError, match="Literal has not been written yet"):
        StringLiteralPartition(id="test", keys=CompositeKey()).compute_content_fingerprint()


def test_StringLiteral_errors() -> None:
    literal = StringLiteral(value="test")
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Literal storage cannot have a `value` preset (test) for a Producer output"
        ),
    ):
        literal.discover_partitions(
            key_types=CompositeKeyTypes(),
            input_fingerprints=InputFingerprints(
                {CompositeKey(i=Int64Key(key=5)): Fingerprint.from_int(5)}
            ),
        )
    with pytest.raises(
        ValueError,
        match=re.escape("Literal storage can only be partitioned if generated by a Producer."),
    ):
        literal.discover_partitions(
            key_types=CompositeKeyTypes(i=Int64Key), input_fingerprints=InputFingerprints()
        )
