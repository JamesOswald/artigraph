import json
from collections.abc import Sequence
from itertools import chain
from typing import Any

from arti.formats.json import JSON
from arti.io import register_reader, register_writer
from arti.storage.literal import StringLiteralPartition, _not_written_err
from arti.types import Collection, Type
from arti.views.python import PythonBuiltin


def _read_json_literal(partition: StringLiteralPartition) -> Any:
    if partition.value is None:
        raise _not_written_err
    return json.loads(partition.value)


@register_reader
def _read_json_stringliteral_python(
    type_: Type,
    format: JSON,
    storage_partitions: Sequence[StringLiteralPartition],
    view: PythonBuiltin,
) -> Any:
    if isinstance(type_, Collection) and type_.is_partitioned:
        return list(
            chain.from_iterable(
                _read_json_literal(storage_partition) for storage_partition in storage_partitions
            )
        )
    else:
        assert len(storage_partitions) == 1  # Better error handled in base read
        return _read_json_literal(storage_partitions[0])


@register_writer
def _write_json_stringliteral_python(
    data: Any,
    type_: Type,
    format: JSON,
    storage_partition: StringLiteralPartition,
    view: PythonBuiltin,
) -> StringLiteralPartition:
    return storage_partition.copy(update={"value": json.dumps(data)})
