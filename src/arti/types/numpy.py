from __future__ import annotations

from functools import partial
from typing import Any, cast

import numpy as np

from arti import types

# NOTE: TypeAdapters for some types may still be missing.

numpy_type_system = types.TypeSystem(key="numpy")


class _NumpyScalarTypeAdapter(types._ScalarClassTypeAdapter):
    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        # NOTE: this works for both direct type and np.dtype comparison, eg:
        # - np.bool_ == np.bool_
        # - np.dtype("bool") == np.bool_
        return cast(bool, type_ == cls.system)


_generate = partial(_NumpyScalarTypeAdapter.generate, type_system=numpy_type_system)

_generate(artigraph=types.Binary, system=np.bytes_)
_generate(artigraph=types.Boolean, system=np.bool_)
_generate(artigraph=types.String, system=np.str_)
for _precision in (16, 32, 64):
    _generate(
        artigraph=getattr(types, f"Float{_precision}"),
        system=getattr(np, f"float{_precision}"),
        priority=_precision,
    )
for _precision in (8, 16, 32, 64):
    _generate(
        artigraph=getattr(types, f"Int{_precision}"),
        system=getattr(np, f"int{_precision}"),
        priority=_precision,
    )
    _generate(
        artigraph=getattr(types, f"UInt{_precision}"),
        system=getattr(np, f"uint{_precision}"),
        priority=_precision,
    )
