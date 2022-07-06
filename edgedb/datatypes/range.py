#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import Any


class Range:

    __slots__ = ("_lower", "_upper", "_inc_lower", "_inc_upper", "_empty")

    def __init__(
        self,
        lower: Any = None,
        upper: Any = None,
        *,
        inc_lower: bool = True,
        inc_upper: bool = False,
        empty: bool = False,
    ) -> None:
        self._empty = empty

        if empty:
            if (
                lower != upper
                or lower is not None and inc_upper and inc_lower
            ):
                raise ValueError(
                    "conflicting arguments in range constructor: "
                    "\"empty\" is `true` while the specified bounds "
                    "suggest otherwise"
                )

            self._lower = self._upper = None
            self._inc_lower = self._inc_upper = False
        else:
            self._lower = lower
            self._upper = upper
            self._inc_lower = lower is not None and inc_lower
            self._inc_upper = upper is not None and inc_upper

    @property
    def lower(self):
        return self._lower

    @property
    def inc_lower(self):
        return self._inc_lower

    @property
    def upper(self):
        return self._upper

    @property
    def inc_upper(self):
        return self._inc_upper

    def is_empty(self):
        return self._empty

    def __bool__(self):
        return not self.is_empty()

    def __eq__(self, other):
        if not isinstance(other, Range):
            return NotImplemented

        return (
            self._lower,
            self._upper,
            self._inc_lower,
            self._inc_upper,
            self._empty
        ) == (
            other._lower,
            other._upper,
            other._inc_lower,
            other._inc_upper,
            self._empty,
        )

    def __hash__(self) -> int:
        return hash((
            self._lower,
            self._upper,
            self._inc_lower,
            self._inc_upper,
            self._empty,
        ))

    def __str__(self) -> str:
        if self._empty:
            desc = "empty"
        else:
            lb = "(" if not self._inc_lower else "["
            if self._lower is not None:
                lb += repr(self._lower)

            if self._upper is not None:
                ub = repr(self._upper)
            else:
                ub = ""

            ub += ")" if self._inc_upper else "]"

            desc = f"{lb}, {ub}"

        return f"<Range {desc}>"

    __repr__ = __str__
