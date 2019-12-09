#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from libc.stdint cimport int64_t, int32_t


DEF MONTHS_PER_YEAR         = 12
DEF USECS_PER_HOUR          = 3600000000
DEF USECS_PER_MINUTE        = 60000000
DEF USECS_PER_SEC           = 1000000
DEF MAX_INTERVAL_PRECISION  = 6


@cython.final
cdef class RelativeDelta:

    def __init__(self, *, int64_t microseconds=0,
                 int32_t days=0, int32_t months=0):
        self.microseconds = microseconds
        self.days = days
        self.months = months

    def __eq__(self, other):
        if type(other) is not RelativeDelta:
            return NotImplemented

        return (
            self.microseconds == (<RelativeDelta>other).microseconds and
            self.days == (<RelativeDelta>other).days and
            self.months == (<RelativeDelta>other).months
        )

    def __hash__(self):
        return hash((RelativeDelta, self.microseconds, self.days, self.months))

    def __repr__(self):
        return f'<edgedb.RelativeDelta "{self}">'

    @cython.cdivision(True)
    def __str__(self):
        # This is closely modeled after interval_out().
        cdef:
            int64_t year = self.months / MONTHS_PER_YEAR
            int64_t mon = self.months % MONTHS_PER_YEAR
            int64_t time = self.microseconds
            int64_t tfrac
            int64_t hour
            int64_t min
            int64_t sec
            int64_t fsec
            list buf = []
            bint neg
            bint is_before = False
            bint is_first = True

        tfrac = time / <int64_t>USECS_PER_HOUR
        time -= tfrac * <int64_t>USECS_PER_HOUR
        hour = tfrac

        if (hour < 0) != (tfrac < 0):
            raise ValueError('interval out of range')

        tfrac = time / <int64_t>USECS_PER_MINUTE
        time -= tfrac * <int64_t>USECS_PER_MINUTE
        min = tfrac
        sec = time / USECS_PER_SEC
        fsec = time - sec * USECS_PER_SEC

        if year:
            buf.append('{}{}{} year{}'.format(
                '' if is_first else ' ',
                '+' if is_before and year > 0 else '',
                year,
                's' if year != 1 else ''))

            is_first = False
            is_before = year < 0

        if mon:
            buf.append('{}{}{} month{}'.format(
                '' if is_first else ' ',
                '+' if is_before and mon > 0 else '',
                mon,
                's' if mon != 1 else ''))

            is_first = False
            is_before = mon < 0

        if self.days:
            buf.append('{}{}{} day{}'.format(
                '' if is_first else ' ',
                '+' if is_before and self.days > 0 else '',
                self.days,
                's' if self.days != 1 else ''))

            is_first = False
            is_before = self.days < 0

        if is_first or hour != 0 or min != 0 or sec != 0 or fsec != 0:
            neg = hour < 0 or min < 0 or sec < 0 or fsec < 0
            buf.append('{}{}{:0>2}:{:0>2}:{:0>2}'.format(
                '' if is_first else ' ',
                '-' if neg else ('+' if is_before else ''),
                abs(hour), abs(min), abs(sec)))

            fsec = abs(fsec)
            if fsec:
                buf.append(f'.{fsec:0>6}'.rstrip('0'))

        return ''.join(buf)


cdef new_duration(int64_t microseconds, int32_t days, int32_t months):
    cdef RelativeDelta dur = RelativeDelta.__new__(RelativeDelta)

    dur.microseconds = microseconds
    dur.days = days
    dur.months = months

    return dur
