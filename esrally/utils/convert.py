# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


def bytes_to_kb(b):
    return b / 1024.0 if b else b


def bytes_to_mb(b):
    return b / 1024.0 / 1024.0 if b else b


def bytes_to_gb(b):
    return b / 1024.0 / 1024.0 / 1024.0 if b else b


def bytes_to_human_string(b):
    if b is None:
        return "N/A"

    value, unit = _bytes_to_human(b)
    if unit == "bytes":
        return "%d bytes" % b

    return "%.1f %s" % (value, unit)


def bytes_to_human_value(b):
    return _bytes_to_human(b)[0]


def bytes_to_human_unit(b):
    return _bytes_to_human(b)[1]


def bytes_to_unit(unit, b):
    if unit == "N/A":
        return b
    elif unit == "GB":
        return bytes_to_gb(b)
    elif unit == "MB":
        return bytes_to_mb(b)
    elif unit == "kB":
        return bytes_to_kb(b)
    else:
        return b


def _bytes_to_human(b):
    if b is None:
        return b, "N/A"
    gb = bytes_to_gb(b)
    if gb > 1.0 or gb < -1.0:
        return gb, "GB"
    mb = bytes_to_mb(b)
    if mb > 1.0 or mb < -1.0:
        return mb, "MB"
    kb = bytes_to_kb(b)
    if kb > 1.0 or kb < -1.0:
        return (kb, "kB")
    return b, "bytes"


def number_to_human_string(number):
    return f"{number:,}"


def mb_to_bytes(mb):
    return int(mb * 1024 * 1024) if mb else mb


def gb_to_bytes(gb):
    return gb * 1024 * 1024 * 1024 if gb else gb


def seconds_to_ms(s):
    return s * 1000 if s else s


def seconds_to_hour_minute_seconds(s):
    if s:
        hours = s // 3600
        minutes = (s - 3600 * hours) // 60
        seconds = s - 3600 * hours - 60 * minutes
        return hours, minutes, seconds
    else:
        return s, s, s


def ms_to_seconds(ms):
    return ms / 1000.0 if ms else ms


def ms_to_minutes(ms):
    return ms / 1000.0 / 60.0 if ms else ms


def factor(n):
    return lambda v: v * n


def to_bool(value):
    if value in ["True", "true", "Yes", "yes", "t", "y", "1", True]:
        return True
    elif value in ["False", "false", "No", "no", "f", "n", "0", False]:
        return False
    else:
        raise ValueError("Cannot convert [%s] to bool." % value)
