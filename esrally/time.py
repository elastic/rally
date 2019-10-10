# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import time
from datetime import datetime, timezone


def to_epoch_millis(t):
    """
    Convert a time instance retrieved via time.time() to a timestamp in milliseconds since epoch.
    :param t: a time instance
    :return: the corresponding value of milliseconds since epoch.
    """
    return int(round(t * 1000))


def to_iso8601(dt):
    """
    Convert a datetime instance to a ISO-8601 compliant string.
    :param dt: A datetime instance
    :return: The corresponding ISO-8601 formatted string
    """
    return "%04d%02d%02dT%02d%02d%02dZ" % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def from_is8601(ts):
    """
    Convert an ISO-8601 compliant string (as created by ``to_iso8601(dt)``) to a datetime instance.

    :param ts: an ISO-8601 compliant string
    :return: The corresponding datetime instance.
    """
    return datetime.strptime(ts, "%Y%m%dT%H%M%SZ")


def sleep(seconds):
    time.sleep(seconds)


def _to_datetime(val, date_format=None, default_tz=None):
    if isinstance(val, datetime):
        return val
    # unix timestamp
    elif isinstance(val, float):
        return datetime.fromtimestamp(val, default_tz)
    elif isinstance(val, str):
        ret = datetime.strptime(val, date_format)
        if ret.tzinfo is None:
            ret = ret.replace(tzinfo=default_tz)
        return ret
    else:
        raise TypeError("Cannot convert unrecognized type '%s' with value '%s' to datetime." % (type(val), str(val)))


def days_ago(start_date, end_date, date_format="%d-%m-%Y", default_tz=timezone.utc):
    """

    Calculates the difference in days between a start date and an end date.

    :param start_date: The start date. May be a datetime instance, a unix timestamp or a string representation of a date.
    :param end_date: The end date. May be a datetime instance, a unix timestamp or a string representation of a date.
    :param date_format: If one or both date values are provided as strings, the date format which is needed for conversion.
    Default: "%d-%m-%Y"
    :param default_tz: Timezone to use for dates using unix timestamp or not already specified in date_format
    Default: timezone.UTC
    :return: The difference between start and end date in complete days.
    """
    start = _to_datetime(start_date, date_format, default_tz)
    end = _to_datetime(end_date, date_format, default_tz)
    return (end - start).days


class Clock:
    """
    A clock abstracts time measurements. Its main purpose is to ease testing
    """

    @staticmethod
    def now():
        """
        :return: The current time.
        """
        return time.time()

    @staticmethod
    def stop_watch():
        return StopWatch()


class StopWatch:
    def __init__(self):
        self._start = None
        self._stop = None

    def start(self):
        self._start = self._now()

    def stop(self):
        self._stop = self._now()

    def split_time(self):
        return self._interval(self._start, self._now())

    def total_time(self):
        return self._interval(self._start, self._stop)

    def _interval(self, t0, t1):
        if t0 is None:
            raise RuntimeError("start time is None")
        if t1 is None:
            raise RuntimeError("end time is None")
        return t1 - t0

    def _now(self):
        return time.perf_counter()
