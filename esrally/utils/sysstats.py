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

import platform

import psutil

# py-cpuinfo raises an Exception on unsupported systems. This is as specific as we can get.
# noinspection PyBroadException
try:
    import cpuinfo
    cpuinfo_available = True
except Exception:
    cpuinfo_available = False


def logical_cpu_cores():
    """
    :return: The number of logical CPU cores.
    """
    return psutil.cpu_count()


def physical_cpu_cores():
    """
    :return: The number of physical CPU cores.
    """
    return psutil.cpu_count(logical=False)


def cpu_model():
    """
    :return: The CPU model name.
    """
    if cpuinfo_available:
        cpu_info = cpuinfo.get_cpu_info()
        if "brand" in cpu_info:
            return cpu_info["brand"]
    return "Unknown"


def cpu_arch():
    """
    :return: The CPU architecture name.
    """
    return platform.uname().machine


def disks():
    # only physical partitions (no memory partitions etc)
    return psutil.disk_partitions(all=False)


def total_memory():
    """
    :return: total available memory in bytes
    """
    return psutil.virtual_memory().total


def os_name():
    return platform.uname().system


def os_version():
    return platform.uname().release


def disk_io_counters():
    return psutil.disk_io_counters(perdisk=False)


def process_io_counters(handle):
    """
    :param handle: handle retrieved by calling setup_process_stats(pid).
    :return: Either the current value of the associated process' disk I/O counters or None if process I/O counters are unsupported.
    """
    try:
        return handle.io_counters()
    except AttributeError:
        return None


def setup_process_stats(pid):
    """
    Sets up process stats measurements for the provided process id.

    :param pid: The process to watch. Must be a running process.
    :return: An opaque handle that has to be provided for all subsequent calls to process stats APIs.
    """
    return psutil.Process(pid)
