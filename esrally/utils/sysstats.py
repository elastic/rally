import platform
import cpuinfo
import psutil


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
    return cpuinfo.get_cpu_info()["brand"]


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


def cpu_utilization(handle, interval=1.0):
    """
    :param handle: handle retrieved by calling setup_process_stats(pid).
    :param interval: The measurement interval in seconds. Optional. Defaults to 1 second.
    :return: The CPU usage in percent.
    """
    return handle.cpu_percent(interval=interval)
