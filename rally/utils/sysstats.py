import multiprocessing


def number_of_cpu_cores():
    """
    :return: The number of logical CPU cores. May be 0 on some platforms.
    """
    return multiprocessing.cpu_count()
