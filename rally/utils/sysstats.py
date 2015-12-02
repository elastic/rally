import multiprocessing


def number_of_cpu_cores():
  return multiprocessing.cpu_count()
