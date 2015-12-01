# TODO dm: Think about whether we want to split this in order to support also Lucene benchmarking (-> later)

# Abstract representation for a series (corresponds to a concrete benchmark)
#
# A benchmark defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
#
# Contains of 1 .. n tracks
class Series:
  def __init__(self, name, tracks):
    self._name = name
    self._tracks = tracks

  def name(self):
    return self._name

  def tracks(self):
    return self._tracks

  def estimated_disk_usage_in_bytes(self):
    # TODO dm: Implement a best-effort guess and provide a proper info the user at startup (+ startup option to suppress this for CI builds)
    return 0

  # TODO dm: Consider using abc (http://stackoverflow.com/questions/4382945/abstract-methods-in-python)
  def setup(self, config):
    # Download necessary data etc.
    raise NotImplementedError("abstract method")


# Abstract representation for tracks
#
# A track defines the concrete operations that will be done and also influences system configuration
class Track:
  def __init__(self, name):
    self._name = name

  # TODO dm: Is there a more idiomatic way?
  def name(self):
    return self._name

  # TODO dm: Consider using abc (http://stackoverflow.com/questions/4382945/abstract-methods-in-python)
  def required_cluster_status(self):
    raise NotImplementedError("abstract method")

    # TODO dm: This is just here to ease the migration, consider gathering metrics for all tracks later

  def requires_metrics(self):
    raise NotImplementedError("abstract method")

  # TODO dm: rename to configure_cluster? prepare_cluster_configuration?
  def setup(self, config):
    raise NotImplementedError("abstract method")

  def setup_benchmark(self, cluster):
    raise NotImplementedError("abstract method")

  def benchmark_indexing(self, cluster, metrics):
    raise NotImplementedError("abstract method")

  def benchmark_searching(self):
    raise NotImplementedError("abstract method")
