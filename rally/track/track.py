##########################################################################################
#
#
# BEWARE: Unfortunately, this is one of the weakest spots of the domain model. Expect a
#         lot of shuffling around! This is just here to get going.
#
#
##########################################################################################

# Abstract representation for a track (corresponds to a concrete benchmark)
#
# A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
#
# Contains of 1 .. n track setups
class Track:
  def __init__(self, name, track_setups):
    self._name = name
    self._track_setups = track_setups

  def name(self):
    return self._name

  def track_setups(self):
    return self._track_setups

  def estimated_disk_usage_in_bytes(self):
    # TODO dm: Implement a best-effort guess and provide a proper info the user at startup (+ startup option to suppress this for CI builds)
    return 0

  # TODO dm: Consider using abc (http://stackoverflow.com/questions/4382945/abstract-methods-in-python)
  def setup(self, config):
    # Download necessary data etc.
    raise NotImplementedError("abstract method")


# Abstract representation for track setups
#
# A track setup defines the concrete operations that will be done and also influences system configuration
class TrackSetup:
  def __init__(self, name):
    self._name = name

  def name(self):
    return self._name

  # TODO dm: Consider using abc (http://stackoverflow.com/questions/4382945/abstract-methods-in-python)
  def required_cluster_status(self):
    raise NotImplementedError("abstract method")

  # TODO dm: This is just here to ease the migration, consider gathering metrics for everything later
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
