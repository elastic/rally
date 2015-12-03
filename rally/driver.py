import rally.metrics as m


# Benchmark runner
class Driver:
  def __init__(self, config):
    self._config = config

  def setup(self, cluster, track):
    track.setup_benchmark(cluster)
    cluster.wait_for_status(track.required_cluster_status())

  def go(self, cluster, track):
    metrics = m.MetricsCollector(self._config, track.name())
    # TODO dm: This is just here to ease the migration, consider gathering metrics for *all* tracks later
    if track.requires_metrics():
      metrics.start_collection(cluster)
    # TODO dm: I sense this is too concrete for a driver -> abstract this a bit later (should move to track, they all need a unified interface)
    track.benchmark_indexing(cluster, metrics)
  # TODO dm: *Might* be interesting to gather metrics also for searching (esp. memory consumption) -> later
    track.benchmark_searching(cluster, metrics)
    # TODO dm: Check with Mike if it is ok to include this here (original code shut down the server first)
    # This is also just a hack for now (should be in track for first step and metrics for second one)
    data_paths = self._config.opts("provisioning", "local.data.paths")
    track.printIndexStats(data_paths[0])
    metrics.stop_collection()
