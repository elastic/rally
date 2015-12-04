import rally.metrics as m


# Benchmark runner
class Driver:
  def __init__(self, config):
    self._config = config

  def setup(self, cluster, track_setup):
    track_setup.setup_benchmark(cluster)
    cluster.wait_for_status(track_setup.required_cluster_status())

  def go(self, cluster, track_setup):
    metrics = m.MetricsCollector(self._config, track_setup.name())
    # TODO dm: This is just here to ease the migration, consider gathering metrics for *all* track setups later
    if track_setup.requires_metrics():
      metrics.start_collection(cluster)
    # TODO dm: I sense this is too concrete for a driver -> abstract this a bit later (should move to trac setupk, they all need a unified interface)
    track_setup.benchmark_indexing(cluster, metrics)
    # TODO dm: *Might* be interesting to gather metrics also for searching (esp. memory consumption) -> later
    track_setup.benchmark_searching(cluster, metrics)
    # TODO dm: Check with Mike if it is ok to include this here (original code shut down the server first)
    # This is also just a hack for now (should be in track for first step and metrics for second one)
    data_paths = self._config.opts("provisioning", "local.data.paths")
    track_setup.printIndexStats(data_paths[0])
    metrics.stop_collection()
