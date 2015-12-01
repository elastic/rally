import track.track as track
import cluster.cluster as cluster
import telemetry.metrics as m


# Benchmark runner
class Driver:
  def setup(self, cluster, track):
    track.setup_benchmark(cluster)
    cluster.wait_for_status(track.required_cluster_status())

  def go(self, cluster, track):
    metrics = m.MetricsCollector()
    # TODO dm: This is just here to ease the migration, consider gathering metrics for *all* tracks later
    if track.requires_metrics():
      metrics.startCollection(cluster)
    # TODO dm: I sense this is too concrete for a driver -> abstract this a bit later
    track.benchmark_indexing(cluster, metrics)
    metrics.stopCollection()
    # TODO dm: *Might* be interesting to gather metrics also for searching (esp. memory consumption) -> later
    track.benchmark_searching(cluster)
