import logging
import statistics

import rally.metrics
import rally.utils.convert as convert

logger = logging.getLogger("rally.reporting")


class SummaryReporter:
  def __init__(self, config):
    self._config = config

  def report(self, track):
    self.print_header("------------------------------------------------------")
    self.print_header("    _______             __   _____                    ")
    self.print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
    self.print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
    self.print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
    self.print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ ")
    self.print_header("------------------------------------------------------")

    selected_setups = self._config.opts("benchmarks", "tracksetups.selected")
    invocation = self._config.opts("meta", "time.start")
    for track_setup in track.track_setups:
      if track_setup.name in selected_setups:
        if len(selected_setups) > 1:
          self.print_header("*** Track setup %s ***\n" % track_setup.name)

        store = rally.metrics.EsMetricsStore(self._config)
        store.open(invocation, track.name, track_setup.name)

        self.report_index_throughput(store)
        print("")
        self.report_search_latency(store, track)
        self.report_total_times(store)

        self.print_header("System Metrics")
        self.report_cpu_usage(store)
        self.report_gc_times(store)
        print("")

        self.print_header("Index Metrics")
        self.report_disk_usage(store)
        self.report_segment_memory(store)
        self.report_segment_counts(store)
        print("")

        self.report_stats_times(store)

  def print_header(self, message):
    print("\033[1m%s\033[0m" % message)

  def report_index_throughput(self, store):
    self.print_header("Indexing Results (Throughput):")
    print("  %d docs/sec" % store.get_one("indexing_throughput"))

  def report_search_latency(self, store, track):
    self.print_header("Query Latency:")
    for q in track.queries:
      query_latency = store.get("query_latency_%s" % q.name)
      if query_latency:
        #TODO dm: Output percentiles, not the median...
        formatted_median = '%.1f' % statistics.median(query_latency)
        print("  Median query latency [%s]: %sms" % (q.name, formatted_median))
      else:
        print("Could not determine CPU usage from metrics store")

  def report_total_times(self, store):
    self.print_header("Total times:")
    print('  Indexing time      : %.1f min' % convert.ms_to_minutes(store.get_one("indexing_total_time")))
    print('  Merge time         : %.1f min' % convert.ms_to_minutes(store.get_one("merges_total_time")))
    print('  Refresh time       : %.1f min' % convert.ms_to_minutes(store.get_one("refresh_total_time")))
    print('  Flush time         : %.1f min' % convert.ms_to_minutes(store.get_one("flush_total_time")))
    print('  Merge throttle time: %.1f min' % convert.ms_to_minutes(store.get_one("merges_total_throttled_time")))
    print("")

  def report_cpu_usage(self, store):
    percentages = store.get("cpu_utilization_1s")
    if percentages:
      #TODO dm: Output percentiles, not the median...
      formatted_median = '%.1f' % statistics.median(percentages)
      print("  Median indexing CPU utilization: %s%%" % formatted_median)
    else:
      print("Could not determine CPU usage from metrics store")

  def report_gc_times(self, store):
    young_gc_time = store.get_one("node_total_young_gen_gc_time")
    old_gc_time = store.get_one("node_total_old_gen_gc_time")
    print("  Total time spent in young gen GC: %.5fs" % convert.ms_to_seconds(young_gc_time))
    print("  Total time spent in old gen GC: %.5fs" % convert.ms_to_seconds(old_gc_time))

  def report_disk_usage(self, store):
    index_size = store.get_one("final_index_size_bytes")
    bytes_written = store.get_one("disk_io_write_bytes")
    print("  Final index size: %.1fGB (%.1fMB)" % (convert.bytes_to_gb(index_size), convert.bytes_to_mb(index_size)))
    print("  Totally written: %.1fGB (%.1fMB)" % (convert.bytes_to_gb(bytes_written), convert.bytes_to_mb(bytes_written)))

  def report_segment_memory(self, store):
    print("  Total heap used for segments     : %.2fMB" % self._mb(store, "segments_memory_in_bytes"))
    print('  Total heap used for doc values   : %.2fMB' % self._mb(store, "segments_doc_values_memory_in_bytes"))
    print('  Total heap used for terms        : %.2fMB' % self._mb(store, "segments_terms_memory_in_bytes"))
    print('  Total heap used for norms        : %.2fMB' % self._mb(store, "segments_norms_memory_in_bytes"))
    print('  Total heap used for stored fields: %.2fMB' % self._mb(store, "segments_stored_fields_memory_in_bytes"))

  def _mb(self, store, key):
    return convert.bytes_to_mb(store.get_one(key))

  def report_segment_counts(self, store):
    print("  Index segment count: %s" % store.get_one("segments_count"))

  def report_stats_times(self, store):
    self.print_header("Stats request latency:")
    print("  Indices stats: %.2fms" % store.get_one("indices_stats_latency"))
    print("  Nodes stats: %.2fms" % store.get_one("node_stats_latency"))
    print("")
