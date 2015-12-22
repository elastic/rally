import logging

import rally.metrics

logger = logging.getLogger("rally.reporting")


class SummaryReporter:
  def __init__(self, config):
    self._config = config

  def report(self, track):
    self.print_header("------------------------------------------------------")
    self.print_header("---------- EXPERIMENTAL - With Metrics Store----------")
    self.print_header("------------------------------------------------------")
    self.print_header("    _______             __   _____                    ")
    self.print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
    self.print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
    self.print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
    self.print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ \n\n")
    self.print_header("------------------------------------------------------")

    selected_setups = self._config.opts("benchmarks", "tracksetups.selected")
    invocation = self._config.opts("meta", "time.start")
    for track_setup in track.track_setups:
      if track_setup.name in selected_setups:
        store = rally.metrics.MetricsStore(self._config, invocation, track, track_setup)
        store.open_for_read()

        self.print_header("System Metrics")
        self.writeCPUPercent(store)
        print("")

    self.print_header("------------------------------------------------------")
    self.print_header("---------- EXPERIMENTAL - With Metrics Store----------")
    self.print_header("------------------------------------------------------")


  def print_header(self, message):
    print("\033[1m%s\033[0m" % message)

  def writeCPUPercent(self, store):
    # TODO dm: Actually, we should only do this for "default"...
    percentages = store.get("cpu_utilization_1s", value_converter=float)
    # TOOD dm: Output percentiles, not the median...
    if percentages:
      percentages = sorted(percentages)
      median = percentages[int(len(percentages) / 2)]
      formatted_median = '%.1f' % median
      print("  Median indexing CPU utilization: %s%%" % formatted_median)
    else:
      print("Could not determine CPU usage from metrics store")

