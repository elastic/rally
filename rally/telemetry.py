import logging

logger = logging.getLogger("rally.telemetry")

class Telemetry:
  def __init__(self, config):
    self._config = config
    self._devices = [FlightRecorder(config)]

  def list(self):
    print("Available telemetry devices:")
    for device in self._devices:
      print("\t%s (%s): %s" % (device.command, device.human_name, device.help))
    print("\nKeep in mind that each telemetry devices may incur a runtime overhead which can skew results.")

  def install(self, setup):
    enabled_devices = [e.strip() for e in self._config.opts("telemetry", "devices").split(",")]
    opts = {}
    for device in self._devices:
      if device.command in enabled_devices:
        # TODO dm: The maps need to be properly merged, otherwise the overwrite each other when we really have multiple devices...
        opts.update(device.install(setup))
    return opts

########################################################################################
#
# Telemetry devices
#
########################################################################################

class FlightRecorder:
  def __init__(self, config):
    self._config = config

  @property
  def command(self):
    return "jfr"

  @property
  def human_name(self):
    return "Flight Recorder"

  @property
  def help(self):
    return "Enables Java Flight Recorder on the benchmark candidate (will only work on Oracle JDK)"

  def install(self, setup):
    log_root = "%s/%s" % (self._config.opts("system", "log.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
    log_file = "%s/%s.jfr" % (log_root, setup.name)

    logger.info("%s profiler: Writing telemetry data to [%s]." % (self.human_name, log_file))
    print("%s: writing flight recording to %s" % (self.human_name, log_file))
    # TODO dm: Consider using also a delay, starting flight recording only after a specific trigger (e.g. warmup is done), etc., etc.
    # TODO dm: We should probably put the file name in quotes or escape it properly somehow (if there are spaces in the path...)
    return {"ES_JAVA_OPTS": "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder "
                            "-XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=%s"% (log_file)}
