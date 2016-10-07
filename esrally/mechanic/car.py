import tabulate

from esrally import exceptions, PROGRAM_NAME
from esrally.utils import sysstats, console


def list_cars():
    console.println("Available cars:\n")
    console.println(tabulate.tabulate([[c.name] for c in cars], headers=["Name"]))


def select_car(cfg):
    name = cfg.opts("benchmarks", "car")
    for c in cars:
        if c.name == name:
            return c
    raise exceptions.SystemSetupError("Unknown car [%s]. You can list the available cars with %s list cars." % (name, PROGRAM_NAME))


mergePartsLogYmlConfig = '''
es.logger.level: INFO
rootLogger: ${es.logger.level}, console, file
logger:
  action: DEBUG
  com.amazonaws: WARN
  com.amazonaws.jmx.SdkMBeanRegistrySupport: ERROR
  com.amazonaws.metrics.AwsSdkMetrics: ERROR
  org.apache.http: INFO
  index.search.slowlog: TRACE, index_search_slow_log_file
  index.indexing.slowlog: TRACE, index_indexing_slow_log_file
  index.engine.lucene.iw: TRACE

additivity:
  index.search.slowlog: false
  index.indexing.slowlog: false
  deprecation: false

appender:
  console:
    type: console
    layout:
      type: consolePattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"

  file:
    type: dailyRollingFile
    file: ${path.logs}/${cluster.name}.log
    datePattern: "'.'yyyy-MM-dd"
    layout:
      type: pattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %.10000m%n"

  index_search_slow_log_file:
    type: dailyRollingFile
    file: ${path.logs}/${cluster.name}_index_search_slowlog.log
    datePattern: "'.'yyyy-MM-dd"
    layout:
      type: pattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"

  index_indexing_slow_log_file:
    type: dailyRollingFile
    file: ${path.logs}/${cluster.name}_index_indexing_slowlog.log
    datePattern: "'.'yyyy-MM-dd"
    layout:
      type: pattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"
'''

mergePartsLog4j2Config = '''
status = error

# log action execution errors for easier debugging
logger.action.name = action
logger.action.level = debug

appender.console.type = Console
appender.console.name = console
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = [%d{ISO8601}][%-5p][%-25c] %marker%m%n

appender.rolling.type = RollingFile
appender.rolling.name = rolling
appender.rolling.fileName = ${sys:es.logs}.log
appender.rolling.layout.type = PatternLayout
appender.rolling.layout.pattern = [%d{ISO8601}][%-5p][%-25c] %marker%.10000m%n
appender.rolling.filePattern = ${sys:es.logs}-%d{yyyy-MM-dd}.log
appender.rolling.policies.type = Policies
appender.rolling.policies.time.type = TimeBasedTriggeringPolicy
appender.rolling.policies.time.interval = 1
appender.rolling.policies.time.modulate = true

rootLogger.level = info
rootLogger.appenderRef.console.ref = console
rootLogger.appenderRef.rolling.ref = rolling

logger.verbose_iw.name = org.elasticsearch.index.engine.Engine.SM
logger.verbose_iw.level = trace
logger.verbose_iw.appenderRef.console.ref = console
logger.verbose_iw.appenderRef.rolling.ref = rolling
logger.verbose_iw.additivity = false

appender.deprecation_rolling.type = RollingFile
appender.deprecation_rolling.name = deprecation_rolling
appender.deprecation_rolling.fileName = ${sys:es.logs}_deprecation.log
appender.deprecation_rolling.layout.type = PatternLayout
appender.deprecation_rolling.layout.pattern = [%d{ISO8601}][%-5p][%-25c] %marker%.10000m%n
appender.deprecation_rolling.filePattern = ${sys:es.logs}_deprecation-%i.log.gz
appender.deprecation_rolling.policies.type = Policies
appender.deprecation_rolling.policies.size.type = SizeBasedTriggeringPolicy
appender.deprecation_rolling.policies.size.size = 1GB
appender.deprecation_rolling.strategy.type = DefaultRolloverStrategy
appender.deprecation_rolling.strategy.max = 4

logger.deprecation.name = deprecation
logger.deprecation.level = warn
logger.deprecation.appenderRef.deprecation_rolling.ref = deprecation_rolling
logger.deprecation.additivity = false

appender.index_search_slowlog_rolling.type = RollingFile
appender.index_search_slowlog_rolling.name = index_search_slowlog_rolling
appender.index_search_slowlog_rolling.fileName = ${sys:es.logs}_index_search_slowlog.log
appender.index_search_slowlog_rolling.layout.type = PatternLayout
appender.index_search_slowlog_rolling.layout.pattern = [%d{ISO8601}][%-5p][%-25c] %marker%.10000m%n
appender.index_search_slowlog_rolling.filePattern = ${sys:es.logs}_index_search_slowlog-%d{yyyy-MM-dd}.log
appender.index_search_slowlog_rolling.policies.type = Policies
appender.index_search_slowlog_rolling.policies.time.type = TimeBasedTriggeringPolicy
appender.index_search_slowlog_rolling.policies.time.interval = 1
appender.index_search_slowlog_rolling.policies.time.modulate = true

logger.index_search_slowlog_rolling.name = index.search.slowlog
logger.index_search_slowlog_rolling.level = trace
logger.index_search_slowlog_rolling.appenderRef.index_search_slowlog_rolling.ref = index_search_slowlog_rolling
logger.index_search_slowlog_rolling.additivity = false

appender.index_indexing_slowlog_rolling.type = RollingFile
appender.index_indexing_slowlog_rolling.name = index_indexing_slowlog_rolling
appender.index_indexing_slowlog_rolling.fileName = ${sys:es.logs}_index_indexing_slowlog.log
appender.index_indexing_slowlog_rolling.layout.type = PatternLayout
appender.index_indexing_slowlog_rolling.layout.pattern = [%d{ISO8601}][%-5p][%-25c] %marker%.10000m%n
appender.index_indexing_slowlog_rolling.filePattern = ${sys:es.logs}_index_indexing_slowlog-%d{yyyy-MM-dd}.log
appender.index_indexing_slowlog_rolling.policies.type = Policies
appender.index_indexing_slowlog_rolling.policies.time.type = TimeBasedTriggeringPolicy
appender.index_indexing_slowlog_rolling.policies.time.interval = 1
appender.index_indexing_slowlog_rolling.policies.time.modulate = true

logger.index_indexing_slowlog.name = index.indexing.slowlog.index
logger.index_indexing_slowlog.level = trace
logger.index_indexing_slowlog.appenderRef.index_indexing_slowlog_rolling.ref = index_indexing_slowlog_rolling
logger.index_indexing_slowlog.additivity = false
'''


class Car:
    def __init__(self, name, config_snippet=None, logging_config=None, nodes=1, processors=1, heap=None,
                 java_opts=None, gc_opts=None):
        """
        Creates new settings for a benchmark candidate.

        :param name: A descriptive name for this car.
        :param config_snippet: A string snippet that will be appended as is to elasticsearch.yml of the benchmark candidate.
        :param logging_config: A string representing the entire contents of logging.yml. If not set, the factory defaults will be used.
        :param nodes: The number of nodes to start. Defaults to 1 node. All nodes are started on the same machine.
        :param processors: The number of processors to use (per node).
        :param heap: A string defining the maximum amount of Java heap to use. For allows values, see the documentation on -Xmx
        in `<http://docs.oracle.com/javase/8/docs/technotes/tools/unix/java.html#BABHDABI>`
        :param java_opts: Additional Java options to pass to each node on startup.
        :param gc_opts: Additional garbage collector options to pass to each node on startup.
        """
        self.name = name
        self.custom_config_snippet = config_snippet
        self.custom_logging_config = logging_config
        self.nodes = nodes
        self.processors = processors
        self.heap = heap
        self.java_opts = java_opts
        self.gc_opts = gc_opts

    def __str__(self):
        return self.name


cars = [
    Car(name="defaults"),
    Car(name="4gheap", heap="4g"),
    Car(name="two_nodes", nodes=2, processors=sysstats.logical_cpu_cores() // 2),
    Car(name="verbose_iw", logging_config={"logging.yml": mergePartsLogYmlConfig, "log4j2.properties": mergePartsLog4j2Config})
]
