import tabulate

from esrally.utils import sysstats


def list_cars():
    print("Available cars:\n")
    print(tabulate.tabulate([[c.name] for c in cars], headers=["Name"]))


mergePartsLogConfig = '''
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
    Car(name="verbose_iw", logging_config=mergePartsLogConfig)
]
