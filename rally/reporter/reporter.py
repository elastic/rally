import bisect
import json
import os
import shutil
import re
import datetime

import utils.io

"""
Parses the log files and creates a dygraph.
"""

#TODO dm: Can we separate data and output format better? Use some kind of template engine and just transform the metrics structure here?

KNOWN_CHANGES = (
  ('2014-03-18', 'Switch to SerialMergeScheduler by default'),
  ('2014-04-15', 'Switch back to ConcurrentMergeScheduler'),
  ('2014-04-25', 'Don\'t lookup version for auto generated id and create'),
  ('2014-04-26', 'Upgraded to Ubuntu 14.04 LTS (kernel 3.13.0-32-generic #57)'),
  ('2014-05-13', 'Install broken with NoClassDefFoundError[com/tdunning/math/stats/TDigest]'),
  ('2014-05-20', 'Add index throttling when merges fall behind'),
  ('2014-07-10', 'Switch to official Elasticsearch Python client'),
  ('2014-07-24', 'Don\'t load bloom filters by default', 'FastUpdate'),
  #('2014-07-26', 'Disabled transparent huge pages'),
  #('2014-08-31', 'Re-enabled transparent huge pages'),
  ('2014-09-03', 'Switch auto id from random UUIDs to Flake IDs'),
  ('2014-09-06', 'Upgrade to Lucene 4.10'),
  ('2014-11-25', 'Workaround for LUCENE-6094, so ES shuts down quickly'),
  ('2014-12-13', 'Enable compound-file-format by default (#8934)'),
  ('2015-01-17', 'Switch to Lucene\'s auto-io-throttling for merge rate limiting'),
  ('2015-01-30', 'Disable auto-generated-ID index optimization (index.optimize_auto_generated_id=false)'),
  #('2015-01-29', 'Fixed bug in FastUpdates run that was deleting much more than the target 25% documents', 'FastUpdate'),
  #('2015-02-05', 'Index raw (not_analyzed) for all string fields'),
  ('2015-02-28', 'Upgrade Lucene to include LUCENE-6260 (Simplify ExactPhraseScorer)', 'Phrase (msec)'),
  ('2015-03-10', 'Upgraded JDK from 1.8.0_25-b17 to 1.8.0_40-b25'),
  ('2015-03-23-05-00-00', 'Upgrade to Jackson 2.5.1 (git show 9f9ada4)'),
  ('2015-03-24', 'Remove garbage-creating dead code (git show f34c6b7)'),
  ('2015-03-28', 'Enable doc-values by default (#10209)'),
  ('2015-05-01', 'Do not index doc values for _field_names field (#10893)'),
  # no longer true, because of back-testing:
  #('2015-05-05', 'Increase bulk index size from 500 docs to 5000 docs'),
  ('2015-05-08', 'Make modifying operations durable by default (#11011)'),
  ('2015-08-30', 'Transport shard level actions by node (#12944)'),
  ('2015-09-04', 'Upgrade to Lucene 5.4-snapshot-r1701068'),
  ('2015-09-04', 'Upgrade Lucene to include LUCENE-6756 (Give MatchAllDocsQuery a specialized BulkScorer)', 'Default (msec)'),
  ('2015-10-05', 'Randomize what time of day the benchmark runs'),
  ('2015-10-14', 'Use 30s refresh_interval instead of default 1s', 'Fast'),
)

KNOWN_CHANGES_IN_BUILD = (
  ('2015-05-05', 'Fix security manager so libsigar can run (git show dbcdb4)'),
  ('2015-05-23', 'Add boolean flag to enable/disable sigar'),
  ('2015-06-7', 'Fold plugins into master build (multi-modules)'),
  ('2015-06-10', 'Add more plugins into master build'),
  ('2015-08-19', 'Run "mvn clean verify" instead of "mvn clean test"'),
  ('2015-11-01', 'Switch from maven to gradle'),
  )


class Reporter:
  def __init__(self, config):
    self._config = config

  def report(self, tracks):
    byMode = {}
    allTimes = set()

    for track in tracks:
      d = {}
      for tup in self.loadSeries(track.name()):
        allTimes.add(tup[0])
        d[tup[0]] = tup[1:]
      byMode[track.name()] = d

    allTimes = list(allTimes)
    allTimes.sort()

    base_dir = self._config.opts("reporting", "report.base.dir")
    file_name = self._config.opts("reporting", "output.html.report.filename")

    # TODO dm: This will be too invasive if create an archive of reports later
    if os.path.exists(base_dir):
      shutil.rmtree(base_dir)
    utils.io.ensure_dir(base_dir)
    shutil.copyfile("%s/reporter/dygraph-combined.js" % script_dir, "%s/dygraph-combined.js" % base_dir)
    # TODO dm: We don't get too fancy for now later we should have some kind of archive structure in place...
    full_output_path = "%s/%s" % (base_dir, file_name)


    with open(full_output_path, 'w') as f:
      #TODO dm: Consider inlining dygraph-combined.js or at least shipping it somehow...
      f.write('''
      <!DOCTYPE html>
      <html lang="en">
      <head>
      <meta charset="utf-8">
      <meta http-equiv="content-type" content="text/html; charset=UTF-8">
      <title>[Elasticsearch Nightly Benchmarks]</title>
      <script type="text/javascript"
        src="dygraph-combined.js"></script>
      </head>
      <body>
      <p>Below are the results of the Elasticsearch nightly benchmark runs. The Apache Software Foundation also provides a similar page for the <a href="https://people.apache.org/~mikemccand/lucenebench/">Lucene nightly benchmarks</a>.</p>
      <h2>Results</h2>
      <table><tr>
      <td><div id="chart" style="width:800px; height:500px"></div></td>
      <td>\n\n<br><br><div id="chart_labels" style="width:250px; height:500px"></div></td>
      </tr></table>
      <script type="text/javascript">
      function zp(num,count) {
       var ret = num + '';
       while(ret.length < count) {
         ret = "0" + ret;
       }
       return ret;
     }

     function onClick(ev, msec, pts) {

       // Strange that I have to do this:
       var minIndex = -1;
       for(i=0;i<pts.length;i++) {
         if (minIndex == -1 || Math.abs(pts[i].canvasy - ev.offsetY) < Math.abs(pts[minIndex].canvasy - ev.offsetY)) {
           minIndex = i;
         }
       }

       var d = new Date(msec);
       var name = pts[minIndex].name

       // Reverse the mapping we did when generating the graph:
       if (name == "Defaults") {
         mode = "defaults";
       } else if (name == "Defaults (4G heap)") {
         mode = "4gheap";
       } else if (name == "Fast") {
         mode = "fastsettings";
       } else if (name == "FastUpdate") {
         mode = "fastupdates";
       } else if (name == "Defaults (2 nodes)") {
         mode = "two_nodes_defaults";
       } else if (name == "EC2 i2.2xlarge Defaults 4G") {
         mode = "ec2.i2.2xlarge";
       } else {
         mode = "unknown";
       }

       var s = "logs/" + mode + "/" + d.getFullYear() + "-" + zp(1+d.getMonth(), 2) + "-" + zp(d.getDate(), 2);
       if (d.getHours() != 0 || d.getMinutes() != 0 || d.getSeconds() != 0) {
         s += "-" + zp(d.getHours(), 2) + "-" + zp(d.getMinutes(), 2) + "-" + zp(d.getSeconds(), 2);
       }
       s += ".txt";
       // console.log(s);
       top.location = s;
     }
        g = new Dygraph(

          // containing div
          document.getElementById("chart"),
      ''')
      #TODO dm: Be very wary of the order here!!! - see similar comment there (see also logging_track.py)
      # Indexing docs/sec:
      headers = ['Date', 'Defaults', 'Defaults (4G heap)', 'Fast', 'FastUpdate', 'Defaults (2 nodes)', 'EC2 i2.2xlarge Defaults 4G']
      f.write('    "%s\\n"\n' % ','.join(headers))

      chartTimes = []
      for timeStamp in allTimes:
        l = []
        any = False
        for track in tracks:
          if timeStamp in byMode[track.name()]:
            dps, indexKB, segCount = byMode[track.name()][timeStamp][:3]
            if dps is None:
              dps = ''
            else:
              dps = '%.3f' % (dps/1000.0)
              any = True
          else:
            dps = ''
          l.append(dps)
        if any:
          chartTimes.append(timeStamp)

        f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

      f.write(''',
      { "title": "Nightly indexing performance on master",
        "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
        "includeZero": true,
        "xlabel": "Date",
        "ylabel": "K Docs/sec",
        "connectSeparatedPoints": true,
        "hideOverlayOnMouseOut": false,
        "labelsDiv": "chart_labels",
        "labelsSeparateLines": true,
        "legend": "always",
        "clickCallback": onClick,
        labelsDivStyles: {"background-color": "transparent"},
        showRoller: true,
        }
        );
        ''')

      self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Defaults (4G heap)')

      f.write('</script>')

      self.writeCPUPercent(f, byMode, allTimes)

      # Final disk usage and total bytes written to disk:
      f.write('\n\n<br><br>')
      f.write('''
      <table><tr>
      <td><div id="chart_disk_usage" style="width:800px; height:500px"></div></td>
      <td><div id="chart_disk_usage_labels" style="width:250px; height:500px"></div></td>
      </tr></table>
      <script type="text/javascript">

        g = new Dygraph(

          // containing div
          document.getElementById("chart_disk_usage"),
    ''')

      headers = ['Date', 'Final index size', 'Total bytes written']
      f.write('    "%s\\n"\n' % ','.join(headers))

      chartTimes = []
      for timeStamp in allTimes:
        l = []
        any = False
        # TODO dm: Mentions a specific benchmark -> abstraction!!
        for mode in 'defaults',:
          if timeStamp in byMode[mode]:
            indexKB = byMode[mode][timeStamp][1]
            if indexKB is not None:
              l.append('%.1f' % (indexKB / 1024 / 1024.))
              any = True
            else:
              l.append('')
              # TODO dm: Mentions a specific benchmark -> abstraction!!
            bytesWritten = byMode['defaults'][timeStamp][9]
            if bytesWritten is not None:
              x = '%.2f' % (bytesWritten/1024./1024./1024.)
              any = True
            else:
              x = ''
            l.append(x)
          else:
            l.append('')
            l.append('')
        if any:
          chartTimes.append(timeStamp)
        f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

      f.write(''',
      { "title": "Indexing disk usage",
        "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
        "includeZero": true,
        "xlabel": "Date",
        "ylabel": "GB",
        "connectSeparatedPoints": true,
        "hideOverlayOnMouseOut": false,
        "labelsDiv": "chart_disk_usage_labels",
        "labelsSeparateLines": true,
        "legend": "always",
        }
        );
        ''')

      self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Final index size')
      f.write('</script>')


      # Seg counts:
      f.write('\n\n<br><br>')
      f.write('''
      <table><tr>
      <td><div id="chart_seg_counts" style="width:800px; height:500px"></div></td>
      <td><div id="chart_seg_counts_labels" style="width:250px; height:500px"></div></td>
      </tr></table>
      <script type="text/javascript">
        g = new Dygraph(

          // containing div
          document.getElementById("chart_seg_counts"),
    ''')

      headers = ['Date', 'Defaults', 'Defaults (4G heap)', 'Fast', 'FastUpdate']
      f.write('    "%s\\n"\n' % ','.join(headers))

      chartTimes = []
      for timeStamp in allTimes:
        l = []
        any = False
        # FIXME dm: Some benchmarks are missing here. Is this intended? Add missing ones?
        for mode in 'defaults', '4gheap', 'fastsettings', 'fastupdates':
          if timeStamp in byMode[mode]:
            dps, indexKB, segCount = byMode[mode][timeStamp][:3]
            if segCount is not None:
              any = True
              x = '%d' % segCount
            else:
              x = ''
          else:
            x = ''
          l.append(x)
        if any:
          chartTimes.append(timeStamp)
        f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

      f.write(''',
      { "title": "Index segment counts",
        "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
        "includeZero": true,
        "xlabel": "Date",
        "ylabel": "Segment count",
        "connectSeparatedPoints": true,
        "hideOverlayOnMouseOut": false,
        "labelsDiv": "chart_seg_counts_labels",
        "labelsSeparateLines": true,
        "legend": "always",
        }
        );
        ''')

      self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Defaults (4G heap)')

      f.write('</script>')

      # 'gradle assemble' time
      self.writeEsBuildTimeGraph(f)

      # default (match all) search time
      self.writeSearchTimesGraph(f, byMode, allTimes)

      # indices/nodes stats time
      self.writeStatsTimeGraph(f, byMode, allTimes)

      # GC times
      self.writeGCTimesGraph(f, byMode, allTimes)

      f.write('''
      <h2>Benchmark Scenarios</h2>
      <p>This test indexes 6.9M short documents (log lines, total 14 GB json) using 8 client threads and 500 docs per _bulk request against a single node running on a dual Xeon X5680 (12 real cores, 24 with hyperthreading) and 48 GB RAM. </p>
      <p><tt>Defaults, 2 nodes</tt> is append-only, using all default settings, but runs 2 nodes on 1 box (5 shards, 1 replica).</p>
      <p><tt>Defaults</tt> is append-only, using all default settings.</p>
      <p><tt>Defaults (4G heap)</tt> is the same as <code>Defaults</code> except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.</p>
      <p><tt>Fast</tt> is append-only, using 4 GB heap, and these settings:
      <pre>
      refresh_interval: 30s
      index.store.throttle.type: none
      indices.store.throttle.type: none

      index.number_of_shards: 6
      index.number_of_replicas: 0

      index.translog.flush_threshold_size: 4g
      index.translog.flush_threshold_ops: 500000
      </pre>
      </p>
      <p><tt>FastUpdate</tt> is the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID already exists in the index.</p>
      </body>
      </html>
      ''')

      f.close()

      # TODO dm: copy dygraph-combined.js!!



      #TODO dm: Copying to ec2 is not yet supported (Prio 1) (for migration)
      #TODO dm: Ensure that clicking on logs also works locally (not just for the EC2 version) (low prio)
      #if '-copy' in sys.argv:
      #  cmd = 's3cmd put -P %s s3://benchmarks.elasticsearch.org/index.html' % constants.GRAPH_OUT_FILE_NAME
      #  if os.system(cmd):
      #    raise RuntimeError('"%s" failed' % cmd)
      #  for mode in '4gheap', 'fastsettings', 'fastupdates', 'defaults', 'two_nodes_defaults', 'ec2.i2.2xlarge':
      #    cmd = 's3cmd sync -P %s/%s/ s3://benchmarks.elasticsearch.org/logs/%s/' % (constants.LOGS_DIR, mode, mode)
      #  if os.system(cmd):
      #    raise RuntimeError('"%s" failed' % cmd)

  def _log_dir(self):
    #TODO dm: No this is not nice and also not really correct, we should separate the build logs from regular ones -> later (see also metrics.py)
    return self._build_log_dir()

  def _build_log_dir(self):
    return self._config.opts("build", "log.dir")

  def toString(self, timeStamp):
    return '%04d-%02d-%02d %02d:%02d:%02d' % \
                      (timeStamp.year,
                       timeStamp.month,
                       timeStamp.day,
                       timeStamp.hour,
                       timeStamp.minute,
                       int(timeStamp.second))

  def yearMonthDay(self, timeStamp):
    return timeStamp.year, timeStamp.month, timeStamp.day

  def getLabel(self, label):
    if label < 26:
      s = chr(65+label)
    else:
      s = '%s%s' % (chr(65+(label/26 - 1)), chr(65 + (label%26)))
    return s

  def loadOneFile(self, fileName, m):
    oome = False
    dps = None
    exc = False
    indexKB = None
    segCount = None
    searchTimes = {}
    nodesStatsMSec = None
    indicesStatsMSec = None
    youngGCSec = None
    oldGCSec = None
    cpuPct = None
    bytesWritten = None

    with open(fileName) as f:
      while True:
        line = f.readline()
        if line == '':
          break
        if line.find('java.lang.OutOfMemoryError') != -1 or line.find('abort: OOME') != -1:
          oome = True
        if line.startswith('Total docs/sec: '):
          dps = float(line[16:].strip())
        if line.startswith('index size '):
          indexKB = int(line[11:].strip().replace(' KB', ''))
        if line.find('.py", line') != -1:
          exc = True
        if line.startswith('Indices stats took'):
          indicesStatsMSec = float(line.strip().split()[3])
        if line.startswith('Node stats took'):
          nodesStatsMSec = float(line.strip().split()[3])
        if line.startswith('CPU median: '):
          cpuPct = float(line.strip()[12:])
        if line.startswith('WRITES: '):
          bytesWritten = int(line[8:].split()[0])

        if line.startswith('STATS: ') or line.startswith('INDICES STATS:'):
          d = self.parseStats(line, f)
          segCount = d['_all']['primaries']['segments']['count']

        if line.startswith('NODES STATS: ') and '/defaults/' in fileName:
          d = self.parseStats(line, f)
          nodes = d['nodes']
          if len(nodes) != 1:
            raise RuntimeError('expected one node but got %s' % len(nodes))
          node = list(nodes.keys())[0]
          node = nodes[node]
          d = node['jvm']['gc']['collectors']
          oldGCSec = d['old']['collection_time_in_millis']/1000.0
          youngGCSec = d['young']['collection_time_in_millis']/1000.0

        if line.startswith('SEARCH ') and ' (median): ' in line:
          searchType = line[7:line.find(' (median): ')]
          t = float(line.strip().split()[-2])
          searchTimes[searchType] = t
          #print('search time %s: %s = %s sec' % (fileName, searchType, t))

    # print('%s: oldGC=%s newGC=%s' % (fileName, oldGCSec, youngGCSec))

    if oome or exc or dps is None:
      # dps = 1
      print('WARNING: exception/oome/no dps in log "%s"; dropping data point' % fileName)
      return None

    if indexKB is None:
      if '4gheap' in fileName:
        print('WARNING: missing index size for log "%s"; dropping data point' % fileName)
        return None
      else:
        indexKB = 1

    year = int(m.group(1))
    month = int(m.group(2))
    day = int(m.group(3))
    if len(m.groups()) == 6:
      hour = int(m.group(4))
      minute = int(m.group(5))
      second = int(m.group(6))
    else:
      hour = 0
      minute = 0
      second = 0

    if (year, month, day) in (
      # Installer was broken?
      (2014, 5, 13),
      # Something went badly wrong:
      (2015, 6, 2)):
      return None

    return datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second), dps, indexKB, segCount, searchTimes, indicesStatsMSec, nodesStatsMSec, oldGCSec, youngGCSec, cpuPct, bytesWritten

  def parseStats(self, line, f):
    x = line.find('STATS: ')
    s = line[x+7:].strip()
    if s == '{':
      # Handle pretty-printed node stats:
      statsLines = [s]
      while True:
        line = f.readline()
        if line == '':
          raise RuntimeError('hit EOF trying to parse node stats output')
        statsLines.append(line)
        if line.rstrip() == '}':
          break
        if line.startswith('}node1: [GC') or line.startswith('}node0: [GC'):
          statsLines[-1] = '}'
          break
      d = json.loads(''.join(statsLines))
    else:
      idx = s.find('node1: [GC')
      if idx != -1:
        s = s[:idx]
      d = eval(s)
    return d

  def loadSeries(self, subdir):
    results = []

    reYMD = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)\.txt$')
    reYMDNew = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)\.txt.new$')
    reYMDHMS = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)\.txt$')

    rootDir = '%s/%s/' % (self._log_dir(), subdir)

    newSeen = set()
    l = []
    try:
      l = os.listdir(rootDir)
    except FileNotFoundError:
      print("Warning %s does not exist. Skipping..." % rootDir)
      return results
    l.sort()
    for fileName in l:
      m = reYMD.match(fileName)
      if m is None:
        m = reYMDNew.match(fileName)
      if m is None:
        m = reYMDHMS.match(fileName)
      if m is not None:
        # Prefer the .new log if present, else fallback to old:
        if not fileName.endswith('.new') and os.path.exists('%s/%s.new' % (rootDir, fileName)):
          fileName = fileName + '.new'
        if fileName.endswith('.new'):
          if fileName in newSeen:
            continue
          newSeen.add(fileName)
        try:
          tup = self.loadOneFile('%s%s' % (rootDir, fileName), m)
        except:
          print('FAILED: exception while parsing %s%s' % (rootDir, fileName))
          raise
        if tup is not None:
          d = tup[0]

          if d <= datetime.datetime(year=2014, month=4, day=15):
            # For some reason, before this, the number of indexed docs in the end != the number we had indexed, so we can't back-test prior to this
            continue

          if d >= datetime.datetime(year=2014, month=8, day=7) and \
             d <= datetime.datetime(year=2014, month=8, day=18):
            # #6939 cause a bug on 8/6 in strict mappings, not fixed until #7304 on 8/19
            continue

          results.append(tup)

    # Sort by date:
    results.sort()

    return results

  def writeSearchTimesGraph(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    f.write('''
    <table><tr>
    <td><div id="chart_search_times" style="width:800px; height:500px"></div></td>
    <td><div id="chart_search_times_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_search_times"),
  ''')

    searchTypes = set()
    for timeStamp in allTimes:
      if timeStamp in byMode['defaults']:
        dps, indexKB, segCount, searchTimes = byMode['defaults'][timeStamp][:4]
        searchTypes.update(searchTimes.keys())

    searchTypes = list(searchTypes)

    headers = ['Date']

    pretty = {'term': 'Term',
              'phrase': 'Phrase',
              'default': 'Default',
              'scroll_all': 'Scroll All',
              'hourly_agg': 'Aggs (hourly timeStamp)'}

    for searchType in searchTypes:
      headers.append('%s (msec)' % pretty.get(searchType, searchType))
    f.write('    "%s\\n"\n' % ','.join(headers))

    chartTimes = []

    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults']:
        searchTimes = byMode['defaults'][timeStamp][3]
      else:
        searchTimes = {}
      any = False
      for searchType in searchTypes:
        t = searchTimes.get(searchType)
        if t is None:
          x = ''
        else:
          x = '%.2f' % (1000.0*t)
          any = True
        l.append(x)
      if any:
        chartTimes.append(timeStamp)
      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    f.write(''',
    { "title": "Search times",
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "Milliseconds",
      "connectSeparatedPoints": true,
      "hideOverlayOnMouseOut": false,
      "labelsDiv": "chart_search_times_labels",
      "labelsSeparateLines": true,
      "legend": "always"
      }
      );
      ''')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Term (msec)')

    f.write('</script>')

  def writeStatsTimeGraph(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    f.write('''
    <table><tr>
    <td><div id="chart_stats_time" style="width:800px; height:500px"></div></td>
    <td><div id="chart_stats_time_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_stats_time"),
  ''')

    headers = ['Date', 'Indices stats (msec)', 'Nodes stats (msec)']
    f.write('    "%s\\n"\n' % ','.join(headers))

    chartTimes = []
    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults']:
        dps, indexKB, segCount, defaultSearchSec, indicesStatsMSec, nodesStatsMSec = byMode['defaults'][timeStamp][:6]
        if indicesStatsMSec is not None:
          x = '%.5f' % indicesStatsMSec
        else:
          x = ''
        l.append(x)
        if nodesStatsMSec is not None:
          x = '%.5f' % nodesStatsMSec
        else:
          x = ''
        l.append(x)
      else:
        l.append('')
        l.append('')
      if l != ['', '']:
        chartTimes.append(timeStamp)
      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    f.write(''',
    { "title": "Stats request time",
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "Milliseconds",
      "connectSeparatedPoints": true,
      "hideOverlayOnMouseOut": false,
      "labelsDiv": "chart_stats_time_labels",
      "labelsSeparateLines": true,
      "legend": "always",
      }
      );
      ''')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Indices stats (msec)')

    f.write('</script>')

  def writeCPUPercent(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    f.write('''
    <table><tr>
    <td><div id="chart_cpu" style="width:800px; height:500px"></div></td>
    <td><div id="chart_cpu_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_cpu"),
  ''')

    headers = ['Date', 'Defaults (%)']
    f.write('    "%s\\n"\n' % ','.join(headers))

    chartTimes = []
    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults']:
        cpuPct = byMode['defaults'][timeStamp][8]
        if cpuPct is not None:
          x = '%.1f' % cpuPct
        else:
          x = ''
        l.append(x)
      else:
        l.append('')
      if l != ['']:
        chartTimes.append(timeStamp)
      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    f.write(''',
    { "title": "Indexing CPU utilization (defaults)",
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "CPU (median %)",
      "connectSeparatedPoints": true,
      "hideOverlayOnMouseOut": false,
      "labelsDiv": "chart_cpu_labels",
      "labelsSeparateLines": true,
      "legend": "always",
      }
      );
      ''')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Defaults (%)')

    f.write('</script>')

  def writeGCTimesGraph(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    f.write('''
    <table><tr>
    <td><div id="chart_gc_time" style="width:800px; height:500px"></div></td>
    <td><div id="chart_gc_time_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_gc_time"),
  ''')

    headers = ['Date', 'GC young gen (sec)', 'GC old gen (sec)']
    f.write('    "%s\\n"\n' % ','.join(headers))

    chartTimes = []

    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults'] and self.yearMonthDay(timeStamp) not in ((2014, 5, 18), (2014, 5, 19)):
        dps, indexKB, segCount, defaultSearchSec, indicesStatsMSec, nodesStatsMSec, oldGCSec, youngGCSec = byMode['defaults'][timeStamp][:8]
        if youngGCSec is not None:
          x = '%.5f' % youngGCSec
        else:
          x = ''
        l.append(x)
        if oldGCSec is not None:
          x = '%.5f' % oldGCSec
        else:
          x = ''
        l.append(x)
      else:
        l.append('')
        l.append('')
      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))
      if l != ['', '']:
        chartTimes.append(timeStamp)

    f.write(''',
    { "title": "GC times",
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "Seconds",
      "connectSeparatedPoints": true,
      "hideOverlayOnMouseOut": false,
      "labelsDiv": "chart_gc_time_labels",
      "labelsSeparateLines": true,
      "legend": "always",
      }
      );
      ''')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'GC young gen (sec)')

    f.write('</script>')

  def writeAnnots(self, f, chartTimes, annots, defaultSeries):
    if len(chartTimes) == 0:
      return

    # First, just aggregate by the timeStamp we need to attach each annot to, so in
    # case more than one change lands on one point, we can show all of them under a
    # single annot:

    firstTimeStamp = chartTimes[0]

    label = 0
    byTimeStamp = {}
    for annot in annots:
      date, reason = annot[:2]
      if len(annot) > 2:
        series = annot[2]
      else:
        series = defaultSeries

      parts = list(int(x) for x in date.split('-'))
      year, month, day = parts[:3]
      if len(parts) == 6:
        hour, min, sec = parts[3:]
      elif len(parts) != 3:
        raise RuntimeError('invalid timestamp "%s": should be YYYY-MM-DD[-HH-MM-SS]' % date)
      else:
        hour = min = sec = 0
      timeStamp = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=sec)

      if timeStamp < firstTimeStamp:
        # If this annot is from before this chart started, skip it:
        continue

      # Place the annot on the next data point that's <= the annot timestamp, in this chart:
      idx = bisect.bisect_left(chartTimes, timeStamp)
      if idx is not None:
        if idx not in byTimeStamp:
          byTimeStamp[idx] = {}
        if series not in byTimeStamp[idx]:
          byTimeStamp[idx][series] = [label]
          label += 1
        byTimeStamp[idx][series].append(reason)

    # Then render the annots:
    f.write('g.ready(function() {g.setAnnotations([')
    for idx, d in byTimeStamp.items():
      timeStamp = chartTimes[idx]
      for series, items in d.items():
        messages = r'\n\n'.join(items[1:])
        f.write('{series: "%s", x: "%s", shortText: "%s", text: "%s"},\n' % \
                (series, self.toString(timeStamp), self.getLabel(items[0]), messages.replace('"', '\\"')))
    f.write(']);});\n')

  def writeEsBuildTimeGraph(self, f):
    # Load/parse "gradle assemble" times:
    results = []
    reYMD = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)\.txt$')
    reYMDHMS = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)\.txt$')
    l = []
    buildlogDirectory = self._build_log_dir()
    try:
      l = os.listdir(buildlogDirectory)
    except FileNotFoundError:
      print("Warning %s does not exist. Skipping build time stats..." % buildlogDirectory)
    l.sort()
    for fileName in l:
      m = reYMD.match(fileName)
      if m is None:
        m = reYMDHMS.match(fileName)
      if m is not None:
        #print("%s" % fileName)
        with open('%s/%s' % (buildlogDirectory, fileName), 'r') as fIn:
          success = False
          minutes = None
          #TODO dm: (a) we're just interested in the result -> read the file backwards (b) have a separate metrics collector intead of the log file
          for line in fIn.readlines():
            if "BUILD SUCCESS" in line:
              success = True
            i = line.find('Total time:')
            if i != -1:
              if 'secs' in line:
                # Gradle:
                tup = line[i+11:].strip().split()
                if len(tup) != 4:
                  raise RuntimeError('unexpected time: %s' % line)
                if tup[1] != 'mins' or tup[3] != 'secs':
                  raise RuntimeError('unexpected time: %s' % line)
                minutes = int(tup[0]) + float(tup[2])/60.
              else:
                s = line[i+11:].replace(' min', '').strip()
                tup = s.split(':')
                if len(tup) != 2:
                  raise RuntimeError('unexpected time %s' % s)
                minutes = int(tup[0]) + int(tup[1])/60.
                #print('  %s' % minutes)

          if success:
            if minutes is not None:
              year = int(m.group(1))
              month = int(m.group(2))
              day = int(m.group(3))
              if len(m.groups()) == 6:
                hour = int(m.group(4))
                minute = int(m.group(5))
                second = int(m.group(6))
              else:
                hour = minute = second = 0
              d = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
              results.append((d, minutes))
            else:
              raise RuntimeError('build success but could not find total time')

    results.sort()

    f.write('''
    \n\n<br><br>
    <a name="buildtime"></a>
    <table><tr>
    <td><div id="chart_build_time" style="width:800px; height:500px"></div></td>
    <td><div id="chart_build_time_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_build_time"),
    ''')

    headers = ['Date', 'Time (minutes)']
    f.write('    "%s\\n"\n' % ','.join(headers))

    for date, minutes in results:
      f.write('    + "%4d-%02d-%02d %02d:%02d:%02d,%s\\n"\n' % (date.year, date.month, date.day, date.hour, date.minute, int(date.second), minutes))

    f.write(''',
    { "title": "Time for \'gradle assemble\'",
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "Minutes",
      "connectSeparatedPoints": true,
      "hideOverlayOnMouseOut": false,
      "labelsDiv": "chart_build_time_labels",
      "labelsSeparateLines": true,
      "legend": "always",
      }
      );
      ''')

    self.writeAnnots(f, list(x[0] for x in results), KNOWN_CHANGES_IN_BUILD, 'Time (minutes)')

    f.write('</script>')
