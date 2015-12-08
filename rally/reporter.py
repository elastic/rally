import bisect
import json
import os
import glob
import shutil
import re
import datetime
import pickle
import logging

import rally.utils.io
import rally.utils.format

logger = logging.getLogger("rally.reporting")

"""
Parses the log files and creates dygraphs.
"""

# TODO dm: Can we separate data and output format better? Use some kind of template engine and just transform the metrics structure here?

# TODO dm: This is very specific to the current nightlies -> should move to some build-specific postprocessing step
KNOWN_CHANGES = (
  ('2014-03-18', 'Switch to SerialMergeScheduler by default'),
  ('2014-04-15', 'Switch back to ConcurrentMergeScheduler'),
  ('2014-04-25', 'Don\'t lookup version for auto generated id and create'),
  ('2014-04-26', 'Upgraded to Ubuntu 14.04 LTS (kernel 3.13.0-32-generic #57)'),
  ('2014-05-13', 'Install broken with NoClassDefFoundError[com/tdunning/math/stats/TDigest]'),
  ('2014-05-20', 'Add index throttling when merges fall behind'),
  ('2014-07-10', 'Switch to official Elasticsearch Python client'),
  ('2014-07-24', 'Don\'t load bloom filters by default', 'FastUpdate'),
  # ('2014-07-26', 'Disabled transparent huge pages'),
  # ('2014-08-31', 'Re-enabled transparent huge pages'),
  ('2014-09-03', 'Switch auto id from random UUIDs to Flake IDs'),
  ('2014-09-06', 'Upgrade to Lucene 4.10'),
  ('2014-11-25', 'Workaround for LUCENE-6094, so ES shuts down quickly'),
  ('2014-12-13', 'Enable compound-file-format by default (#8934)'),
  ('2015-01-17', 'Switch to Lucene\'s auto-io-throttling for merge rate limiting'),
  ('2015-01-30', 'Disable auto-generated-ID index optimization (index.optimize_auto_generated_id=false)'),
  # ('2015-01-29', 'Fixed bug in FastUpdates run that was deleting much more than the target 25% documents', 'FastUpdate'),
  # ('2015-02-05', 'Index raw (not_analyzed) for all string fields'),
  ('2015-02-28', 'Upgrade Lucene to include LUCENE-6260 (Simplify ExactPhraseScorer)', 'Phrase (msec)'),
  ('2015-03-10', 'Upgraded JDK from 1.8.0_25-b17 to 1.8.0_40-b25'),
  ('2015-03-23-05-00-00', 'Upgrade to Jackson 2.5.1 (git show 9f9ada4)'),
  ('2015-03-24', 'Remove garbage-creating dead code (git show f34c6b7)'),
  ('2015-03-28', 'Enable doc-values by default (#10209)'),
  ('2015-05-01', 'Do not index doc values for _field_names field (#10893)'),
  # no longer true, because of back-testing:
  # ('2015-05-05', 'Increase bulk index size from 500 docs to 5000 docs'),
  ('2015-05-08', 'Make modifying operations durable by default (#11011)'),
  ('2015-08-30', 'Transport shard level actions by node (#12944)'),
  ('2015-09-04', 'Upgrade to Lucene 5.4-snapshot-r1701068'),
  ('2015-09-04', 'Upgrade Lucene to include LUCENE-6756 (Give MatchAllDocsQuery a specialized BulkScorer)', 'Default (msec)'),
  ('2015-10-05', 'Randomize what time of day the benchmark runs'),
  ('2015-10-14', 'Use 30s refresh_interval instead of default 1s', 'Fast'),
  ('2015-11-22', 'Benchmark uncovers LUCENE-6906 bug', 'FastUpdate'),
  ('2015-12-01-19-40-30', 'Upgrade to beast2 (72 cores, 256 GB RAM)'),
)

KNOWN_CHANGES_IN_BUILD = (
  ('2015-05-05', 'Fix security manager so libsigar can run (git show dbcdb4)'),
  ('2015-05-23', 'Add boolean flag to enable/disable sigar'),
  ('2015-06-7', 'Fold plugins into master build (multi-modules)'),
  ('2015-06-10', 'Add more plugins into master build'),
  ('2015-08-19', 'Run "mvn clean verify" instead of "mvn clean test"'),
  ('2015-11-01', 'Switch from maven to gradle'),
  ('2015-12-01-19-40-30', 'Upgrade to beast2 (72 cores, 256 GB RAM)'),
)


class Reporter:
  def __init__(self, config):
    self._config = config
    self._nextGraph = 0
    self._toPrettyName = {'defaults': 'Defaults',
                          '4gheap': 'Defaults (4G heap)',
                          'fastsettings': 'Fast',
                          'fastupdates': 'FastUpdate',
                          'two_nodes_defaults': 'Defaults (2 nodes)',
                          'ec2.i2.2xlarge': 'EC2 i2.2xlarge Defaults 4G',
                          'buildtimes': 'Test time (minutes)'}

  def report(self, track):
    self._nextGraph = 100

    #TODO dm: Beware, this will *not* work as soon as we have multiple benchmarks!
    # NOTE: turn this back on for faster iterations on just UI changes:
    if False and os.path.exists('results.pk'):
      with open('results.pk', 'rb') as f:
        byMode, byModeBroken, allTimes = pickle.load(f)
    else:
      byMode = {}
      byModeBroken = {}
      allTimes = set()

      for track_setup in track.track_setups:
        track_setup_name = track_setup.name
        d = {}
        lastTup = None
        # Dates that had a failed run for this series:
        broken = set()
        logger.info('Loading %s...' % self._toPrettyName[track_setup_name])
        for tup in self.loadSeries(track_setup_name):
          timeStamp = tup[0]
          allTimes.add(timeStamp)
          if len(tup) == 2 or tup[1] is None:
            broken.add(timeStamp)
            if lastTup is None:
              continue
            tup = tup[:1] + lastTup[1:]
            # logger.debug('%s, %s: use last tup ts=%s %s' % (mode, timeStamp, lastTup[0], tup))
          else:
            lastTup = tup
          d[tup[0]] = tup[1:]
        byMode[track_setup_name] = d
        byModeBroken[track_setup_name] = broken

      allTimes = list(allTimes)
      allTimes.sort()

      #FIXME dm: check why this is not working...
      #with open('results.pk', 'wb') as f:
      #  pickle.dump((byMode, byModeBroken, allTimes), f)

    script_dir = self._config.opts("system", "rally.root")
    base_dir = self._config.opts("reporting", "report.base.dir")
    file_name = self._config.opts("reporting", "output.html.report.filename")

    # TODO dm: This will be too invasive if create an archive of reports later
    if os.path.exists(base_dir):
      shutil.rmtree(base_dir)
    rally.utils.io.ensure_dir(base_dir)
    shutil.copyfile("%s/resources/dygraph-combined.js" % script_dir, "%s/dygraph-combined.js" % base_dir)
    # TODO dm: We don't get too fancy for now later we should have some kind of archive structure in place...
    full_output_path = "%s/%s" % (base_dir, file_name)

    with open(full_output_path, 'w') as f:
      f.write('''
      <!DOCTYPE html>
      <html lang="en">
      <head>
      <meta charset="utf-8">
      <meta http-equiv="content-type" content="text/html; charset=UTF-8">
      <title>[Elasticsearch Nightly Benchmarks]</title>
      <script type="text/javascript"
        src="dygraph-combined.js"></script>
      <style>
        a:hover * {
          text-decoration: underline;
        }
        html *
        {
          #font-size: 1em !important;
          text-decoration: none;
          #color: #000 !important;
          font-family: Helvetica !important;
        }
      </style>
      </head>
      <body>
      <p>Below are the results of the Elasticsearch nightly benchmark runs. The Apache Software Foundation also provides a similar page for the <a href="https://people.apache.org/~mikemccand/lucenebench/">Lucene nightly benchmarks</a>.</p>
      <h2>Results</h2>
      ''')

      buildTimes = self.loadBuildTimeResults()

      brokenBuildTimes = set()
      for timeStamp, minutes in buildTimes:
        if minutes is None:
          brokenBuildTimes.add(timeStamp)
      byModeBroken['buildtimes'] = brokenBuildTimes

      self.writeCommonGraphFunctions(f, byMode, byModeBroken, allTimes)

      # Indexing docs/sec:
      self.writeIndexPerformanceChart(f, byMode, allTimes)

      self.writeTotalTimesChart(f, byMode, allTimes)

      self.writeSegmentMemory(f, byMode, allTimes)

      self.writeCPUPercent(f, byMode, allTimes)

      self.writeDiskUsage(f, byMode, allTimes)

      self.writeSegmentCounts(f, byMode, allTimes)

      # 'gradle assemble' time
      self.writeEsBuildTimeGraph(f, buildTimes, allTimes)

      self.writeSearchTimesGraph(f, byMode, allTimes)

      # indices/nodes stats time
      self.writeStatsTimeGraph(f, byMode, allTimes)

      # GC times
      self.writeGCTimesGraph(f, byMode, allTimes)

      f.write('  <div style="position: absolute; top: %spx">\n' % self._nextGraph)

      f.write('<h2>Benchmark Scenarios</h2>')
      f.write('<p>%s</p>' % track.description)
      for track_setup in track.track_setups:
        f.write('<p><tt>%s</tt>: %s</p>' % (track_setup.name, track_setup.description))

      f.write('''
      </div>
      </body>
      </html>
      ''')

      f.close()

      # One of the very few occasions where we print directly to console to give users a hint
      print("Reporting data are available in %s" % full_output_path)

  # TODO dm: copy dygraph-combined.js!!
  # TODO dm: Copying to ec2 is not yet supported (Prio 1) (for migration)
  # TODO dm: Ensure that clicking on logs also works locally (not just for the EC2 version) (low prio)
  # if '-copy' in sys.argv:
  #  cmd = 's3cmd put -P %s s3://benchmarks.elasticsearch.org/index.html' % constants.GRAPH_OUT_FILE_NAME
  #  if os.system(cmd):
  #    raise RuntimeError('"%s" failed' % cmd)
  #  for mode in '4gheap', 'fastsettings', 'fastupdates', 'defaults', 'two_nodes_defaults', 'ec2.i2.2xlarge', 'buildtime':
  #    cmd = 's3cmd -m text/plain sync -P %s/%s/ s3://benchmarks.elasticsearch.org/logs/%s/' % (constants.LOGS_DIR, mode, mode)
  #  if os.system(cmd):
  #    raise RuntimeError('"%s" failed' % cmd)

  def _log_dir(self):
    log_root = self._config.opts("system", "log.dir")
    metrics_log_dir = self._config.opts("benchmarks", "metrics.log.dir")
    return "%s/%s" % (log_root, metrics_log_dir)

  def _build_log_dir(self):
    log_root = self._config.opts("system", "log.dir")
    build_log_dir = self._config.opts("build", "log.dir")
    return "%s/%s" % (log_root, build_log_dir)

  def msecToSec(self, x):
    return x/1000.0

  def msecToMinutes(self, x):
    return x/1000.0/60.0

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
      s = chr(65 + label)
    else:
      s = '%s%s' % (chr(65 + (label / 26 - 1)), chr(65 + (label % 26)))
    return s

  def writeGraphHeader(self, f, id):
    next = self._nextGraph
    f.write('''
    <a name="%s"></a>
    <div id="chart_%s" style="height:500px; position: absolute; left: 0px; right: 260px; top: %spx"></div>
    <div id="chart_%s_labels" style="width: 250px; position: absolute; right: 0px; top: %spx"></div>''' % (id, id, next, id, next + 30))
    self._nextGraph += 550

  def writeGraphFooter(self, f, id, title, yLabel):
    f.write(''',
    { "title": "<a href=\'#%s\'><font size=+2>%s</font></a>",
      "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "%s",
      "connectSeparatedPoints": true,
      "hideOverlayOnMouseOut": false,
      "labelsDiv": "chart_%s_labels",
      "labelsSeparateLines": true,
      "legend": "always",
      "clickCallback": onClick,
      "drawPointCallback": drawPoint,
      "drawPoints": true,
      }
      );
      ''' % (id, title, yLabel, id))

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
    segTotalMemoryBytes = None
    flushTimeMillis = None
    refreshTimeMillis = None
    mergeTimeMillis = None
    mergeThrottleTimeMillis = None
    indexingTimeMillis = None
    dvTotalMemoryBytes = None
    storedFieldsTotalMemoryBytes = None
    termsTotalMemoryBytes = None
    normsTotalMemoryBytes = None

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
          primaries = d['_all']['primaries']
          segCount = primaries['segments']['count']
          segTotalMemoryBytes = primaries['segments']['memory_in_bytes']
          dvTotalMemoryBytes = primaries['segments'].get('doc_values_memory_in_bytes')
          storedFieldsTotalMemoryBytes = primaries['segments'].get('stored_fields_memory_in_bytes')
          termsTotalMemoryBytes = primaries['segments'].get('terms_memory_in_bytes')
          normsTotalMemoryBytes = primaries['segments'].get('norms_memory_in_bytes')
          mergeTimeMillis = primaries['merges']['total_time_in_millis']
          mergeThrottleTimeMillis = primaries['merges'].get('total_throttled_time_in_millis')
          indexingTimeMillis = primaries['indexing']['index_time_in_millis']
          refreshTimeMillis = primaries['refresh']['total_time_in_millis']
          flushTimeMillis = primaries['flush']['total_time_in_millis']

        #TODO dm: specific name should move out of here...
        if line.startswith('NODES STATS: ') and '/defaults.txt' in fileName:
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
          #logger.info('search time %s: %s = %s sec' % (fileName, searchType, t))

    # logger.info('%s: oldGC=%s newGC=%s' % (fileName, oldGCSec, youngGCSec))

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

    timeStamp = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

    if oome or exc or dps is None:
      # dps = 1
      #logger.warn('Exception/oome/no dps in log "%s"; marking failed' % fileName)
      return timeStamp, None

    if indexKB is None:
      if '4gheap' in fileName:
        #logger.warn('Missing index size for log "%s"; marking failed' % fileName)
        return timeStamp, None
      else:
        indexKB = 1

    if (year, month, day) in (
        # Installer was broken?
        (2014, 5, 13),
        # Something went badly wrong:
        (2015, 6, 2)):
      return timeStamp, None

    return timeStamp, dps, indexKB, segCount, searchTimes, indicesStatsMSec, nodesStatsMSec, oldGCSec, youngGCSec, cpuPct, bytesWritten, segTotalMemoryBytes, indexingTimeMillis, mergeTimeMillis, refreshTimeMillis, flushTimeMillis, dvTotalMemoryBytes, storedFieldsTotalMemoryBytes, termsTotalMemoryBytes, normsTotalMemoryBytes, mergeThrottleTimeMillis

  def parseStats(self, line, f):
    x = line.find('STATS: ')
    s = line[x + 7:].strip()
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

    # That's the root for a single build, we want here all builds
    #log_root = self._config.opts("system", "log.dir")
    log_root = self._config.opts("system", "log.root.dir")
    metrics_log_dir = self._config.opts("benchmarks", "metrics.log.dir")

    reLogFile = re.compile(r'^%s/(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)\/%s' % (log_root, metrics_log_dir))

    #rootDir = '%s/%s/' % (self._log_dir(), subdir)

    l = []
    try:
      #l = os.listdir(rootDir)
      # TODO dm: With the new structure, "subdir" is the wrong name, this is actually the file name...
      l = glob.glob("%s/*/%s/%s.txt" % (log_root, metrics_log_dir, subdir))
    except FileNotFoundError:
      logger.warn("%s does not exist. Skipping..." % subdir)
      return results
    l.sort()
    count = 0
    for fileName in l:
      #logger.info('visit fileName=%s' % fileName)
      m = reLogFile.match(fileName)
      if m is not None:

        if False and os.path.getsize(fileName) < 10 * 1024:
          # Something silly went wrong:
          continue

        try:
          tup = self.loadOneFile(fileName, m)
        except:
          logger.error('Exception while parsing %s' % fileName)
          raise

        d = tup[0]

        if d <= datetime.datetime(year=2014, month=4, day=15):
          # For some reason, before this, the number of indexed docs in the end != the number we had indexed, so we can't back-test prior to this
          continue

        if d >= datetime.datetime(year=2014, month=8, day=7) and \
                d <= datetime.datetime(year=2014, month=8, day=18):
          # #6939 cause a bug on 8/6 in strict mappings, not fixed until #7304 on 8/19
          tup = d, None

        results.append(tup)
        count += 1
        if count % 100 == 0:
          logger.info('  %d of %d files...' % (count, len(l)))

    logger.info('  %d of %d files...' % (count, len(l)))

    # Sort by date:
    results.sort(key=lambda x: x[0])

    return results

  def writeCommonGraphFunctions(self, f, byMode, byModeBroken, allTimes):

    """
    Writes javascript function (onClick) to handle mouse clicks on the charts,
    sending browser to log file for that point, and to draw failed runs with
    an organge dot (drawPoint).
    """

    f.write('''
    <script type="text/javascript">
      function zp(num,count) {
        var ret = num + '';
        while(ret.length < count) {
          ret = "0" + ret;
        }
        return ret;
      }
  ''')

    # Translate broken timestamps into index:
    brokenIndex = {}
    f.write('     var brokenIndexMap = {};\n')
    # TODO dm: Use track setups here
    for mode in 'defaults', '4gheap', 'fastsettings', 'fastupdates', 'two_nodes_defaults', 'ec2.i2.2xlarge', 'buildtimes':
      f.write('    brokenIndexMap["%s"] = [' % self._toPrettyName[mode])
      for timeStamp in allTimes:
        # TODO dm: We don't need to check 'mode in byModeBroken' when we use track setups here
        if mode in byModeBroken and timeStamp in byModeBroken[mode]:
          x = 1
        else:
          x = 0
        f.write('%s,' % x)
      f.write('];\n')

    for name in 'Total bytes written', 'Final index size', 'Defaults (%)', 'Phrase (msec)', 'Aggs (hourly timeStamp) (msec)', 'Term (msec)', 'Default (msec)', 'Scroll All (msec)', 'Indices stats (msec)', 'Nodes stats (msec)', 'GC young gen (sec)', 'GC old gen (sec)', 'Total heap used (MB)', 'Indexing time (min)', 'Merge time (min)', 'Refresh time (min)', 'Flush time (min)', 'Merge throttle time (min)', 'Doc values (MB)', 'Terms (MB)', 'Norms (MB)', 'Stored fields (MB)':
      f.write('    brokenIndexMap["%s"] = brokenIndexMap["Defaults"];\n' % name)

    f.write('''
      function drawPoint(g, seriesName, ctx, cx, cy, color, pointSize, idx) {
        // Only draw an orange point so we know this was a failed run, but we can still click on it to see the log of why it failed:
        if (brokenIndexMap[seriesName][idx] == 1) {
          ctx.beginPath();
          ctx.fillStyle = "orange";
          ctx.arc(cx, cy, 2, 0, 2 * Math.PI, false);
          ctx.fill();
        }
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
        if (name == "Defaults" ||
            name == "Defaults (%)" ||  // CPU chart
            name == "Final index size" || // Disk usage
            name == "Total bytes written" || // Disk usage
            name == "Nodes stats (msec)" ||
            name == "Indices stats (msec)" ||
            name == "Term (msec)" ||
            name == "Default (msec)" ||
            name == "Phrase (msec)" ||
            name == "Aggs (hourly timeStamp) (msec)" ||
            name == "GC young gen (sec)" || // GC times
            name == "GC old gen (sec)" // GC times
            ) {
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
        } else if (name == "Test time (minutes)") {
          mode = "buildtime";
        } else {
          mode = "defaults";
        }

        var s = "https://benchmarks.elastic.co/logs/" + mode + "/" + d.getFullYear() + "-" + zp(1+d.getMonth(), 2) + "-" + zp(d.getDate(), 2);
        s += "-" + zp(d.getHours(), 2) + "-" + zp(d.getMinutes(), 2) + "-" + zp(d.getSeconds(), 2);
        s += ".txt";
        // console.log(s);
        top.location = s;
      }
    </script>
  ''')

  def writeSearchTimesGraph(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'search_times')
    f.write('''
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
              'hourly_agg': 'Aggs (hourly timeStamp)',
              'country_agg': 'Aggs (Population by Country)'}

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
          x = '%.2f' % (1000.0 * t)
          any = True
        l.append(x)
      if any:
        chartTimes.append(timeStamp)
      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    self.writeGraphFooter(f, 'search_times', 'Search times', 'Milliseconds')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Term (msec)')

    f.write('</script>')

  def writeStatsTimeGraph(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'stats_time')
    f.write('''
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

    self.writeGraphFooter(f, 'stats_time', 'Stats request time', 'Milliseconds')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Indices stats (msec)')

    f.write('</script>')

  def writeIndexPerformanceChart(self, f, byMode, allTimes):
    self.writeGraphHeader(f, 'indexing')

    f.write('''
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_indexing"),
    ''')

    # TODO dm: Be very wary of the order here!!! - see similar comment there (see also logging_track.py)
    headers = ['Date', 'Defaults', 'Defaults (4G heap)', 'Fast', 'FastUpdate', 'Defaults (2 nodes)', 'EC2 i2.2xlarge Defaults 4G']
    f.write('    "%s\\n"\n' % ','.join(headers))

    # Records valid timestamps by series:
    chartTimes = {}

    for timeStamp in allTimes:
      l = []
      any = False
      lastDPS = None
      # TODO dm: Pass track setups in here!!
      for mode in 'defaults', '4gheap', 'fastsettings', 'fastupdates', 'two_nodes_defaults', 'ec2.i2.2xlarge':
        if mode in byMode and timeStamp in byMode[mode]:
          dps, indexKB, segCount = byMode[mode][timeStamp][:3]
          # logger.debug('render %s %s: %s' % (mode, timeStamp, dps))
          if dps is None:
            if lastDPS is not None:
              any = True
              dps = lastDPS
            else:
              dps = ''
          else:
            dps = '%.3f' % (dps / 1000.0)
            any = True
            lastDPS = dps

          # Record the chart time even for failed runs so annots on failure are attached to the orange dots:
          series = self._toPrettyName[mode]
          if series not in chartTimes:
            chartTimes[series] = []
          chartTimes[series].append(timeStamp)
        else:
          dps = ''
        l.append(dps)

      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    self.writeGraphFooter(f, 'indexing', 'Indexing throughput on master', 'K docs/sec')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Defaults (4G heap)')

    f.write('</script>')

  def writeCPUPercent(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'cpu')
    f.write('''
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
      # TODO dm: Mentions specific track setup -> abstract
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

    self.writeGraphFooter(f, 'cpu', 'Indexing CPU utilization (defaults)', 'CPU (median %)')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Defaults (%)')

    f.write('</script>')

  def writeSegmentMemory(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'segment_total_memory')
    f.write('''
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_segment_total_memory"),
  ''')

    headers = ['Date', 'Total heap used (MB)', 'Doc values (MB)', 'Terms (MB)', 'Norms (MB)', 'Stored fields (MB)']
    f.write('    "%s\\n"\n' % ','.join(headers))

    chartTimes = []
    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults']:
        tup = byMode['defaults'][timeStamp]
        segTotalMemoryBytes = tup[10]
        dvTotalMemoryBytes, storedFieldsTotalMemoryBytes, termsTotalMemoryBytes, normsTotalMemoryBytes = tup[15:19]
        if segTotalMemoryBytes is not None:
          l.append('%.2f' % rally.utils.format.bytes_to_mb(segTotalMemoryBytes))
          chartTimes.append(timeStamp)
        else:
          l.append('')
        if dvTotalMemoryBytes is not None:
          l.append('%.2f' % rally.utils.format.bytes_to_mb(dvTotalMemoryBytes))
          l.append('%.2f' % rally.utils.format.bytes_to_mb(termsTotalMemoryBytes))
          l.append('%.2f' % rally.utils.format.bytes_to_mb(normsTotalMemoryBytes))
          l.append('%.2f' % rally.utils.format.bytes_to_mb(storedFieldsTotalMemoryBytes))
        else:
          l.extend(['', '', '', ''])
      else:
        l.extend(['', '', '', '', ''])
      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    self.writeGraphFooter(f, 'segment_total_memory', 'Segment total heap used', 'Heap used (MB)')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Heap used (MB)')

    f.write('</script>')

  def writeTotalTimesChart(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'total_times')
    f.write('''
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_total_times"),
  ''')

    headers = ['Date', 'Indexing time (min)', 'Merge time (min)', 'Refresh time (min)', 'Flush time (min)', 'Merge throttle time (min)']
    f.write('    "%s\\n"\n' % ','.join(headers))

    # Before this the times were off-the-chart for some reason!
    startTime = datetime.datetime(year=2014, month=5, day=20)

    chartTimes = []
    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults'] and timeStamp >= startTime:
        tup = byMode['defaults'][timeStamp]
        indexingTimeMillis, mergeTimeMillis, refreshTimeMillis, flushTimeMillis = tup[11:15]
        mergeThrottleTimeMillis = tup[18]
        if indexingTimeMillis is not None:
          l.append('%.1f' % self.msecToMinutes(indexingTimeMillis))
          l.append('%.1f' % self.msecToMinutes(mergeTimeMillis))
          l.append('%.1f' % self.msecToMinutes(refreshTimeMillis))
          l.append('%.1f' % self.msecToMinutes(flushTimeMillis))
          if mergeThrottleTimeMillis is None:
            l.append('')
          else:
            l.append('%.1f' % self.msecToMinutes(mergeThrottleTimeMillis))
          chartTimes.append(timeStamp)
        else:
          l.extend(['', '', '', '', ''])
      else:
        l.extend(['', '', '', '', ''])
      f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    self.writeGraphFooter(f, 'total_times', 'Total times', 'Time (min)')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Indexing time (min)')

    f.write('</script>')

  def writeGCTimesGraph(self, f, byMode, allTimes):
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'gc_time')
    f.write('''
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
      # TODO dm: Mentions specific track setup -> abstract
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

    self.writeGraphFooter(f, 'gc_time', 'GC times', 'Seconds')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'GC young gen (sec)')

    f.write('</script>')

  def writeAnnots(self, f, chartTimes, annots, defaultSeries):
    if len(chartTimes) == 0:
      return

    # First, just aggregate by the timeStamp we need to attach each annot to, so in
    # case more than one change lands on one point, we can show all of them under a
    # single annot:

    if type(chartTimes) is list:
      firstTimeStamp = chartTimes[0]
    else:
      firstTimeStamp = None
      for l in chartTimes.values():
        if len(l) > 0 and (firstTimeStamp is None or l[0] < firstTimeStamp):
          firstTimeStamp = l[0]

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
      else:
        if len(parts) != 3:
          raise RuntimeError('invalid timestamp "%s": should be YYYY-MM-DD[-HH-MM-SS]' % date)
        hour = min = sec = 0
      timeStamp = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=sec)

      if timeStamp < firstTimeStamp:
        # If this annot is from before this chart started, skip it:
        # logger.info('skip annot %s %s: %s' % (date, series, reason))
        continue

      # Place the annot on the next data point that's <= the annot timestamp, in this chart:
      if type(chartTimes) is dict:
        if series not in chartTimes:
          logger.info('skip annot %s %s: %s (series not in chartTimes)' % (date, series, reason))
          continue
        l = chartTimes[series]
      else:
        l = chartTimes

      idx = bisect.bisect_left(l, timeStamp)

      if idx is not None:
        # This is the timestamp, on or after when the annot was, that exists in the particular
        # series we are annotating:
        bestTimeStamp = l[idx]
        if bestTimeStamp not in byTimeStamp:
          byTimeStamp[bestTimeStamp] = {}
        if series not in byTimeStamp[bestTimeStamp]:
          byTimeStamp[bestTimeStamp][series] = [label]
          label += 1
        byTimeStamp[bestTimeStamp][series].append(reason)

    # Then render the annots:
    f.write('g.ready(function() {g.setAnnotations([')
    for timeStamp, d in byTimeStamp.items():
      for series, items in d.items():
        messages = r'\n\n'.join(items[1:])
        f.write('{series: "%s", x: "%s", shortText: "%s", text: "%s"},\n' % \
                (series, self.toString(timeStamp), self.getLabel(items[0]), messages.replace('"', '\\"')))
    f.write(']);});\n')

  def writeDiskUsage(self, f, byMode, allTimes):

    # Final disk usage and total bytes written to disk:
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'disk_usage')
    f.write('''
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
      for mode in 'defaults',:
        if timeStamp in byMode[mode]:
          indexKB = byMode[mode][timeStamp][1]
          if indexKB is not None:
            l.append('%.1f' % (indexKB / 1024 / 1024.))
            any = True
          else:
            l.append('')
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

    self.writeGraphFooter(f, 'disk_usage', 'Indexing disk usage', 'GB')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Final index size')
    f.write('</script>')

  def writeSegmentCounts(self, f, byMode, allTimes):

    # Seg counts:
    f.write('\n\n<br><br>')
    self.writeGraphHeader(f, 'seg_counts')
    f.write('''
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
      for mode in 'defaults', '4gheap', 'fastsettings', 'fastupdates':
        if mode in byMode and timeStamp in byMode[mode]:
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

    self.writeGraphFooter(f, 'seg_counts', 'Index segment counts', 'Segment count')

    self.writeAnnots(f, chartTimes, KNOWN_CHANGES, 'Defaults (4G heap)')

    f.write('</script>')

  def loadBuildTimeResults(self):
    # Load/parse "gradle assemble" times:
    results = []
    reYMD = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)\.txt$')
    reYMDHMS = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)\.txt$')
    buildlogDirectory = self._build_log_dir()
    try:
      l = os.listdir(buildlogDirectory)
    except FileNotFoundError:
      logger.warn("%s does not exist. Skipping build time stats..." % buildlogDirectory)
      l = []
    l.sort()
    for fileName in l:
      m = reYMD.match(fileName)
      if m is None:
        m = reYMDHMS.match(fileName)
      if m is not None:
        #logger.info("%s" % fileName)
        with open('%s/%s' % (buildlogDirectory, fileName), 'r') as fIn:
          success = False
          minutes = None
          # TODO dm: (a) we're just interested in the result -> read the file backwards (b) have a separate metrics collector intead of the log file
          for line in fIn.readlines():
            if "BUILD SUCCESS" in line:
              success = True
            i = line.find('Total time:')
            if i != -1:
              if 'secs' in line:
                # Gradle:
                tup = line[i+11:].strip().split()
                if len(tup) == 2 and tup[1] == 'secs':
                  minutes = float(tup[0])/60.0
                elif len(tup) != 4:
                  raise RuntimeError('unexpected time %s, file %s/%s' % (line, buildlogDirectory, fileName))
                else:
                  if tup[1] != 'mins' or tup[3] != 'secs':
                    raise RuntimeError('unexpected time %s, file %s/%s' % (line, buildlogDirectory, fileName))
                  minutes = int(tup[0]) + float(tup[2])/60.
              else:
                s = line[i+11:].replace(' min', '').strip()
                tup = s.split(':')
                if len(tup) == 1 and tup[0].endswith(' s'):
                  minutes = float(tup[0][:-2].strip())/60.0
                elif len(tup) != 2:
                  raise RuntimeError('unexpected time %s, file %s/%s' % (s, buildlogDirectory, fileName))
                else:
                  minutes = int(tup[0]) + int(tup[1])/60.

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

          if success:
            if minutes is not None:
              results.append((d, minutes))
            else:
              raise RuntimeError('build success but could not find total time')
          else:
            # Record that this run was broken:
            results.append((d, None))

    results.sort()
    return results

  def writeEsBuildTimeGraph(self, f, results, allTimes):
    f.write('<a name="buildtime"></a>\n')

    self.writeGraphHeader(f, 'build_time')

    f.write('''
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_build_time"),
    ''')

    headers = ['Date', 'Test time (minutes)']
    f.write('    "%s\\n"\n' % ','.join(headers))

    lastMinutes = None

    byTimeStamp = {}
    for timeStamp, minutes in results:
      byTimeStamp[timeStamp] = minutes

    for timeStamp in allTimes:
      if timeStamp in byTimeStamp:
        minutes = byTimeStamp[timeStamp]
        if minutes is None:
          # This point will be rendered as broken:
          minutes = lastMinutes
        else:
          lastMinutes = minutes
        if minutes is None:
          f.write('    + "%s,\\n"\n' % (self.toString(timeStamp)))
        else:
          f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), minutes))
      else:
        f.write('    + "%s,\\n"\n' % (self.toString(timeStamp)))

    self.writeGraphFooter(f, 'build_time', 'Time for \'gradle check \'', 'Minutes')

    self.writeAnnots(f, list(x[0] for x in results), KNOWN_CHANGES_IN_BUILD, 'Test time (minutes)')

    f.write('</script>')
