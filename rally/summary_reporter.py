import json
import os
import glob
import re
import datetime
import logging

import rally.utils.io
import rally.utils.format

# this is somewhat of a hack. I'd rather have a reporting data model in place and have a reporter just output that...

logger = logging.getLogger("rally.reporting")


class SummaryReporter:
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

    self.print_header("    _______             __   _____                    ")
    self.print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
    self.print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
    self.print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
    self.print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ \n\n")

    self.writeIndexPerformanceChart(byMode, allTimes)
    self.writeSearchTimesGraph(byMode, allTimes)

    self.writeTotalTimesChart(byMode, allTimes)

    # System Metrics
    self.print_header("System Metrics")
    self.writeCPUPercent(byMode, allTimes)
    self.writeGCTimesGraph(byMode, allTimes)
    print("")

    self.print_header("Index Metrics")
    self.writeDiskUsage(byMode, allTimes)
    self.writeSegmentMemory(byMode, allTimes)
    self.writeSegmentCounts(byMode, allTimes)
    print("")

    self.writeStatsTimeGraph(byMode, allTimes)

  def print_header(self, message):
    print("\033[1m%s\033[0m" % message)

  def _log_dir(self):
    log_root = self._config.opts("system", "log.dir")
    metrics_log_dir = self._config.opts("benchmarks", "metrics.log.dir")
    return "%s/%s" % (log_root, metrics_log_dir)

  def _build_log_dir(self):
    log_root = self._config.opts("system", "log.dir")
    build_log_dir = self._config.opts("build", "log.dir")
    return "%s/%s" % (log_root, build_log_dir)

  def msecToSec(self, x):
    return x / 1000.0

  def msecToMinutes(self, x):
    return x / 1000.0 / 60.0

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

        # TODO dm: specific name should move out of here...
        if line.startswith('NODES STATS: ') and '/defaults.txt' in fileName:
          d = self.parseStats(line, f)
          nodes = d['nodes']
          if len(nodes) != 1:
            raise RuntimeError('expected one node but got %s' % len(nodes))
          node = list(nodes.keys())[0]
          node = nodes[node]
          d = node['jvm']['gc']['collectors']
          oldGCSec = d['old']['collection_time_in_millis'] / 1000.0
          youngGCSec = d['young']['collection_time_in_millis'] / 1000.0

        if line.startswith('SEARCH ') and ' (median): ' in line:
          searchType = line[7:line.find(' (median): ')]
          t = float(line.strip().split()[-2])
          searchTimes[searchType] = t
          # logger.info('search time %s: %s = %s sec' % (fileName, searchType, t))

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
      # logger.warn('Exception/oome/no dps in log "%s"; marking failed' % fileName)
      return timeStamp, None

    if indexKB is None:
      if '4gheap' in fileName:
        # logger.warn('Missing index size for log "%s"; marking failed' % fileName)
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

    log_root = self._config.opts("system", "log.dir")
    # That's the root for all builds, we want here a single build
    overall_log_root = self._config.opts("system", "log.root.dir")
    metrics_log_dir = self._config.opts("benchmarks", "metrics.log.dir")

    reLogFile = re.compile(r'^%s/(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)\/%s' % (overall_log_root, metrics_log_dir))

    # rootDir = '%s/%s/' % (self._log_dir(), subdir)
    l = []
    try:
      logger.info("Searching %s/%s/%s.txt" % (log_root, metrics_log_dir, subdir))
      # TODO dm: With the new structure, "subdir" is the wrong name, this is actually the file name...
      l = glob.glob("%s/%s/%s.txt" % (log_root, metrics_log_dir, subdir))
    except FileNotFoundError:
      logger.warn("%s does not exist. Skipping..." % subdir)
      return results
    l.sort()
    count = 0
    for fileName in l:
      logger.info('visit fileName=%s' % fileName)
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

  def writeSearchTimesGraph(self, byMode, allTimes):
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

    self.print_header("Search Time Results (Latency):")

    for searchType in searchTypes:
      headers.append('%s (msec)' % pretty.get(searchType, searchType))

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
        # l.append(x)
        print("  %s: %s ms" % (pretty.get(searchType, searchType), x))
      if any:
        chartTimes.append(timeStamp)
        # f.write('    + "%s,%s\\n"\n' % (self.toString(timeStamp), ','.join(l)))

    print("")

  def writeStatsTimeGraph(self, byMode, allTimes):
    self.print_header("Stats request latency:")
    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults']:
        dps, indexKB, segCount, defaultSearchSec, indicesStatsMSec, nodesStatsMSec = byMode['defaults'][timeStamp][:6]
        if indicesStatsMSec is not None:
          x = '%.5f' % indicesStatsMSec
          print("  Indices stats: %sms" % x)
        if nodesStatsMSec is not None:
          x = '%.5f' % nodesStatsMSec
          print("  Nodes stats: %sms" % x)
    print("")

  def writeIndexPerformanceChart(self, byMode, allTimes):
    # TODO dm: Be very wary of the order here!!! - see similar comment there (see also logging_track.py)
    headers = ['Date', 'Defaults', 'Defaults (4G heap)', 'Fast', 'FastUpdate', 'Defaults (2 nodes)', 'EC2 i2.2xlarge Defaults 4G']

    # Records valid timestamps by series:
    chartTimes = {}

    self.print_header("Indexing Results (Throughput):")

    for timeStamp in allTimes:
      l = []
      any = False
      lastDPS = None
      # TODO dm: Pass track setups in here!!
      for mode in 'defaults', '4gheap', 'fastsettings', 'fastupdates', 'two_nodes_defaults', 'ec2.i2.2xlarge':
        if mode in byMode and timeStamp in byMode[mode]:
          dps, indexKB, segCount = byMode[mode][timeStamp][:3]
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
          if len(byMode) > 1:
            print("  %s K docs/sec (track setup: %s)" % (dps, mode))
          else:
            print("  %s K docs/sec" % dps)
          if series not in chartTimes:
            chartTimes[series] = []
          chartTimes[series].append(timeStamp)
        else:
          dps = ''
        l.append(dps)

    print("")

  def writeCPUPercent(self, byMode, allTimes):
    chartTimes = []
    for timeStamp in allTimes:
      l = []
      # TODO dm: Mentions specific track setup -> abstract
      if timeStamp in byMode['defaults']:
        cpuPct = byMode['defaults'][timeStamp][8]
        if cpuPct is not None:
          x = '%.1f' % cpuPct
          print("  Median indexing CPU utilization: %s%%" % x)
        else:
          x = ''
        l.append(x)
      else:
        l.append('')
      if l != ['']:
        chartTimes.append(timeStamp)

  def writeSegmentMemory(self, byMode, allTimes):
    headers = ['Date', 'Total heap used (MB)', 'Doc values (MB)', 'Terms (MB)', 'Norms (MB)', 'Stored fields (MB)']
    chartTimes = []
    for timeStamp in allTimes:
      l = []
      if timeStamp in byMode['defaults']:
        tup = byMode['defaults'][timeStamp]
        segTotalMemoryBytes = tup[10]
        dvTotalMemoryBytes, storedFieldsTotalMemoryBytes, termsTotalMemoryBytes, normsTotalMemoryBytes = tup[15:19]
        if segTotalMemoryBytes is not None:
          print('  Total heap used for segments     : %.2fMB' % rally.utils.format.bytes_to_mb(segTotalMemoryBytes))
          chartTimes.append(timeStamp)
        if dvTotalMemoryBytes is not None:
          print('  Total heap used for doc values   : %.2fMB' % rally.utils.format.bytes_to_mb(dvTotalMemoryBytes))
          print('  Total heap used for terms        : %.2fMB' % rally.utils.format.bytes_to_mb(termsTotalMemoryBytes))
          print('  Total heap used for norms        : %.2fMB' % rally.utils.format.bytes_to_mb(normsTotalMemoryBytes))
          print('  Total heap used for stored fields: %.2fMB' % rally.utils.format.bytes_to_mb(storedFieldsTotalMemoryBytes))

  def writeTotalTimesChart(self, byMode, allTimes):

    self.print_header("Total times:")
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
          print('  Indexing time      : %.1f min' % self.msecToMinutes(indexingTimeMillis))
          print('  Merge time         : %.1f min' % self.msecToMinutes(mergeTimeMillis))
          print('  Refresh time       : %.1f min' % self.msecToMinutes(refreshTimeMillis))
          print('  Flush time         : %.1f min' % self.msecToMinutes(flushTimeMillis))
          if mergeThrottleTimeMillis is not None:
            print('  Merge throttle time: %.1f min' % self.msecToMinutes(mergeThrottleTimeMillis))
          chartTimes.append(timeStamp)
    print("")

  def writeGCTimesGraph(self, byMode, allTimes):
    headers = ['Date', 'GC young gen (sec)', 'GC old gen (sec)']

    chartTimes = []

    for timeStamp in allTimes:
      l = []
      # TODO dm: Mentions specific track setup -> abstract
      if timeStamp in byMode['defaults'] and self.yearMonthDay(timeStamp) not in ((2014, 5, 18), (2014, 5, 19)):
        dps, indexKB, segCount, defaultSearchSec, indicesStatsMSec, nodesStatsMSec, oldGCSec, youngGCSec = byMode['defaults'][timeStamp][:8]
        if youngGCSec is not None:
          x = '%.5f' % youngGCSec
          print("  Total time spent in young gen GC: %ss" % x)
        else:
          x = ''
        l.append(x)
        if oldGCSec is not None:
          x = '%.5f' % oldGCSec
          print("  Total time spent in old gen GC: %ss" % x)
        else:
          x = ''
        l.append(x)
      else:
        l.append('')
        l.append('')
      if l != ['', '']:
        chartTimes.append(timeStamp)

  def writeDiskUsage(self, byMode, allTimes):
    chartTimes = []
    for timeStamp in allTimes:
      l = []
      any = False
      for mode in 'defaults',:
        if timeStamp in byMode[mode]:
          indexKB = byMode[mode][timeStamp][1]
          if indexKB is not None:
            x = '%.1f' % (indexKB / 1024 / 1024.)
            y = '%.1f' % (indexKB / 1024.)
            l.append(x)
            print("  Final index size: %sGB (%sMB)" % (x, y))
            any = True
          else:
            l.append('')
          bytesWritten = byMode['defaults'][timeStamp][9]
          if bytesWritten is not None:
            x = '%.2f' % (bytesWritten / 1024. / 1024. / 1024.)
            y = '%.2f' % (bytesWritten / 1024. / 1024.)
            print("  Totally written: %sGB (%sMB)" % (x, y))
            any = True
          else:
            x = ''
          l.append(x)
        else:
          l.append('')
          l.append('')
      if any:
        chartTimes.append(timeStamp)

  def writeSegmentCounts(self, byMode, allTimes):
    headers = ['Date', 'Defaults', 'Defaults (4G heap)', 'Fast', 'FastUpdate']

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
            if len(byMode) > 1:
              print("  Index segment count: %s (track setup: %s)" % (x, mode))
            else:
              print("  Index segment count: %s" % x)
          else:
            x = ''
        else:
          x = ''
        l.append(x)
      if any:
        chartTimes.append(timeStamp)
