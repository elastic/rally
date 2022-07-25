### 2.6.0

#### Highlights

* [#1532](https://github.com/elastic/rally/pull/1532): Use main branch of Elasticsearch for source builds
* [#1520](https://github.com/elastic/rally/pull/1520): Create and use a unique ES API key for each simulated client

#### Enhancements

* [#1535](https://github.com/elastic/rally/pull/1535): Use new logo on GitHub, PyPI and Read the Docs
* [#1530](https://github.com/elastic/rally/pull/1530): Add OS mem stats to node-stats telemetry device
* [#1526](https://github.com/elastic/rally/pull/1526): Add runner for paging through composite aggregations

#### Bug Fixes

* [#1522](https://github.com/elastic/rally/pull/1522): Fix pyenv install on Apple Silicons Macs

### 2.5.0

#### Highlights

* [#1496](https://github.com/elastic/rally/pull/1496): Fix use_ssl: True on Python 3.10
* [#1471](https://github.com/elastic/rally/pull/1471): Introduce field-caps operation-type

#### Enhancements

* [#1516](https://github.com/elastic/rally/pull/1516): Create an ES client per simulated client instead of per worker.
* [#1503](https://github.com/elastic/rally/pull/1503): Explicit offline mode
* [#1502](https://github.com/elastic/rally/pull/1502): Display ZGC collector stats in report
* [#1499](https://github.com/elastic/rally/pull/1499): Flexible support of GC collectors in telemetry.
* [#1492](https://github.com/elastic/rally/pull/1492): Support wrapped templates produced by elastic-package dump
* [#1478](https://github.com/elastic/rally/pull/1478): Allow configuring Elasticsearch cluster name
* [#1475](https://github.com/elastic/rally/pull/1475): Add raw-request op type for latency charts
* [#1473](https://github.com/elastic/rally/pull/1473): Show both median and mean ML processing time in charts

#### Bug Fixes

* [#1521](https://github.com/elastic/rally/pull/1521): Retry incomplete HTTP downloads
* [#1518](https://github.com/elastic/rally/pull/1518): Relax target-hosts check for multi clusters
* [#1500](https://github.com/elastic/rally/pull/1500): When running rally with no options, print help and exit.
* [#1489](https://github.com/elastic/rally/pull/1489): Support composable template without top-level `template`
* [#1486](https://github.com/elastic/rally/pull/1486): Remove template key requirement from composable template param source
* [#1482](https://github.com/elastic/rally/pull/1482): Allow compare to continue for missing disk usage stats
* [#1472](https://github.com/elastic/rally/pull/1472): Ensure telemetry path exists for heapdump

#### Miscellaneous Changes

* [#1529](https://github.com/elastic/rally/pull/1529): Make git.clone remote kwarg-only
* [#1527](https://github.com/elastic/rally/pull/1527): Make esrally.utils.git branch agnostic
* [#1512](https://github.com/elastic/rally/pull/1512): Prepare switch to Elasticsearch Python client 8.2.0

### 2.4.0

#### Highlights

* [#1435](https://github.com/elastic/rally/pull/1435): Update Elasticsearch support policy after 8.0 release

#### Enhancements

* [#1461](https://github.com/elastic/rally/pull/1461): Add 'root' attribute to Track class
* [#1412](https://github.com/elastic/rally/pull/1412): Parallelize on corpora basis in bulk task clients

#### Bug Fixes

* [#1469](https://github.com/elastic/rally/pull/1469): Set enable_cleanup_closed by default
* [#1455](https://github.com/elastic/rally/pull/1455): Pick up data streams in disk-usage-stats telemetry
* [#1440](https://github.com/elastic/rally/pull/1440): Fix build by downgrading markupsafe

#### Doc Changes

* [#1447](https://github.com/elastic/rally/pull/1447): Stop leading users away from finding errors in docs
* [#1433](https://github.com/elastic/rally/pull/1433): minor change to docs
* [#1422](https://github.com/elastic/rally/pull/1422): Rename 'indices stats API' to 'index stats API'
* [#1403](https://github.com/elastic/rally/pull/1403): Document exporting certificates from PKCS#12 keystores

### 2.3.1

#### Highlights

* [#1355](https://github.com/elastic/rally/pull/1355): Support Python 3.10

#### Enhancements

* [#1362](https://github.com/elastic/rally/pull/1362): Add timeout parameter to bulk operation

#### Bug Fixes

* [#1418](https://github.com/elastic/rally/pull/1418): Derive correct artifact name for ARM architecture
* [#1368](https://github.com/elastic/rally/pull/1368): Call the correct functions for `scroll-search` and `paginated-search` operations

#### Doc Changes

* [#1358](https://github.com/elastic/rally/pull/1358): Document challenge's user-info property
* [#1357](https://github.com/elastic/rally/pull/1357): Simplify python-caches-clean Makefile command
* [#1356](https://github.com/elastic/rally/pull/1356): Stop mentioning removed "configure" command

#### Miscellaneous Changes

* [#1387](https://github.com/elastic/rally/pull/1387): Switch to native pytest in tests/utils

### 2.3.0

#### Enhancements

* [#1343](https://github.com/elastic/rally/pull/1343): Re-introduce fix for listing nested tracks
* [#1336](https://github.com/elastic/rally/pull/1336): Check benchmark stop at least every second in SamplerThread
* [#1333](https://github.com/elastic/rally/pull/1333): Add percentage diff column to compare subcommand
* [#1330](https://github.com/elastic/rally/pull/1330): Add master node telemetry device
* [#1319](https://github.com/elastic/rally/pull/1319): Add support for listing nested tracks
* [#1313](https://github.com/elastic/rally/pull/1313): Use bridge network in container mode
* [#1312](https://github.com/elastic/rally/pull/1312): Update rally-* index template settings to default to Elasticsearch defaults 
* [#1311](https://github.com/elastic/rally/pull/1311): Show race id in console
* [#1310](https://github.com/elastic/rally/pull/1310): Configure persistent datastore index settings
* [#1305](https://github.com/elastic/rally/pull/1305): Allow parallel tasks to exit on any completion
* [#1303](https://github.com/elastic/rally/pull/1303): Add create and delete ILM policy runners
* [#1296](https://github.com/elastic/rally/pull/1296): Add data streams telemetry device
* [#1292](https://github.com/elastic/rally/pull/1292): Bump Python 3.8 version for dev and CI to 3.8.10
* [#1291](https://github.com/elastic/rally/pull/1291) (Breaking): Drop relative-time-ms metric
* [#1290](https://github.com/elastic/rally/pull/1290): update elasticsearch client to 7.13.2
* [#1288](https://github.com/elastic/rally/pull/1288): add transform-stats operation type
* [#1285](https://github.com/elastic/rally/pull/1285): Ensure exit code 130 is returned from SIGINT

#### Bug Fixes

* [#1318](https://github.com/elastic/rally/pull/1318): Fix num of docs in -1k generated corpus
* [#1294](https://github.com/elastic/rally/pull/1294): Fix composable templates runner
* [#1293](https://github.com/elastic/rally/pull/1293): fix download from s3/gs by moving http query parameter downstream

#### Doc Changes

* [#1337](https://github.com/elastic/rally/pull/1337): Clarify definition of processing time
* [#1332](https://github.com/elastic/rally/pull/1332): Improve docs in various ways
* [#1327](https://github.com/elastic/rally/pull/1327): Allow more copy/pasting in cluster management docs
* [#1326](https://github.com/elastic/rally/pull/1326): Improve documentation in various ways
* [#1325](https://github.com/elastic/rally/pull/1325): Document how to use stat on GNU/Linux too
* [#1299](https://github.com/elastic/rally/pull/1299): Add Makefile target to run all CI checks

#### Miscellaneous Changes

* [#1304](https://github.com/elastic/rally/pull/1304): Run integration tests with the default distro
* [#1302](https://github.com/elastic/rally/pull/1302): Upgrade Python client to 7.14.0
* [#1298](https://github.com/elastic/rally/pull/1298): Use explicit config files for black and isort
* [#1281](https://github.com/elastic/rally/pull/1281): Format code with black and isort
* [#1276](https://github.com/elastic/rally/pull/1276): Switch to pytest asserts

### 2.2.1

#### Enhancements

* [#1273](https://github.com/elastic/rally/pull/1273): Re-introduce relative-time property
* [#1268](https://github.com/elastic/rally/pull/1268): Fix error for shard-stats
* [#1266](https://github.com/elastic/rally/pull/1266): Support gradual ramp-up
* [#1258](https://github.com/elastic/rally/pull/1258): Record shard allocation
* [#1254](https://github.com/elastic/rally/pull/1254): assert developers have block storage dependencies installed
* [#1252](https://github.com/elastic/rally/pull/1252): Make S3 support optional
* [#1243](https://github.com/elastic/rally/pull/1243): Ensure destructive actions work regardless of wildcard input

#### Bug Fixes

* [#1287](https://github.com/elastic/rally/pull/1287): Do not treat increase in transform processing/indexing/search time as improvement
* [#1284](https://github.com/elastic/rally/pull/1284): Ensure max_connections value is logged correctly
* [#1270](https://github.com/elastic/rally/pull/1270): Include requests when installing google-resumable-media
* [#1268](https://github.com/elastic/rally/pull/1268): Fix error for shard-stats
* [#1263](https://github.com/elastic/rally/pull/1263): Add pyenv shims to PATH

#### Doc Changes

* [#1274](https://github.com/elastic/rally/pull/1274): Add example for raw-request
* [#1267](https://github.com/elastic/rally/pull/1267): Consider new API calls in static response example
* [#1265](https://github.com/elastic/rally/pull/1265): Consolidate and improve track/team repo revision logic docs

#### Miscellaneous Changes

* [#1283](https://github.com/elastic/rally/pull/1283): Set daemon attribute directly instead of using deprecated setter

### 2.2.0

#### Enhancements

* [#1245](https://github.com/elastic/rally/pull/1245): Strictly check for supported version
* [#1238](https://github.com/elastic/rally/pull/1238) (Breaking): Disallow to specify cluster settings in the track
* [#1236](https://github.com/elastic/rally/pull/1236): add support for numbers alignment in table output (#1202)
* [#1235](https://github.com/elastic/rally/pull/1235):  Allow to selectively ignore response errors
* [#1234](https://github.com/elastic/rally/pull/1234): Allow to specify static responses
* [#1231](https://github.com/elastic/rally/pull/1231) (Breaking): Drop relative-time metric
* [#1220](https://github.com/elastic/rally/pull/1220): Store duration time of task in rally-results metrics record

#### Doc Changes

* [#1250](https://github.com/elastic/rally/pull/1250): Add note about ES version support
* [#1247](https://github.com/elastic/rally/pull/1247): Update telemetry device docs
* [#1233](https://github.com/elastic/rally/pull/1233): Document proper usage of the Docker image
* [#1230](https://github.com/elastic/rally/pull/1230): Update community resources videos

#### Miscellaneous Changes

* [#1244](https://github.com/elastic/rally/pull/1244): Avoid ephemeral ports in integration tests
* [#1226](https://github.com/elastic/rally/pull/1226): Fix docker release

### 2.1.0

#### Highlights

* [#1190](https://github.com/elastic/rally/pull/1190): Add support for search_after and point-in-time

#### Enhancements

* [#1223](https://github.com/elastic/rally/pull/1223): Fix Issue #1222: esrally CLI should always return 130 when cancelled
* [#1221](https://github.com/elastic/rally/pull/1221): Store relative-time in milliseconds
* [#1216](https://github.com/elastic/rally/pull/1216): Upgrade template engine
* [#1214](https://github.com/elastic/rally/pull/1214): Collect metrics on composite subtasks
* [#1213](https://github.com/elastic/rally/pull/1213): Support chart_name filters in chart annotations
* [#1211](https://github.com/elastic/rally/pull/1211): Add support for merge_count and merge_time graphs
* [#1207](https://github.com/elastic/rally/pull/1207): Expose an API to handle file offset tables
* [#1203](https://github.com/elastic/rally/pull/1203): Log results of cluster health check
* [#1200](https://github.com/elastic/rally/pull/1200): Add optional name in assertion message
* [#1199](https://github.com/elastic/rally/pull/1199): Throttle requests from the beginning
* [#1193](https://github.com/elastic/rally/pull/1193): Add IT tests to test installation according to docs
* [#1178](https://github.com/elastic/rally/pull/1178): Always rely on source artifact caching
* [#1175](https://github.com/elastic/rally/pull/1175) (Breaking): Always require a subcommand
* [#1172](https://github.com/elastic/rally/pull/1172): Improve isolation in race control tests
* [#1171](https://github.com/elastic/rally/pull/1171): Add request-id to search operator
* [#1165](https://github.com/elastic/rally/pull/1165): Fallback to closest minor branch for track/team repositories
* [#1093](https://github.com/elastic/rally/pull/1093): Modify chart generator to support Kibana 7.x dashboards

#### Bug Fixes

* [#1217](https://github.com/elastic/rally/pull/1217): Fix auto build of docs
* [#1209](https://github.com/elastic/rally/pull/1209): Ensure stable mappings for metrics
* [#1192](https://github.com/elastic/rally/pull/1192): Fix compare subcommand
* [#1177](https://github.com/elastic/rally/pull/1177): Use operation name consistently in metrics samples

#### Doc Changes

* [#1185](https://github.com/elastic/rally/pull/1185): Fix installation docs after #1151

### 2.0.4

#### Enhancements

* [#1163](https://github.com/elastic/rally/pull/1163): Make some document properties extensible
* [#1162](https://github.com/elastic/rally/pull/1162): Log trace of uncaught exception in load generator
* [#1159](https://github.com/elastic/rally/pull/1159): Rally improve artifact download headers
* [#1157](https://github.com/elastic/rally/pull/1157): Add support for assertions
* [#1155](https://github.com/elastic/rally/pull/1155): Deprecate invoking Rally without a subcommand
* [#1154](https://github.com/elastic/rally/pull/1154): Allow to filter tasks by tag
* [#1153](https://github.com/elastic/rally/pull/1153): Only consider race id when loading a race
* [#1146](https://github.com/elastic/rally/pull/1146): Show mean throughput in command line report
* [#1143](https://github.com/elastic/rally/pull/1143): Upgrade psutil dependency

#### Bug Fixes

* [#1160](https://github.com/elastic/rally/pull/1160): Include empty mean value in summary stats
* [#1147](https://github.com/elastic/rally/pull/1147): Don't require a name to register a track processor

#### Doc Changes

* [#1169](https://github.com/elastic/rally/pull/1169): Document meta-data returned by operations
* [#1167](https://github.com/elastic/rally/pull/1167): Clarify num of docs in corpora when action and metadata is used
* [#1148](https://github.com/elastic/rally/pull/1148): Make docs build stricter

### 2.0.3

#### Highlights

* [#1112](https://github.com/elastic/rally/pull/1112): Execute complex request hierarchies
* [#1100](https://github.com/elastic/rally/pull/1100): Improve throughput throttling for bulks

#### Enhancements

* [#1140](https://github.com/elastic/rally/pull/1140): Improve responsiveness of track preparation
* [#1136](https://github.com/elastic/rally/pull/1136): Support non default repositories in chart generator
* [#1135](https://github.com/elastic/rally/pull/1135): Allow to customize track preparation
* [#1129](https://github.com/elastic/rally/pull/1129): Support async search API
* [#1118](https://github.com/elastic/rally/pull/1118): Allow index pattern for source-index in shrink-index operation
* [#1115](https://github.com/elastic/rally/pull/1115): Error wrapper script on unclean local state
* [#1109](https://github.com/elastic/rally/pull/1109): Throttle tasks based on presence of a scheduler
* [#1108](https://github.com/elastic/rally/pull/1108): Add support for FOSSA
* [#1099](https://github.com/elastic/rally/pull/1099): Composable + Component template support
* [#1096](https://github.com/elastic/rally/pull/1096): Add CI for Python 3.9.0
* [#1091](https://github.com/elastic/rally/pull/1091): Check registered parameter sources more strictly
* [#1070](https://github.com/elastic/rally/pull/1070): Add support for custom headers and request-timeout
* [#1065](https://github.com/elastic/rally/pull/1065): Allow to specify corpus meta-data

#### Bug Fixes

* [#1132](https://github.com/elastic/rally/pull/1132): Allow for filtering of administrative tasks
* [#1126](https://github.com/elastic/rally/pull/1126): Respect specified order in composite task
* [#1125](https://github.com/elastic/rally/pull/1125): Wait until cluster health is green
* [#1124](https://github.com/elastic/rally/pull/1124): Treat some zero target-intervals as unthrottled
* [#1122](https://github.com/elastic/rally/pull/1122): Remove implicit request timeout from force merge operation in polling
* [#1121](https://github.com/elastic/rally/pull/1121): Ensure that sleep tasks are timed
* [#1119](https://github.com/elastic/rally/pull/1119): Support trailing slashes for all URL schemes
* [#1117](https://github.com/elastic/rally/pull/1117): Don't change scheduler on failed requests
* [#1116](https://github.com/elastic/rally/pull/1116): Ensure admin tasks are always executed
* [#1111](https://github.com/elastic/rally/pull/1111): Allow trailing / in base-url
* [#1103](https://github.com/elastic/rally/pull/1103): Accept some differing units for throttling

#### Doc Changes

* [#1145](https://github.com/elastic/rally/pull/1145): Fix punctuation
* [#1137](https://github.com/elastic/rally/pull/1137): Add a configuration file reference
* [#1133](https://github.com/elastic/rally/pull/1133): Clarify latency-vs-service-time FAQ
* [#1102](https://github.com/elastic/rally/pull/1102): Clarify task execution order in docs with respect to --include-tasks

### 2.0.2

#### Enhancements

* [#1097](https://github.com/elastic/rally/pull/1097): Upgrade google-auth to latest
* [#1094](https://github.com/elastic/rally/pull/1094): Add support for Google Cloud Storage buckets
* [#1092](https://github.com/elastic/rally/pull/1092): Add Support for Datastreams
* [#1064](https://github.com/elastic/rally/pull/1064): Fail fast if leading `/` is missing from `path` in raw-request
* [#1061](https://github.com/elastic/rally/pull/1061): Pass JAVA15_HOME in build
* [#1055](https://github.com/elastic/rally/pull/1055): Check if total_transform_processing_times exist for compare reports
* [#1051](https://github.com/elastic/rally/pull/1051): Force Merge Runner Improvements -  Polling
* [#1047](https://github.com/elastic/rally/pull/1047): Add runner to wait until snapshot has been created
* [#1045](https://github.com/elastic/rally/pull/1045): Show processing time also in comparison reports
* [#1035](https://github.com/elastic/rally/pull/1035): Warn on request errors
* [#1031](https://github.com/elastic/rally/pull/1031): Upgrade Elasticsearch client library to 7.8

#### Bug Fixes

* [#1088](https://github.com/elastic/rally/pull/1088): Use the new pip dependency resolver
* [#1075](https://github.com/elastic/rally/pull/1075): Ensure that request error meta-data is string
* [#1071](https://github.com/elastic/rally/pull/1071): Fix parallel completed-by task identification
* [#1055](https://github.com/elastic/rally/pull/1055): Check if total_transform_processing_times exist for compare reports

#### Doc Changes

* [#1062](https://github.com/elastic/rally/pull/1062): Document how to benchmark an Elastic Cloud cluster
* [#1049](https://github.com/elastic/rally/pull/1049): Docs - hints for handling errors and identifying queries and responses

### 2.0.1

#### Enhancements

* [#1041](https://github.com/elastic/rally/pull/1041): Add indexing_pressure to nodes stats
* [#1039](https://github.com/elastic/rally/pull/1039): Check for pointless statements
* [#1038](https://github.com/elastic/rally/pull/1038): Check for trailing comma tuple
* [#1029](https://github.com/elastic/rally/pull/1029): Add integration test for proxy
* [#1028](https://github.com/elastic/rally/pull/1028): Fetch artifact revision lazily on build
* [#1026](https://github.com/elastic/rally/pull/1026): [WIP] Migrate first test_docker_dev_image 
* [#1025](https://github.com/elastic/rally/pull/1025): Upgrade to Thespian 3.10.1
* [#1024](https://github.com/elastic/rally/pull/1024): Allow runners to determine throughput
* [#1023](https://github.com/elastic/rally/pull/1023): Migrate configure test to new IT infrastructure
* [#1022](https://github.com/elastic/rally/pull/1022): add transform support
* [#1019](https://github.com/elastic/rally/pull/1019): Add timestamps as meta-info in index recovery
* [#1016](https://github.com/elastic/rally/pull/1016): Allow to forcefully close SSL connections
* [#1015](https://github.com/elastic/rally/pull/1015): Scale connection pool automatically
* [#1013](https://github.com/elastic/rally/pull/1013): Allow to measure create/restore snapshot speed
* [#1012](https://github.com/elastic/rally/pull/1012): Make exists_set_param macro available to tracks
* [#1011](https://github.com/elastic/rally/pull/1011): add support for transform API's
* [#1001](https://github.com/elastic/rally/pull/1001): Allow to downsample request metrics
* [#998](https://github.com/elastic/rally/pull/998): Cache source artifacts by default
* [#997](https://github.com/elastic/rally/pull/997): Allow to abort on fatal errors
* [#994](https://github.com/elastic/rally/pull/994): Report number of GCs
* [#992](https://github.com/elastic/rally/pull/992): Cache source artifacts

#### Bug Fixes

* [#1044](https://github.com/elastic/rally/pull/1044): Improve support for benchmarks with many clients
* [#1027](https://github.com/elastic/rally/pull/1027): Resolve JAVA_HOME for source builds lazily
* [#1018](https://github.com/elastic/rally/pull/1018): Check for recovery details when finished
* [#1008](https://github.com/elastic/rally/pull/1008): Allow to specify a runtime-jdk during installation
* [#1007](https://github.com/elastic/rally/pull/1007): Wait until indices are green in generated track
* [#996](https://github.com/elastic/rally/pull/996): Ensure artifact cache directory exists
* [#987](https://github.com/elastic/rally/pull/987): Validate if car allows for using the bundled JDK

#### Doc Changes

* [#1043](https://github.com/elastic/rally/pull/1043): Installation and offline dist improvements
* [#1037](https://github.com/elastic/rally/pull/1037): Correct Rally forum URL
* [#1006](https://github.com/elastic/rally/pull/1006): Add info about querying challenges in the docs
* [#1003](https://github.com/elastic/rally/pull/1003): Add network troubleshooting tips
* [#993](https://github.com/elastic/rally/pull/993): Update the release process
* [#984](https://github.com/elastic/rally/pull/984): Add developer info about intellij

### 2.0.0

#### Highlights

* [#970](https://github.com/elastic/rally/pull/970): Add tooling to create a track from existing data
* [#944](https://github.com/elastic/rally/pull/944): Allow to use significantly more clients
* [#935](https://github.com/elastic/rally/pull/935): Add an asyncio-based load generator
* [#875](https://github.com/elastic/rally/pull/875): Add 'Tracker' track-generation tool

#### Enhancements

* [#988](https://github.com/elastic/rally/pull/988): Add experimental segment stats telemetry device
* [#986](https://github.com/elastic/rally/pull/986): Allow to override GC log parameters
* [#977](https://github.com/elastic/rally/pull/977): Migrate track param check to new IT infrastructure
* [#972](https://github.com/elastic/rally/pull/972): Pass actor system env vars in integration test
* [#971](https://github.com/elastic/rally/pull/971): Add thespian logs to ci
* [#969](https://github.com/elastic/rally/pull/969): Add an it method for rally race
* [#968](https://github.com/elastic/rally/pull/968): Add integration tests with eventdata
* [#967](https://github.com/elastic/rally/pull/967): Raise an error when attempting to bulk-index without any corpora
* [#966](https://github.com/elastic/rally/pull/966): Migrate more integration tests
* [#963](https://github.com/elastic/rally/pull/963): Reduce mapped memory with many bulk clients
* [#962](https://github.com/elastic/rally/pull/962): Increase bulk size to metrics store
* [#959](https://github.com/elastic/rally/pull/959): Clarify the impact on refreshes for node-stats telemetry
* [#958](https://github.com/elastic/rally/pull/958): Avoid duplicate URL parsing for async connections
* [#954](https://github.com/elastic/rally/pull/954): Add flag to handle running processes automatically
* [#953](https://github.com/elastic/rally/pull/953): Pass JAVA14_HOME in build
* [#952](https://github.com/elastic/rally/pull/952): Allow to disable HTTP compression for all queries
* [#951](https://github.com/elastic/rally/pull/951): Upgrade Thespian to 3.10.0
* [#950](https://github.com/elastic/rally/pull/950): Add support for a RALLY_HOME env var everywhere
* [#949](https://github.com/elastic/rally/pull/949): Upgrade Elasticsearch client to 7.6.0
* [#947](https://github.com/elastic/rally/pull/947): Use pbzip2/pigz to decompress corpora if available
* [#945](https://github.com/elastic/rally/pull/945): Add an override for the rally homedir
* [#941](https://github.com/elastic/rally/pull/941): Improve response processing
* [#938](https://github.com/elastic/rally/pull/938): Store units in results
* [#934](https://github.com/elastic/rally/pull/934) (Breaking): Bump minimum Python version to 3.8
* [#932](https://github.com/elastic/rally/pull/932): Improve layout of generated dashboards
* [#931](https://github.com/elastic/rally/pull/931): Store Rally revision in metrics metadata
* [#930](https://github.com/elastic/rally/pull/930): Update psutil to latest version
* [#929](https://github.com/elastic/rally/pull/929): Allow index-stats to compare non-existing path
* [#928](https://github.com/elastic/rally/pull/928): Don't invoke setup.py directly
* [#925](https://github.com/elastic/rally/pull/925): Allow to specify an indices stats condition
* [#923](https://github.com/elastic/rally/pull/923): Assume a TTY for interactive commands
* [#919](https://github.com/elastic/rally/pull/919): Run integration tests with pytest
* [#914](https://github.com/elastic/rally/pull/914): Improve release docs and IT prechecks
* [#913](https://github.com/elastic/rally/pull/913): Normalize handling of --preserve-install

#### Bug Fixes

* [#983](https://github.com/elastic/rally/pull/983): Assign worker-id globally
* [#978](https://github.com/elastic/rally/pull/978): Remove os environment variables from env prep
* [#965](https://github.com/elastic/rally/pull/965): Always shutdown worker thread pool
* [#961](https://github.com/elastic/rally/pull/961): Determine most recent sample per client
* [#960](https://github.com/elastic/rally/pull/960): Disable docstring linter check
* [#956](https://github.com/elastic/rally/pull/956): Fix fallback implementation of ML-related runners
* [#943](https://github.com/elastic/rally/pull/943): Fix request timeout handling
* [#942](https://github.com/elastic/rally/pull/942): Honor response-compression-enabled parameter
* [#927](https://github.com/elastic/rally/pull/927): Add unit for all query charts
* [#918](https://github.com/elastic/rally/pull/918): Gather system metrics on Docker stop if possible

#### Doc Changes

* [#980](https://github.com/elastic/rally/pull/980): Change next Rally version in migration docs
* [#955](https://github.com/elastic/rally/pull/955): Update docs to advise use of pbzip2 for new track corpus
* [#948](https://github.com/elastic/rally/pull/948): Align track reference structure with track
* [#939](https://github.com/elastic/rally/pull/939): Correct JMC download link
* [#937](https://github.com/elastic/rally/pull/937): Fix port number in recipes docs

### 1.4.1

#### Highlights

* [#890](https://github.com/elastic/rally/pull/890): Speed up client-side bulk-handling

#### Enhancements

* [#912](https://github.com/elastic/rally/pull/912): Honor only specified Python versions in prereq
* [#909](https://github.com/elastic/rally/pull/909): Add custom probing URL
* [#908](https://github.com/elastic/rally/pull/908): Validate docker-compose exists in it tests
* [#907](https://github.com/elastic/rally/pull/907): Specify useful variables for Rally CI
* [#906](https://github.com/elastic/rally/pull/906): Specify useful variables for Rally CI
* [#904](https://github.com/elastic/rally/pull/904): Eliminate deprecation warnings in Python 3.7+
* [#903](https://github.com/elastic/rally/pull/903): Show more user-friendly track loader errors
* [#899](https://github.com/elastic/rally/pull/899) (Breaking): Use zeroes instead of whitespaces as padding bytes
* [#895](https://github.com/elastic/rally/pull/895): Log race id on startup
* [#887](https://github.com/elastic/rally/pull/887): Allow to override Python binary for make
* [#886](https://github.com/elastic/rally/pull/886): Fix race condition with old ES processes in IT tests
* [#884](https://github.com/elastic/rally/pull/884): Detach ES using Python standard mechanism
* [#880](https://github.com/elastic/rally/pull/880): Show recent log output on integration test failure
* [#879](https://github.com/elastic/rally/pull/879): Don't read from stdout/stderr on ES startup
* [#873](https://github.com/elastic/rally/pull/873): Prevent installation with unsupported Python
* [#872](https://github.com/elastic/rally/pull/872): Extend error msg to specify operation name for missing index
* [#865](https://github.com/elastic/rally/pull/865): Updating force merge operation to force merge only track indices by default

#### Bug Fixes

* [#901](https://github.com/elastic/rally/pull/901): Fix race in wait_for_pidfile.
* [#894](https://github.com/elastic/rally/pull/894): Only set distribution-version if known
* [#893](https://github.com/elastic/rally/pull/893): Only clear  metrics store on benchmark end
* [#891](https://github.com/elastic/rally/pull/891): Align exception handling in disk IO telemetry
* [#885](https://github.com/elastic/rally/pull/885): Fix issue with fetching test-mode -1k document corpora.
* [#883](https://github.com/elastic/rally/pull/883): Stop all locally running nodes
* [#882](https://github.com/elastic/rally/pull/882): Make REST API check stricter
* [#881](https://github.com/elastic/rally/pull/881): Wait for REST layer before version check
* [#877](https://github.com/elastic/rally/pull/877): Don't write metrics to a closed metrics store
* [#874](https://github.com/elastic/rally/pull/874): Store system metrics if race metadata are present
* [#871](https://github.com/elastic/rally/pull/871): Skip setup phase in IT tests during docker release
* [#868](https://github.com/elastic/rally/pull/868): Fix W0631
* [#860](https://github.com/elastic/rally/pull/860): More resilient node shutdown

#### Doc Changes

* [#902](https://github.com/elastic/rally/pull/902): Add example how to determine actor system status

### 1.4.0

#### Highlights

* [#853](https://github.com/elastic/rally/pull/853): Allow to use the bundled JDK in Elasticsearch
* [#830](https://github.com/elastic/rally/pull/830): Manage Elasticsearch nodes with dedicated subcommands
* [#815](https://github.com/elastic/rally/pull/815): Add Python 3.8 in Rally tests

#### Enhancements

* [#863](https://github.com/elastic/rally/pull/863): Allow piped stdin in run_subprocess_with_logging
* [#862](https://github.com/elastic/rally/pull/862): Add support for excluded tasks in chart_generator
* [#850](https://github.com/elastic/rally/pull/850): Allow to show track details
* [#844](https://github.com/elastic/rally/pull/844): Add task exclude filter
* [#840](https://github.com/elastic/rally/pull/840): Add make target to serve docs locally
* [#836](https://github.com/elastic/rally/pull/836): Upgrade boto3
* [#832](https://github.com/elastic/rally/pull/832): Only keep the most recent build log
* [#829](https://github.com/elastic/rally/pull/829): Reduce usage of platform-specific code
* [#818](https://github.com/elastic/rally/pull/818) (Breaking): Store build.log in log directory
* [#816](https://github.com/elastic/rally/pull/816): Harmonize local pyenv versions with CI ones
* [#805](https://github.com/elastic/rally/pull/805): Add lint + precommit make targets.
* [#803](https://github.com/elastic/rally/pull/803): Calculate system metrics per node
* [#802](https://github.com/elastic/rally/pull/802): Whitelist py.test in tox tests
* [#798](https://github.com/elastic/rally/pull/798): Allow definition of body in restore-snapshot operation
* [#793](https://github.com/elastic/rally/pull/793): Add ability to restore from a snapshot
* [#789](https://github.com/elastic/rally/pull/789): Let the runner determine progress
* [#788](https://github.com/elastic/rally/pull/788): Manage dev dependencies in setup.py
* [#785](https://github.com/elastic/rally/pull/785): Don't attach telemetry devices for Docker
* [#783](https://github.com/elastic/rally/pull/783): Upgrade pytest to 5.2.0
* [#779](https://github.com/elastic/rally/pull/779) (Breaking): Gather cluster-level metrics in driver
* [#778](https://github.com/elastic/rally/pull/778) (Breaking): Expose race-id as command line parameter
* [#776](https://github.com/elastic/rally/pull/776): Add heapdump telemetry device
* [#774](https://github.com/elastic/rally/pull/774): Raise an error if race is not found by id
* [#773](https://github.com/elastic/rally/pull/773): Store race files always as race.json
* [#771](https://github.com/elastic/rally/pull/771): Store track-related meta-data in results
* [#767](https://github.com/elastic/rally/pull/767): Remove merge times from command line report
* [#766](https://github.com/elastic/rally/pull/766): Ensure tox environment is clean
* [#765](https://github.com/elastic/rally/pull/765): Start release process creating NOTICE
* [#727](https://github.com/elastic/rally/pull/727): Allow function-level invocation of integration-test.sh.

#### Bug Fixes

* [#861](https://github.com/elastic/rally/pull/861): Pass plugin params for all plugins
* [#859](https://github.com/elastic/rally/pull/859): Detach Elasticsearch on startup
* [#858](https://github.com/elastic/rally/pull/858): Use the venv pylint in the makefile
* [#841](https://github.com/elastic/rally/pull/841): Store Disk I/O metrics if available
* [#825](https://github.com/elastic/rally/pull/825): Change remaining it tests to port 19200
* [#820](https://github.com/elastic/rally/pull/820): Upgrade psutil to work with newer linux kernels
* [#814](https://github.com/elastic/rally/pull/814): Fix error handling for automatically derived version
* [#804](https://github.com/elastic/rally/pull/804): Allow multiple nodes per IP
* [#801](https://github.com/elastic/rally/pull/801): Initialize schedule lazily
* [#800](https://github.com/elastic/rally/pull/800): Properly wait for recovery to finish
* [#799](https://github.com/elastic/rally/pull/799): Don't set retries for restore snapshot
* [#784](https://github.com/elastic/rally/pull/784): Attach telemetry device on Docker launch
* [#781](https://github.com/elastic/rally/pull/781): Add support for OSNAME and ARCH variables in dist repo URLs.
* [#768](https://github.com/elastic/rally/pull/768): Honor ingest-percentage for bulks
* [#763](https://github.com/elastic/rally/pull/763) (Breaking): Run a task completely even without time-periods

#### Doc Changes

* [#834](https://github.com/elastic/rally/pull/834): Clarify uses of distribution-version
* [#826](https://github.com/elastic/rally/pull/826): Add note about mustache template
* [#821](https://github.com/elastic/rally/pull/821): Move logrotate comments to separate lines
* [#817](https://github.com/elastic/rally/pull/817): Fix mistake in wait-for-recovery docs
* [#810](https://github.com/elastic/rally/pull/810): Fix duplicated IP in distributed_load diagram
* [#782](https://github.com/elastic/rally/pull/782): Update Elasticsearch version of targets and metricstore in CCR recipe

### 1.3.0

#### Enhancements

* [#764](https://github.com/elastic/rally/pull/764) (Breaking): Remove MergeParts internal telemetry device
* [#762](https://github.com/elastic/rally/pull/762): Check that origin points to main repo for release
* [#761](https://github.com/elastic/rally/pull/761): Allow to retry until success
* [#760](https://github.com/elastic/rally/pull/760): Improve logging of schedules
* [#759](https://github.com/elastic/rally/pull/759): Show track and team revision when listing races
* [#758](https://github.com/elastic/rally/pull/758): Improve error message on SSL errors
* [#755](https://github.com/elastic/rally/pull/755): Add pull request template
* [#748](https://github.com/elastic/rally/pull/748): Consistently render links to the documentation
* [#746](https://github.com/elastic/rally/pull/746): Fixup ProcessLauncherTests issues
* [#744](https://github.com/elastic/rally/pull/744): Change DiskIO callbacks to use attach/detach
* [#739](https://github.com/elastic/rally/pull/739) (Breaking): Remove lap feature and all references.
* [#737](https://github.com/elastic/rally/pull/737): Allow to attach telemetry devices without reprovisioning
* [#735](https://github.com/elastic/rally/pull/735): Assume string type for params by default
* [#731](https://github.com/elastic/rally/pull/731): Update DiskIo telemetry device to persist the counters
* [#730](https://github.com/elastic/rally/pull/730): Be resilient upon startup
* [#729](https://github.com/elastic/rally/pull/729) (Breaking): Drop 1.x support for cluster metadata
* [#728](https://github.com/elastic/rally/pull/728): Allow to set distribution version as parameter
* [#726](https://github.com/elastic/rally/pull/726): Don't check complete list of parameters in integration test
* [#725](https://github.com/elastic/rally/pull/725): Capture team and track revisions in metrics metadata
* [#723](https://github.com/elastic/rally/pull/723): Always exit on OOME
* [#721](https://github.com/elastic/rally/pull/721): Update DiskIo telemetry device to persist the counters
* [#720](https://github.com/elastic/rally/pull/720): Change filestore to be indexed by unique ID
* [#719](https://github.com/elastic/rally/pull/719): ES as a Daemon (again)
* [#716](https://github.com/elastic/rally/pull/716) (Breaking): Drop support for Elasticsearch 1.x
* [#711](https://github.com/elastic/rally/pull/711): Change telemetry devices to rely on jvm.config instead of ES_JAVA_OPTS
* [#701](https://github.com/elastic/rally/pull/701): Implement ES daemon-mode in process launcher

#### Bug Fixes

* [#751](https://github.com/elastic/rally/pull/751): Option track-revision should work with track-repository
* [#750](https://github.com/elastic/rally/pull/750): Retrieve timestamped commit hash separately
* [#747](https://github.com/elastic/rally/pull/747): Log git output

### 1.2.1

#### Highlights

* [#702](https://github.com/elastic/rally/pull/702): Add Rally Docker image to release process
* [#688](https://github.com/elastic/rally/pull/688): Fail Rally early if there are unused variables in track-params

#### Enhancements

* [#713](https://github.com/elastic/rally/pull/713): Check tags in track and team repos
* [#709](https://github.com/elastic/rally/pull/709) (Breaking): Drop support for Python 3.4
* [#708](https://github.com/elastic/rally/pull/708): Align default request param extraction for queries
* [#707](https://github.com/elastic/rally/pull/707): Upgrade pip for virtualenv
* [#705](https://github.com/elastic/rally/pull/705): Provide default for datastore.secure in all cases
* [#704](https://github.com/elastic/rally/pull/704): Add download subcommand
* [#700](https://github.com/elastic/rally/pull/700): Allow stdout print when running in Docker
* [#694](https://github.com/elastic/rally/pull/694): Measure execution time of bulk ingest pipeline
* [#691](https://github.com/elastic/rally/pull/691): Remove node_count_per_host

#### Bug Fixes

* [#710](https://github.com/elastic/rally/pull/710) (Breaking): Don't pass request_cache by default
* [#706](https://github.com/elastic/rally/pull/706): Register pyenv Python versions
* [#699](https://github.com/elastic/rally/pull/699): Fix home directory paths in provisioner_test.
* [#698](https://github.com/elastic/rally/pull/698): Enter degraded mode on bootstrap failure
* [#693](https://github.com/elastic/rally/pull/693): Assume UTC timezone if not specified

#### Doc Changes

* [#714](https://github.com/elastic/rally/pull/714): Document which ES versions are supported by Rally
* [#703](https://github.com/elastic/rally/pull/703): Document known issues in dev setup

### 1.1.0

#### Enhancements

* [#683](https://github.com/elastic/rally/pull/683): Store mean for response-related metrics
* [#681](https://github.com/elastic/rally/pull/681): Use single node discovery type if suitable
* [#679](https://github.com/elastic/rally/pull/679): Skip Python install if already existing
* [#678](https://github.com/elastic/rally/pull/678): Upgrade Jinja to 2.10.1
* [#674](https://github.com/elastic/rally/pull/674): [Experimental] Capture peak usage of JVM mem pools
* [#671](https://github.com/elastic/rally/pull/671): Add ability to download from S3 buckets
* [#670](https://github.com/elastic/rally/pull/670): Pass JAVA12_HOME in integration tests
* [#669](https://github.com/elastic/rally/pull/669): Allow to override request timeout for force-merge
* [#668](https://github.com/elastic/rally/pull/668): Chart generator segment memory and new track combinations
* [#667](https://github.com/elastic/rally/pull/667): Add sleep operation
* [#666](https://github.com/elastic/rally/pull/666): Introduce new command line parameter --track-revision
* [#662](https://github.com/elastic/rally/pull/662): Add distribution flavor to metrics metadata
* [#660](https://github.com/elastic/rally/pull/660): Add user tags when comparing races
* [#659](https://github.com/elastic/rally/pull/659): Sort the track parameters / user tags when listing races
* [#654](https://github.com/elastic/rally/pull/654): Allow to use ES 7.x as metrics store
* [#649](https://github.com/elastic/rally/pull/649) (Breaking): Pass request-params as is in supported operations
* [#648](https://github.com/elastic/rally/pull/648): Updates to support 7.x APIs

#### Bug Fixes

* [#687](https://github.com/elastic/rally/pull/687): Fix release charts after #684
* [#686](https://github.com/elastic/rally/pull/686): Honor track-params in chart-generator
* [#684](https://github.com/elastic/rally/pull/684): Use license instead of dist flavor in charts
* [#682](https://github.com/elastic/rally/pull/682): Provide a platform-independent Rally binary
* [#675](https://github.com/elastic/rally/pull/675): Fix chart generator segment memory bug
* [#673](https://github.com/elastic/rally/pull/673): Honor runtime JDK in provisioner
* [#661](https://github.com/elastic/rally/pull/661): Small change in the venv-create
* [#655](https://github.com/elastic/rally/pull/655): Add compatibility layer for existing metrics store
* [#652](https://github.com/elastic/rally/pull/652): Properly authenticate at proxy server

#### Doc Changes

* [#676](https://github.com/elastic/rally/pull/676): Document ccr-stats telemetry device
* [#651](https://github.com/elastic/rally/pull/651): fix typo in custom_scheduler example

### 1.0.4

#### Enhancements

* [#650](https://github.com/elastic/rally/pull/650): Use --prune for all git fetch operations
* [#647](https://github.com/elastic/rally/pull/647): Make types optional
* [#646](https://github.com/elastic/rally/pull/646): Add node_name in node-stats docs for ...
* [#644](https://github.com/elastic/rally/pull/644): Allow collection of jvm gc section in node-stats telemetry device
* [#642](https://github.com/elastic/rally/pull/642): Allow passing any request parameter to the create index API
* [#641](https://github.com/elastic/rally/pull/641): Temporarily disable Python 3.4 in build
* [#639](https://github.com/elastic/rally/pull/639): Add recovery-stats telemetry device
* [#637](https://github.com/elastic/rally/pull/637): Ignore JSON logs for merge parts analysis
* [#633](https://github.com/elastic/rally/pull/633): Ensure Makefile install target includes all release dependencies

#### Bug Fixes

* [#638](https://github.com/elastic/rally/pull/638): Correct recorder-based sampling interval

#### Doc Changes

* [#640](https://github.com/elastic/rally/pull/640): docs: Clarify that path should start with / for raw-request
* [#634](https://github.com/elastic/rally/pull/634): Fix typo

### 1.0.3

#### Enhancements

* [#630](https://github.com/elastic/rally/pull/630): Improve error message on missing repo directory
* [#627](https://github.com/elastic/rally/pull/627): Warn about skewed results when using node-stats telemetry device
* [#625](https://github.com/elastic/rally/pull/625): Allow to specify a team revision
* [#622](https://github.com/elastic/rally/pull/622): Include NOTICE.txt in release
* [#620](https://github.com/elastic/rally/pull/620): Add license headers
* [#617](https://github.com/elastic/rally/pull/617): Fix conflicting pipelines and distribution version
* [#615](https://github.com/elastic/rally/pull/615): Add refresh/merge/flush totals in summary
* [#612](https://github.com/elastic/rally/pull/612): Extract hits either as number or structured object
* [#606](https://github.com/elastic/rally/pull/606): Improve release.sh script and prerequisites
* [#604](https://github.com/elastic/rally/pull/604): Change logging level for esrally command line to DEBUG

#### Bug Fixes

* [#613](https://github.com/elastic/rally/pull/613): Error can be a string, and shards not always present in response.

#### Doc Changes

* [#632](https://github.com/elastic/rally/pull/632): Update Release documentation for Rally
* [#623](https://github.com/elastic/rally/pull/623): Update Developing Rally docs sections
* [#621](https://github.com/elastic/rally/pull/621): Minor typo
* [#610](https://github.com/elastic/rally/pull/610): Command fails with "=", ":" is needed.

### 1.0.2

#### Enhancements

* [#599](https://github.com/elastic/rally/pull/599): Simplify development setup
* [#594](https://github.com/elastic/rally/pull/594): Add make install as default target
* [#587](https://github.com/elastic/rally/pull/587): Fix typos and inconsistencies in help documentation
* [#576](https://github.com/elastic/rally/pull/576): Improved formatting of document count in list tracks output
* [#574](https://github.com/elastic/rally/pull/574): Rename benchmark data directory to root directory
* [#572](https://github.com/elastic/rally/pull/572): More fine-grained ML metrics
* [#571](https://github.com/elastic/rally/pull/571): Reduce logging when loading tracks
* [#570](https://github.com/elastic/rally/pull/570): Add ML API runners
* [#563](https://github.com/elastic/rally/pull/563): Make Rally compatible with Python 3.7

#### Bug Fixes

* [#586](https://github.com/elastic/rally/pull/586): Don't fail git clone is tty is not attached
* [#579](https://github.com/elastic/rally/pull/579): Switch to official approach for HTTP compression
* [#575](https://github.com/elastic/rally/pull/575): Always use remote branch for updates

#### Doc Changes

* [#598](https://github.com/elastic/rally/pull/598): Document --limit



### 1.0.1

#### Enhancements

* [#569](https://github.com/elastic/rally/pull/569): Add shrink index runner
* [#559](https://github.com/elastic/rally/pull/559): Write rendered track to a temporary file
* [#558](https://github.com/elastic/rally/pull/558): Avoid throughput throttling in test mode
* [#557](https://github.com/elastic/rally/pull/557): Adjust flight recorder flags for JDK 11
* [#556](https://github.com/elastic/rally/pull/556): Upgrade to Thespian 3.9.3
* [#555](https://github.com/elastic/rally/pull/555): Load only required corpora
* [#549](https://github.com/elastic/rally/pull/549): Exponential back-off for retrying transport errors to metrics store
* [#546](https://github.com/elastic/rally/pull/546): Update ccr stats endpoint
* [#545](https://github.com/elastic/rally/pull/545): Use non-standard port for metrics store in tests
* [#540](https://github.com/elastic/rally/pull/540): Simplify filtering node-stats-related documents
* [#538](https://github.com/elastic/rally/pull/538): Retry metric store for more transport errors

#### Bug Fixes

* [#565](https://github.com/elastic/rally/pull/565): Improve compatibility when loading invalid JSON
* [#562](https://github.com/elastic/rally/pull/562): Defer startup of mechanic actor
* [#560](https://github.com/elastic/rally/pull/560): Disable automatic log rotation
* [#551](https://github.com/elastic/rally/pull/551): Fix HTTP TLS client certificate verification
* [#544](https://github.com/elastic/rally/pull/544): Fix parsing of boolean values datastore.secure
* [#542](https://github.com/elastic/rally/pull/542): Warn when cluster settings cannot be applied
* [#537](https://github.com/elastic/rally/pull/537): make --test-mode work with uncompressed data #536

#### Doc Changes

* [#533](https://github.com/elastic/rally/pull/533): State clearly that only Unix is supported

### 1.0.0

#### Enhancements

* [#529](https://github.com/elastic/rally/pull/529): Allow to control recency of ids in conflicts
* [#523](https://github.com/elastic/rally/pull/523): Use a single shard for metrics indices
* [#522](https://github.com/elastic/rally/pull/522): Fine-tune logging
* [#521](https://github.com/elastic/rally/pull/521): Remove deprecated usage of parameter source
* [#518](https://github.com/elastic/rally/pull/518): Derive JDK version at runtime
* [#516](https://github.com/elastic/rally/pull/516): Add multi-cluster support for NodeStats telemetry device
* [#515](https://github.com/elastic/rally/pull/515): Fold all stats per shard in the same doc for CCR
* [#514](https://github.com/elastic/rally/pull/514): Report indexing times per shard
* [#513](https://github.com/elastic/rally/pull/513): Allow to store custom metric document format
* [#512](https://github.com/elastic/rally/pull/512): Make challenges optional
* [#509](https://github.com/elastic/rally/pull/509): Remove post_launch bootstrap phase
* [#508](https://github.com/elastic/rally/pull/508): Add node-stats-include-mem option to record jvm heap stats
* [#507](https://github.com/elastic/rally/pull/507): Set retry-on-timeout=true for ES clients used by Telemetry devices
* [#503](https://github.com/elastic/rally/pull/503): Use file-based logging configuration
* [#497](https://github.com/elastic/rally/pull/497): Add record_process_stats() for process.* stats

#### Bug Fixes

* [#526](https://github.com/elastic/rally/pull/526): Resolve artefact name based on download URL
* [#524](https://github.com/elastic/rally/pull/524): Disable interpolation for config file
* [#511](https://github.com/elastic/rally/pull/511): Don't fail if conflict-probability is zero
* [#510](https://github.com/elastic/rally/pull/510): Allow conflict-probability value of 0
* [#500](https://github.com/elastic/rally/pull/500): Convert exception to string in driver

#### Doc Changes

* [#499](https://github.com/elastic/rally/pull/499): Omit needless words in track tutorial
* [#494](https://github.com/elastic/rally/pull/494): Remove usage of "please" in docs

### 0.11.0

#### Enhancements

* [#493](https://github.com/elastic/rally/pull/493): Sample more node stats
* [#490](https://github.com/elastic/rally/pull/490): Record "took" for bulk indexing
* [#489](https://github.com/elastic/rally/pull/489): Get distribution download URL from car config
* [#488](https://github.com/elastic/rally/pull/488): Add support for multiple clusters in custom runners
* [#487](https://github.com/elastic/rally/pull/487): Add new Makefile target to run it tests inside Docker
* [#485](https://github.com/elastic/rally/pull/485): Allow to benchmark Elasticsearch with and without x-pack
* [#481](https://github.com/elastic/rally/pull/481): Add post_launch phase for Elasticsearch plugins
* [#480](https://github.com/elastic/rally/pull/480): Add generic node-stats telemetry device
* [#477](https://github.com/elastic/rally/pull/477): Improve simulation of bulk-indexing conflicts
* [#475](https://github.com/elastic/rally/pull/475): Rename telemetry device that gathers GC stats
* [#473](https://github.com/elastic/rally/pull/473): Don't pass type implicitly for queries

#### Bug Fixes

* [#476](https://github.com/elastic/rally/pull/476): Create unique ids even for random conflicts

### 0.10.1

#### Enhancements

* [#471](https://github.com/elastic/rally/pull/471): Enable actor message handlers to fail immediately

#### Bug Fixes

* [#470](https://github.com/elastic/rally/pull/470): Allow to use track parameters in index definition

### 0.10.0

#### Enhancements

* [#469](https://github.com/elastic/rally/pull/469): Align operation param names with runners
* [#468](https://github.com/elastic/rally/pull/468) (Breaking): Require at least Rally 0.7.3 for config migration
* [#466](https://github.com/elastic/rally/pull/466): Don't require git
* [#464](https://github.com/elastic/rally/pull/464): esrally must not log clear text user passwords
* [#462](https://github.com/elastic/rally/pull/462): Upgrade Elasticsearch client to 6.2.0
* [#461](https://github.com/elastic/rally/pull/461) (Breaking): Remove 'index-settings' property
* [#460](https://github.com/elastic/rally/pull/460): Report store and translog size
* [#458](https://github.com/elastic/rally/pull/458): Add parameter support to telemetry devices
* [#456](https://github.com/elastic/rally/pull/456) (Breaking): Remove automatic index management
* [#454](https://github.com/elastic/rally/pull/454): Allow to ingest a subset of a document corpus
* [#453](https://github.com/elastic/rally/pull/453) (Breaking): Remove operation type "index"
* [#441](https://github.com/elastic/rally/pull/441): Allow to pass parameters via a file
* [#440](https://github.com/elastic/rally/pull/440): Use curl retry features to workaround transient network problems

#### Bug Fixes

* [#467](https://github.com/elastic/rally/pull/467): Return "pages" as unit for scrolls
* [#465](https://github.com/elastic/rally/pull/465): Default to upgrading packages with setuptools
* [#449](https://github.com/elastic/rally/pull/449): Fix venv detection with venv and Rally execution on non master
* [#446](https://github.com/elastic/rally/pull/446): Use more specific name for mandatory plugin check selectively

#### Doc Changes

* [#459](https://github.com/elastic/rally/pull/459): Improve documentation of track repo URL
* [#457](https://github.com/elastic/rally/pull/457): Be build-tool agnostic

### 0.9.4

#### Enhancements

* [#437](https://github.com/elastic/rally/pull/437): Bump pytest* versions and beautify make test output
* [#434](https://github.com/elastic/rally/pull/434): Remove Python faulthandler
* [#431](https://github.com/elastic/rally/issues/431): Add a unique race id
* [#429](https://github.com/elastic/rally/issues/429): Allow to use track parameters also in index / template definitions
* [#426](https://github.com/elastic/rally/issues/426): Allow simpler configuration of cluster configuration
* [#377](https://github.com/elastic/rally/issues/377): Index-append operation only indexing bulk-size * clients documents

#### Bug Fixes

* [#423](https://github.com/elastic/rally/issues/423): esrally fails after initial config run if JDK9 is not installed

#### Doc Changes

* [#424](https://github.com/elastic/rally/pull/424): Clarify use of virtualenv in developing doc

### 0.9.3

#### Bug Fixes

* [#420](https://github.com/elastic/rally/issues/420): Cannot benchmark multi-node cluster with benchmark-only pipeline

### 0.9.2

#### Enhancements

* [#418](https://github.com/elastic/rally/issues/418): Measure Elasticsearch startup time
* [#417](https://github.com/elastic/rally/issues/417): Set heap dump path
* [#416](https://github.com/elastic/rally/issues/416): Improve logging when gathering index time related metrics
* [#413](https://github.com/elastic/rally/issues/413): Cannot report to ES cluster with self-signed certificate or in-house certificate authority
* [#411](https://github.com/elastic/rally/issues/411): Store Rally version in results meta-data
* [#410](https://github.com/elastic/rally/issues/410): Allow parameter sources to indicate progress
* [#409](https://github.com/elastic/rally/issues/409): Allow to override car variables
* [#407](https://github.com/elastic/rally/issues/407): Upgrade to thespian 3.9.2
* [#405](https://github.com/elastic/rally/pull/405): Improve internal message handling
* [#404](https://github.com/elastic/rally/issues/404): Add a raw request runner
* [#402](https://github.com/elastic/rally/issues/402): Enforce UTF-8 encoding for file I/O
* [#392](https://github.com/elastic/rally/issues/392): Formatting for user-tags on rally-results

#### Doc Changes

* [#415](https://github.com/elastic/rally/pull/415): Fix typo
* [#400](https://github.com/elastic/rally/pull/400): Fixing minor spelling error in car docs
* [#386](https://github.com/elastic/rally/issues/386): Dead GitHub Link on Rally Docs

### 0.9.1

#### Bug Fixes

* [#399](https://github.com/elastic/rally/issues/399): 0.9.0 - Unable to run race due to missing JDK9

### 0.9.0

#### Enhancements

* [#398](https://github.com/elastic/rally/issues/398): Allow to override plugin variables
* [#387](https://github.com/elastic/rally/issues/387): Require JDK 9 for source builds
* [#384](https://github.com/elastic/rally/issues/384): Provide a specific error message if data file is present but wrong size
* [#383](https://github.com/elastic/rally/issues/383): Abort current benchmark in case of attempted duplicate starts
* [#376](https://github.com/elastic/rally/issues/376): Record indexing throttle time
* [#366](https://github.com/elastic/rally/issues/366): Separate document corpus definition from indices
* [#361](https://github.com/elastic/rally/issues/361): Don't measure every operation
* [#359](https://github.com/elastic/rally/issues/359): Allow to define index settings per index
* [#348](https://github.com/elastic/rally/issues/348): Prefer default data directory when --track-path is given
* [#293](https://github.com/elastic/rally/issues/293): Allow users to explicitly define index management operations

#### Bug Fixes

* [#396](https://github.com/elastic/rally/issues/396): Rally requires a team repo even if used as a load generator
* [#394](https://github.com/elastic/rally/issues/394): Error in track preparation can hang benchmarks

#### Doc Changes

* [#397](https://github.com/elastic/rally/pull/397): Add a gentle reminder to update the Rally kernel with the important elasticsearch system configurations
* [#389](https://github.com/elastic/rally/issues/389): Syntax error in elasticsearch_plugins documentation
* [#388](https://github.com/elastic/rally/pull/388): Update rally daemon port requirements
* [#355](https://github.com/elastic/rally/issues/355): Run In Kubernetes

### 0.8.1

#### Enhancements

* [#375](https://github.com/elastic/rally/issues/375): Add refresh API

### 0.8.0

#### Highlights

* [#310](https://github.com/elastic/rally/issues/310): Introduce track parameters and allow to override them on the command line

#### Enhancements

* [#371](https://github.com/elastic/rally/issues/371): Introduce put pipeline API
* [#369](https://github.com/elastic/rally/issues/369): Prepare rename from operation-type "index" to "bulk"
* [#363](https://github.com/elastic/rally/issues/363): Allow to retry operations (internally)
* [#362](https://github.com/elastic/rally/issues/362): Make plugin remote URL optional
* [#360](https://github.com/elastic/rally/issues/360): Show ES distribution version upon race start
* [#358](https://github.com/elastic/rally/issues/358): Rally tables should always output the same rows
* [#354](https://github.com/elastic/rally/issues/354): Be more lenient when custom parameter source does not provide a parameter
* [#353](https://github.com/elastic/rally/issues/353): Make a few track properties optional
* [#352](https://github.com/elastic/rally/issues/352): Allow to use a challenge element if there is only one challenge in a track
* [#351](https://github.com/elastic/rally/issues/351) (Breaking): Ensure task names are unique
* [#345](https://github.com/elastic/rally/issues/345): Allow to use an ES distribution for plugins that are built separately
* [#326](https://github.com/elastic/rally/issues/326): Allow to define operations inline

#### Bug Fixes

* [#356](https://github.com/elastic/rally/issues/356) (Breaking): Set a sane default socket timeout
* [#350](https://github.com/elastic/rally/issues/350) (Breaking): Number of iterations should be treated per client, not "global"

#### Doc Changes

* [#364](https://github.com/elastic/rally/issues/364): Deprecate --cluster-health

### 0.7.4

#### Enhancements

* [#333](https://github.com/elastic/rally/issues/333): Provide a clear error message for non-existing releases
* [#331](https://github.com/elastic/rally/issues/331): Don't demand a compressed representation of the document corpus
* [#226](https://github.com/elastic/rally/issues/226): Provide a Rally package with all dependencies for offline install
* [#217](https://github.com/elastic/rally/issues/217): Add ability to continuously stream metrics

#### Bug Fixes

* [#344](https://github.com/elastic/rally/issues/344): Client assignment can miss a (small) number of docs
* [#338](https://github.com/elastic/rally/issues/338): Documents are not found if track is referenced via a file
* [#337](https://github.com/elastic/rally/issues/337): Only map numeric HTTP status codes to request meta-data
* [#334](https://github.com/elastic/rally/issues/334): Data partition algorithm failling with parallel indexing tasks

#### Doc Changes

* [#343](https://github.com/elastic/rally/issues/343): If python is required then python-dev should be as well
* [#231](https://github.com/elastic/rally/issues/231): Simplify usage of Rally for offline-only use

### 0.7.3

#### Enhancements

* [#332](https://github.com/elastic/rally/issues/332): Provide more information about errors
* [#330](https://github.com/elastic/rally/issues/330): Provide better error message when Rally reads an incompatible track
* [#329](https://github.com/elastic/rally/issues/329): Allow to skip cluster health check
* [#323](https://github.com/elastic/rally/issues/323): Rally should not demand a local Java install
* [#309](https://github.com/elastic/rally/issues/309): Allow to benchmark plugins from sources
* [#292](https://github.com/elastic/rally/issues/292): Add a "simple track" mode
* [#259](https://github.com/elastic/rally/issues/259): Prepare Rally for Java 9

#### Bug Fixes

* [#328](https://github.com/elastic/rally/issues/328): Challenge-specific index settings are not applied to index template definitions

### 0.7.2

#### Enhancements

* [#322](https://github.com/elastic/rally/issues/322): Add error-type to request meta-data
* [#321](https://github.com/elastic/rally/issues/321): Don't log body when creating a new index
* [#319](https://github.com/elastic/rally/issues/319): Upgrade to thespian 3.8.0
* [#315](https://github.com/elastic/rally/issues/315): Simplify multi-node configuration
* [#313](https://github.com/elastic/rally/issues/313): Allow car mixins
* [#265](https://github.com/elastic/rally/issues/265): Have Rally detect and use more Java versions

#### Bug Fixes

* [#318](https://github.com/elastic/rally/issues/318): Run tasks indefinitely

### 0.7.1

#### Highlights

* [#257](https://github.com/elastic/rally/issues/257): Distribute load-test driver

#### Bug Fixes

* [#316](https://github.com/elastic/rally/issues/316): KeyError: 'vm_vendor' when running benchmark against single host cluster with 0.7.0

### 0.7.0

#### Highlights

* [#71](https://github.com/elastic/rally/issues/71): Allow to benchmark multi-machine clusters
* [#60](https://github.com/elastic/rally/issues/60): Allow benchmarking with plugins

#### Enhancements

* [#314](https://github.com/elastic/rally/issues/314): Allow to filter executed tasks
* [#312](https://github.com/elastic/rally/issues/312) (Breaking): Move action-and-meta-data to (indexing) type definition
* [#308](https://github.com/elastic/rally/issues/308): Unify implementation of track and team repositories
* [#307](https://github.com/elastic/rally/issues/307): Check Python version on startup and fail if it does not meet requirements
* [#304](https://github.com/elastic/rally/issues/304): Make distribution repositories configurable
* [#296](https://github.com/elastic/rally/issues/296): Verify whether the number of documents in the track is correct
* [#278](https://github.com/elastic/rally/issues/278): relative-time should be reset per task
* [#228](https://github.com/elastic/rally/issues/228): Increased flexibility for termination of parallel tasks

### 0.6.2

#### Enhancements

* [#299](https://github.com/elastic/rally/pull/299): Leave the host parsing on elasticsearch-py
* [#227](https://github.com/elastic/rally/issues/227): Enhance flexibility of user tagging
* [#196](https://github.com/elastic/rally/issues/196) (Breaking): Externalize car configuration

#### Bug Fixes

* [#298](https://github.com/elastic/rally/issues/298): Rally froze at the end of a race, did not produce results

### 0.6.1

#### Enhancements

* [#295](https://github.com/elastic/rally/issues/295): Provide number of query hits as metrics metadata
* [#291](https://github.com/elastic/rally/issues/291): Show track size metrics when listing tracks
* [#290](https://github.com/elastic/rally/issues/290): Allow to pass arbitrary request parameters for ES queries
* [#286](https://github.com/elastic/rally/pull/286): Additional metrics for bulk requests
* [#282](https://github.com/elastic/rally/issues/282) (Breaking): Remove list facts subcommand

### 0.6.0

#### Highlights

* [#258](https://github.com/elastic/rally/issues/258): Make 'race' self-contained

#### Enhancements

* [#284](https://github.com/elastic/rally/issues/284) (Breaking): Disallow previously deprecated usage of track properties in meta block
* [#283](https://github.com/elastic/rally/issues/283): Store race results in a format optimized for reporting
* [#279](https://github.com/elastic/rally/issues/279) (Breaking): Separate race and metrics indices
* [#276](https://github.com/elastic/rally/issues/276): Add affected index to meta-data returned by bulk index runner
* [#275](https://github.com/elastic/rally/issues/275): Allow to define per-challenge cluster-settings
* [#267](https://github.com/elastic/rally/issues/267): Provide a progress-indication for downloads
* [#266](https://github.com/elastic/rally/issues/266): Make the gc telemetry device Java 9 compatible
* [#246](https://github.com/elastic/rally/issues/246): Rally should print a warning if there are no measurement samples
* [#223](https://github.com/elastic/rally/issues/223): Allow unlimited number of pages for scroll queries
* [#222](https://github.com/elastic/rally/issues/222): Report number of hits, not just pages, for scroll queries
* [#220](https://github.com/elastic/rally/issues/220) (Breaking): Allow monthly indices for metrics
* [#138](https://github.com/elastic/rally/issues/138) (Breaking): Improve support Elasticsearch 5 as metrics store

#### Bug Fixes

* [#285](https://github.com/elastic/rally/issues/285): Rally is stuck for benchmarks with a very large number of requests
* [#280](https://github.com/elastic/rally/issues/280): The file-reader parameter source does not seem to pass additional parameters

#### Doc Changes

* [#288](https://github.com/elastic/rally/issues/288): Clarify usage of remote track repositories in docs
* [#287](https://github.com/elastic/rally/issues/287): Document throughput semantics for each operation
* [#274](https://github.com/elastic/rally/issues/274): Explain summary report output
* [#271](https://github.com/elastic/rally/issues/271): Document how to implement custom runner classes
* [#264](https://github.com/elastic/rally/issues/264): Documentation on settings and warning message when using a documents archive file

### 0.5.3

#### Highlights

* [#251](https://github.com/elastic/rally/issues/251): Support for non-deterministic distribution of operations

#### Enhancements

* [#260](https://github.com/elastic/rally/issues/260): Have bulk parameter source provide the bulk size
* [#249](https://github.com/elastic/rally/issues/249): Saving tournament report
* [#155](https://github.com/elastic/rally/issues/155): Improve document count handling

#### Bug Fixes

* [#263](https://github.com/elastic/rally/issues/263): Race condition when rolling log files
* [#261](https://github.com/elastic/rally/issues/261): Unable to determine valid external socket address
* [#253](https://github.com/elastic/rally/issues/253): Cannot determine CPU info for POWER8 chips
* [#242](https://github.com/elastic/rally/issues/242): Rally does not detect the distribution version correctly for externally provisioned clusters
* [#235](https://github.com/elastic/rally/issues/235): Allow Python files in a track directory that are unrelated to the track plugin

#### Doc Changes

* [#256](https://github.com/elastic/rally/issues/256): Document additional decompression options
* [#247](https://github.com/elastic/rally/issues/247): Description of available operation types in the docs
* [#241](https://github.com/elastic/rally/issues/241): Clearly document which Elasticsearch versions are supported

### 0.5.2

#### Enhancements

* [#244](https://github.com/elastic/rally/issues/244): Show a nice error message when user tries to run a non-existing challenge on an existing track

#### Bug Fixes

* [#245](https://github.com/elastic/rally/issues/245): Connection behind a proxy fails. has_internet_connection() returns False

### 0.5.1

#### Enhancements

* [#234](https://github.com/elastic/rally/issues/234): Add request error rate to summary report

### 0.5.0

#### Highlights

* [#238](https://github.com/elastic/rally/issues/238): Simplify gathering of facts for benchmark results sharing
* [#216](https://github.com/elastic/rally/issues/216): Add possibility to profile load driver internals
* [#184](https://github.com/elastic/rally/issues/184): Allow to benchmark a single-machine cluster remotely

#### Enhancements

* [#207](https://github.com/elastic/rally/issues/207): Improve response checks in bulk API runner
* [#205](https://github.com/elastic/rally/issues/205): Make track configuration more modular
* [#200](https://github.com/elastic/rally/issues/200): Allow root.dir to be set during configure
* [#199](https://github.com/elastic/rally/issues/199): Let track authors decide on the default challenge
* [#191](https://github.com/elastic/rally/issues/191): Support target-interval for operations

#### Bug Fixes

* [#225](https://github.com/elastic/rally/issues/225): Final score report blank when metrics store specified
* [#221](https://github.com/elastic/rally/issues/221): Scrolls fail against Elasticsearch master
* [#203](https://github.com/elastic/rally/issues/203): Index time metrics are not aware of laps
* [#202](https://github.com/elastic/rally/issues/202): Bulk index source reads data for all indices

#### Doc Changes

* [#224](https://github.com/elastic/rally/issues/224): Issue(s) with --user-tags
* [#214](https://github.com/elastic/rally/issues/214): Improve documentation of "parallel"
* [#213](https://github.com/elastic/rally/issues/213): Document how to support --test-mode in a track
* [#208](https://github.com/elastic/rally/issues/208): Add FAQ item to explain latency, service time and their relation to "took"

### 0.4.0

#### Breaking changes in 0.4.0

The track format has changed a bit due a more flexible approach in how benchmarks are executed:
 
* Operations are defined in the `operations` section, execution details like number of warmup iterations, warmup time etc. are defined as part of the `schedule`.
* Each query needs to be defined as a separate operation and referenced in the `schedule`
* You can (and in fact should) specify a `warmup-time-period` (defined in sections) for bulk index operations. The warmup time period is specified in seconds.

For details please refer to the updated [JSON schema for Rally tracks](https://github.com/elastic/rally/blob/master/esrally/resources/track-schema.json).

Hint: This is just relevant for you, if you have defined your own tracks. We already took care of updating the [official Rally tracks](https://github.com/elastic/rally-tracks).

[All changes](https://github.com/elastic/rally/issues?q=milestone0X0P+00.4.0+is0X0P+0closed)

### 0.3.0

#### Breaking changes in 0.3

We have [separated the previously known "track setup" into two parts](https://github.com/elastic/rally/issues/101):

* Challenges: Which describe what happens during a benchmark (whether to index or search and with which parameters)
* Cars: Which describe the benchmark candidate settings (e.g. heap size, logging configuration etc.)

This influences the command line interface in a couple of ways:

* To list all known cars, we have added a new command `esrally list cars`. To select a challenge, use now `--challenge` instead of `--track-setup` and also specify a car now with `--car`.
* Tournaments created by older versions of Rally are incompatible
* Rally must now be invoked with only one challenge and only one car (previously it was possible to specify multiple track setups)

We have also [moved tracks](https://github.com/elastic/rally/issues/69) to a [dedicated repository](https://github.com/elastic/rally-tracks). This allows you to support tracks for multiple versions of Elasticsearch but also requires that all users have `git` installed.
 
[All changes](https://github.com/elastic/rally/issues?q=milestone0X0P+00.3.0+is0X0P+0closed)

#### Simplified configuration in 0.3

We have spent a lot of time to simplify first time setup of Rally. For starters, you are not required to setup your own metrics store if you don't need it. 
However, you are then just able to run individual benchmarks but you cannot compare results or visualize anything in Kibana. If you don't need this, it is recommended that you
remove the configuration directory and run `esrally configure`. Rally will notify you on its first start of this change and guide you through the process.

Please raise a ticket in case of problems.

### 0.2.1

* Add a [tournament mode](https://github.com/elastic/rally/issues/57). More information in the [user docs](https://esrally.readthedocs.io/en/latest/tournament.html)
* [External benchmarks can now specify target hosts and ports](https://github.com/elastic/rally/issues/83)
* Ability to [add a user-defined tag as metric meta-data](https://github.com/elastic/rally/issues/84)
* [Support .gzipped benchmark data](https://github.com/elastic/rally/issues/87) contributed by @monk-ee. Thanks!
* [Support for perf profiler](https://github.com/elastic/rally/issues/28)
* [Add a fulltext benchmark](https://github.com/elastic/rally/issues/38)

[All changes](https://github.com/elastic/rally/issues?q=milestone0X0P+00.2.1+is0X0P+0closed)

### 0.2.0

Major changes:

* Rally can now [benchmark a binary Elasticsearch distribution](rally/issues#63) (starting with Elasticsearch 5.0.0-alpha1).
* Reporting improvements for [query latency](elastic/rally#10) and [indexing throughput](elastic/rally#59) on the command line.
* We store [benchmark environment data](elastic/rally#54) alongside metrics.
* A new percolator track](elastic/rally#74) contributed by [Martijn van Groningen](https://github.com/martijnvg). Thanks!

[All changes](https://github.com/elastic/rally/issues?q=milestone0X0P+00.2.0+is0X0P+0closed)

### 0.1.0

Major changes:

* Added a [JIT profiler](https://github.com/elastic/rally/issues/43). This allows to check warmup times but also in-depth inspection which
optimizations were performed by the JIT compiler. If the HotSpot disassembler library is available, the logs will also contain the 
disassembled JIT compiler output which can be used for low-level analysis. We recommend to use 
[JITWatch](https://github.com/AdoptOpenJDK/jitwatch) for analysis.
* Added [pipeline support](https://github.com/elastic/rally/issues/61). Pipelines allow to define more flexibly which steps Rally executes
during a benchmark. One of the use-cases for this is to run a benchmark based on a released build of Elasticsearch rather than building it
ourselves.

[All changes](https://github.com/elastic/rally/issues?q=milestone0X0P+00.1.0+is0X0P+0closed)

### 0.0.3

Major changes:

* Migrated the metrics data store from file-based to a dedicated Elasticsearch instance. Graphical reports can be created with
  Kibana (optional but recommended). It is necessary to setup an Elasticsearch cluster to store metrics data (a single node
  is sufficient). The cluster will be configured automatically by Rally. For details please see the documentation.

  Related issues: #8, #21, #46,

[All changes](https://github.com/elastic/rally/issues?q=milestone0X0P+00.0.3+is0X0P+0closed)