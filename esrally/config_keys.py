# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Named constants for configuration keys.

Instead of scattering hard-coded strings for config keys all over the
codebase (which is error-prone, as a typo does not raise an error), we
define the keys as module-level constants so a typo becomes a real
error (`AttributeError` / ``NameError``) instead of a silently missing
config option.

See https://github.com/elastic/rally/issues/1646.
"""

import typing

# section: "system"
SYSTEM_SECTION: typing.Final = "system"

# `rally list` options
LIST_CONFIG_OPTION: typing.Final = "list.config.option"
LIST_MAX_RESULTS: typing.Final = "list.max_results"
LIST_RACES_BENCHMARK_NAME: typing.Final = "list.races.benchmark_name"
LIST_RACES_FORMAT: typing.Final = "list.races.format"
LIST_RACES_USER_TAGS: typing.Final = "list.races.user_tags"
LIST_FROM_DATE: typing.Final = "list.from_date"
LIST_TO_DATE: typing.Final = "list.to_date"
LIST_CHALLENGE: typing.Final = "list.challenge"

# `rally delete` options
DELETE_CONFIG_OPTION: typing.Final = "delete.config.option"
DELETE_ID: typing.Final = "delete.id"

# `rally install` options
INSTALL_ID: typing.Final = "install.id"

# `rally add` options (add results to an existing race)
ADD_CONFIG_OPTION: typing.Final = "add.config.option"
ADD_MESSAGE: typing.Final = "add.message"
ADD_RACE_TIMESTAMP: typing.Final = "add.race_timestamp"
ADD_CHART_TYPE: typing.Final = "add.chart_type"
ADD_CHART_NAME: typing.Final = "add.chart_name"

# administrative options
ADMIN_TRACK: typing.Final = "admin.track"
ADMIN_DRY_RUN: typing.Final = "admin.dry_run"

# race-related options
RACE_ID: typing.Final = "race.id"
TIME_START: typing.Final = "time.start"
ENV_NAME: typing.Final = "env.name"

# runtime/behavior options
OFFLINE_MODE: typing.Final = "offline.mode"
QUIET_MODE: typing.Final = "quiet.mode"
AVAILABLE_CORES: typing.Final = "available.cores"
ASYNCHRONOUS_DEBUG: typing.Final = "async.debug"
PASSENV: typing.Final = "passenv"

# remote benchmarking support
REMOTE_BENCHMARKING_SUPPORTED: typing.Final = "remote.benchmarking.supported"
