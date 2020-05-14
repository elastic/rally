# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging

from esrally import exceptions
from esrally.utils import jvm


def java_home(car_runtime_jdks, specified_runtime_jdk=None, provides_bundled_jdk=False):
    def determine_runtime_jdks():
        if specified_runtime_jdk:
            return [specified_runtime_jdk]
        else:
            return allowed_runtime_jdks

    logger = logging.getLogger(__name__)

    try:
        allowed_runtime_jdks = [int(v) for v in car_runtime_jdks.split(",")]
    except ValueError:
        raise exceptions.SystemSetupError(
            "Car config key \"runtime.jdk\" is invalid: \"{}\" (must be int)".format(car_runtime_jdks))

    runtime_jdk_versions = determine_runtime_jdks()
    if runtime_jdk_versions[0] == "bundled":
        if not provides_bundled_jdk:
            raise exceptions.SystemSetupError("This Elasticsearch version does not contain a bundled JDK. "
                                              "Please specify a different runtime JDK.")
        logger.info("Using JDK bundled with Elasticsearch.")
        # assume that the bundled JDK is the highest available; the path is irrelevant
        return allowed_runtime_jdks[0], None
    else:
        logger.info("Allowed JDK versions are %s.", runtime_jdk_versions)
        major, java_home = jvm.resolve_path(runtime_jdk_versions)
        logger.info("Detected JDK with major version [%s] in [%s].", major, java_home)
        return major, java_home
