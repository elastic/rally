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


def java_home(car, cfg):
    def determine_runtime_jdks(car):
        override_runtime_jdk = cfg.opts("mechanic", "runtime.jdk")
        if override_runtime_jdk:
            return [override_runtime_jdk]
        else:
            runtime_jdks = car.mandatory_var("runtime.jdk")
            try:
                return [int(v) for v in runtime_jdks.split(",")]
            except ValueError:
                raise exceptions.SystemSetupError(
                    "Car config key \"runtime.jdk\" is invalid: \"{}\" (must be int)".format(runtime_jdks))

    logger = logging.getLogger(__name__)

    runtime_jdk_versions = determine_runtime_jdks(car)
    logger.info("Allowed JDK versions are %s.", runtime_jdk_versions)
    major, java_home = jvm.resolve_path(runtime_jdk_versions)
    logger.info("Detected JDK with major version [%s] in [%s].", major, java_home)
    return major, java_home
