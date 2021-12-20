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
import os

import it


@it.random_rally_config
def test_track_dependencies(cfg):
    port = 19200
    it.wait_until_port_is_free(port_number=port)
    dist_version = it.DISTRIBUTIONS[-1]
    # workaround for MacOS and Python deficiency. See http://sealiesoftware.com/blog/archive/2017/6/5/Objective-C_and_fork_in_macOS_1013.html
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
    assert it.race(cfg, f"--distribution-version={dist_version} --track-path=resources/track_with_dependency --kill-running-processes") == 0
    del os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"]
