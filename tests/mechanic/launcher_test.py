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
import io
import os
import uuid
from datetime import datetime
from unittest import TestCase, mock

import psutil

from esrally import config, paths, telemetry
from esrally.mechanic import launcher, team
from esrally.mechanic.provisioner import NodeConfiguration
from esrally.mechanic.team import Car
from esrally.metrics import InMemoryMetricsStore


class MockClientFactory:
    def __init__(self, hosts, client_options):
        self.client_options = client_options

    def create(self):
        return MockClient(self.client_options)


class MockClient:
    def __init__(self, client_options):
        self.client_options = client_options
        self.cluster = SubClient({
            "cluster_name": "rally-benchmark-cluster",
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "Nefarius",
                    "host": "127.0.0.1"
                }
            }
        })
        self.nodes = SubClient({
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "Nefarius",
                    "host": "127.0.0.1",
                    "os": {
                        "name": "Mac OS X",
                        "version": "10.11.4",
                        "available_processors": 8
                    },
                    "jvm": {
                        "version": "1.8.0_74",
                        "vm_vendor": "Oracle Corporation"
                    }
                }
            }
        })
        self._info = {
            "version":
                {
                    "number": "5.0.0",
                    "build_hash": "abc123"
                }
        }

    def info(self):
        if self.client_options.get("raise-error-on-info", False):
            import elasticsearch
            raise elasticsearch.TransportError(401, "Unauthorized")
        return self._info

    def search(self, *args, **kwargs):
        return {}


class SubClient:
    def __init__(self, info):
        self._info = info

    def stats(self, *args, **kwargs):
        return self._info

    def info(self, *args, **kwargs):
        return self._info


class MockPopen:
    def __init__(self, *args, **kwargs):
        # Currently, the only code that checks returncode directly during
        # ProcessLauncherTests are telemetry.  If we return 1 for them we can skip
        # mocking them as their optional functionality is disabled.
        self.returncode = 1
        self.stdout = io.StringIO()

    def communicate(self, input=None, timeout=None):
        return [b"", b""]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # process.run_subprocess_with_logging uses Popen context manager, so let its return be 0
        self.returncode = 0

    def wait(self):
        return 0


class MockProcess:
    def __init__(self, pid):
        self.pid = pid
        self.killed = False

    def name(self):
        return "p{pid}".format(pid=self.pid)

    def wait(self, timeout=None):
        raise psutil.TimeoutExpired(timeout)

    def kill(self):
        self.killed = True


def get_metrics_store(cfg):
    ms = InMemoryMetricsStore(cfg)
    ms.open(race_id=str(uuid.uuid4()),
            race_timestamp=datetime.now(),
            track_name="test",
            challenge_name="test",
            car_name="test")
    return ms


MOCK_PID_VALUE = 1234


class ProcessLauncherTests(TestCase):
    @mock.patch('os.kill')
    @mock.patch('subprocess.Popen', new=MockPopen)
    @mock.patch('esrally.mechanic.java_resolver.java_home', return_value=(12, "/java_home/"))
    @mock.patch('esrally.utils.jvm.supports_option', return_value=True)
    @mock.patch('esrally.utils.io.get_size')
    @mock.patch('os.chdir')
    @mock.patch('esrally.mechanic.launcher.wait_for_pidfile', return_value=MOCK_PID_VALUE)
    @mock.patch('psutil.Process', new=MockProcess)
    def test_daemon_start_stop(self, wait_for_pidfile, chdir, get_size, supports, java_home, kill):
        cfg = config.Config()
        cfg.add(config.Scope.application, "node", "root.dir", "test")
        cfg.add(config.Scope.application, "mechanic", "keep.running", False)
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "telemetry", "params", None)
        cfg.add(config.Scope.application, "system", "env.name", "test")

        ms = get_metrics_store(cfg)
        proc_launcher = launcher.ProcessLauncher(cfg, ms, paths.races_root(cfg))

        node_config = NodeConfiguration(car=Car("default", root_path=None, config_paths=[]), ip="127.0.0.1", node_name="testnode",
                                        node_root_path="/tmp", binary_path="/tmp", log_path="/tmp", data_paths="/tmp")

        nodes = proc_launcher.start([node_config])
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].pid, MOCK_PID_VALUE)

        proc_launcher.stop(nodes)
        self.assertTrue(kill.called)
             
    def test_env_options_order(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "keep.running", False)
        cfg.add(config.Scope.application, "system", "env.name", "test")

        ms = get_metrics_store(cfg)
        proc_launcher = launcher.ProcessLauncher(cfg, ms, races_root_dir="/home")
        default_car = team.Car(names="default-car", root_path=None, config_paths=["/tmp/rally-config"])
        
        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params={}, log_root="/tmp/telemetry", java_major_version=8)
            ]
        t = telemetry.Telemetry(["jfr"], devices=node_telemetry)
        env = proc_launcher._prepare_env(car=default_car, node_name="node0", java_home="/java_home", t=t)

        self.assertEqual("/java_home/bin" + os.pathsep + os.environ["PATH"], env["PATH"])
        self.assertEqual("-XX:+ExitOnOutOfMemoryError -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints " 
                         "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder "
                         "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath=/tmp/telemetry/default-car-node0.jfr "
                         "-XX:StartFlightRecording=defaultrecording=true", env["ES_JAVA_OPTS"])
