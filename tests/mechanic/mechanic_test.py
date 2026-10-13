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

# pylint: disable=protected-access
import asyncio
from unittest import mock

import pytest

from esrally import actor, config, exceptions
from esrally.mechanic import mechanic
from esrally.utils import opts


class TestHostHandling:
    @mock.patch("esrally.utils.net.resolve")
    def test_converts_valid_hosts(self, resolver):
        resolver.side_effect = ["127.0.0.1", "10.16.23.5", "11.22.33.44"]

        hosts = [
            {"host": "127.0.0.1", "port": 9200},
            # also applies default port if none given
            {"host": "10.16.23.5"},
            {"host": "site.example.com", "port": 9200},
        ]

        assert mechanic.to_ip_port(hosts) == [
            ("127.0.0.1", 9200),
            ("10.16.23.5", 9200),
            ("11.22.33.44", 9200),
        ]

    @mock.patch("esrally.utils.net.resolve")
    def test_rejects_hosts_with_unexpected_properties(self, resolver):
        resolver.side_effect = ["127.0.0.1", "10.16.23.5", "11.22.33.44"]

        hosts = [
            {"host": "127.0.0.1", "port": 9200, "ssl": True},
            {"host": "10.16.23.5", "port": 10200},
            {"host": "site.example.com", "port": 9200},
        ]

        with pytest.raises(exceptions.SystemSetupError) as exc:
            mechanic.to_ip_port(hosts)
        assert exc.value.args[0] == (
            "When specifying nodes to be managed by Rally you can only supply hostname:port pairs (e.g. 'localhost:9200'), "
            "any additional options cannot be supported."
        )

    def test_groups_nodes_by_host(self):
        ip_port = [
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("10.16.23.5", 9200),
            ("11.22.33.44", 9200),
            ("11.22.33.44", 9200),
        ]
        assert mechanic.nodes_by_host(ip_port) == {
            ("127.0.0.1", 9200): [0, 1, 2],
            ("10.16.23.5", 9200): [3],
            ("11.22.33.44", 9200): [4, 5],
        }

    def test_extract_all_node_ips(self):
        ip_port = [
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("10.16.23.5", 9200),
            ("11.22.33.44", 9200),
            ("11.22.33.44", 9200),
        ]
        assert mechanic.extract_all_node_ips(ip_port) == {
            "127.0.0.1",
            "10.16.23.5",
            "11.22.33.44",
        }


class TestMechanic:
    class Node:
        def __init__(self, node_name):
            self.node_name = node_name

    class MockLauncher:
        def __init__(self):
            self.started = False

        def start(self, node_configs):
            self.started = True
            return [TestMechanic.Node(f"rally-node-{n}") for n in range(len(node_configs))]

        def stop(self, nodes, metrics_store):
            self.started = False

    # We stub irrelevant methods for the test
    class MockMechanic(mechanic.Mechanic):
        def _current_race(self):
            return "race 17"

        def _add_results(self, current_race, node):
            pass

    @mock.patch("esrally.mechanic.provisioner.cleanup")
    def test_start_stop_nodes(self, cleanup):
        def supplier():
            return "/home/user/src/elasticsearch/es.tar.gz"

        provisioners = [mock.Mock(), mock.Mock()]
        launcher = self.MockLauncher()
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "race.id", "17")
        cfg.add(config.Scope.application, "mechanic", "preserve.install", False)
        metrics_store = mock.Mock()
        m = self.MockMechanic(cfg, metrics_store, supplier, provisioners, launcher)
        m.start_engine()
        assert launcher.started
        for p in provisioners:
            assert p.prepare.called

        m.stop_engine()
        assert not launcher.started
        assert cleanup.call_count == 2


class TestMechanicCoordinator:
    @pytest.fixture
    def cfg(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "client", "hosts", opts.TargetHosts("10.5.5.11:9200,10.5.5.11:9201,127.0.0.1:9200"))
        cfg.add(config.Scope.application, "mechanic", "repository.revision", "abc123")
        return cfg

    @pytest.fixture(autouse=True)
    def no_team(self, monkeypatch):
        monkeypatch.setattr(mechanic, "load_team", mock.Mock(return_value=(None, [])))
        monkeypatch.setattr(mechanic.net, "resolve", lambda host: host)

    @pytest.mark.asyncio
    async def test_does_not_start_externally_provisioned_cluster(self, cfg, fake_ray):
        coordinator = mechanic.MechanicCoordinator(cfg)

        assert await coordinator.start_engine({"race-id": "1"}, external=True) == "abc123"
        await coordinator.stop_engine()

        assert fake_ray.created == []

    @pytest.mark.asyncio
    async def test_starts_one_node_mechanic_per_host_and_port(self, cfg, fake_ray):
        coordinator = mechanic.MechanicCoordinator(cfg)

        assert await coordinator.start_engine({"race-id": "1"}, distribution=True) == "abc123"

        assert fake_ray.required_hosts == ["10.5.5.11", "10.5.5.11", "127.0.0.1"]
        created = fake_ray.created_of(mechanic.NodeMechanicActor)
        assert [(c.kwargs["host"], c.kwargs["name"]) for c in created] == [
            ("10.5.5.11", "node-mechanic-10.5.5.11-9200"),
            ("10.5.5.11", "node-mechanic-10.5.5.11-9201"),
            ("127.0.0.1", "node-mechanic-127.0.0.1-9200"),
        ]
        start_nodes = [c.handle.calls_to("start_nodes")[0][0][0] for c in created]
        assert [(s.ip, s.port, s.node_ids) for s in start_nodes] == [
            ("10.5.5.11", 9200, [0]),
            ("10.5.5.11", 9201, [1]),
            ("127.0.0.1", 9200, [2]),
        ]
        assert all(s.all_node_ips == {"10.5.5.11", "127.0.0.1"} for s in start_nodes)
        assert all(s.all_node_ids == {0, 1, 2} for s in start_nodes)
        assert all(s.distribution and not s.sources for s in start_nodes)
        assert coordinator.node_mechanics == [c.handle for c in created]

    @pytest.mark.asyncio
    async def test_fails_if_a_node_cannot_be_started(self, cfg, fake_ray):
        fake_ray.behaviors[mechanic.NodeMechanicActor] = {"start_nodes": actor.BenchmarkFailure("Error in mechanic")}
        coordinator = mechanic.MechanicCoordinator(cfg)

        with pytest.raises(actor.BenchmarkFailure, match="Error in mechanic"):
            await coordinator.start_engine({"race-id": "1"}, distribution=True)

    @pytest.mark.asyncio
    async def test_stops_all_nodes_even_if_one_fails_to_stop(self, cfg, fake_ray):
        coordinator = mechanic.MechanicCoordinator(cfg)
        await coordinator.start_engine({"race-id": "1"}, distribution=True)
        node_mechanics = list(coordinator.node_mechanics)
        node_mechanics[0].behaviors["stop_nodes"] = actor.BenchmarkFailure("Error in mechanic")

        await coordinator.stop_engine()

        assert all(m.calls_to("stop_nodes") == [((), {})] for m in node_mechanics)
        assert fake_ray.killed == node_mechanics
        assert coordinator.node_mechanics == []

    @pytest.mark.asyncio
    async def test_check_health_raises_background_failures(self, cfg, fake_ray):
        coordinator = mechanic.MechanicCoordinator(cfg)
        await coordinator.start_engine({"race-id": "1"}, distribution=True)
        await coordinator.check_health()

        coordinator.node_mechanics[1].behaviors["health"] = actor.BenchmarkFailure("Error in mechanic")
        with pytest.raises(actor.BenchmarkFailure, match="Error in mechanic"):
            await coordinator.check_health()


@pytest.mark.usefixtures("actor_environment")
class TestNodeMechanicActor:
    @pytest.mark.asyncio
    async def test_flushes_metrics_periodically_and_stops_nodes(self, monkeypatch):
        monkeypatch.setattr(mechanic, "METRIC_FLUSH_INTERVAL_SECONDS", 0.01)
        node_mechanic = mechanic.NodeMechanicActor(config.Config())
        node_mechanic.mechanic = mock.create_autospec(mechanic.Mechanic, instance=True)
        m = node_mechanic.mechanic
        node_mechanic._flush_task = asyncio.create_task(node_mechanic._flush_metrics_periodically())

        await asyncio.sleep(0.05)
        assert m.flush_metrics.called
        node_mechanic.reset_relative_time()
        m.reset_relative_time.assert_called_once_with()

        await node_mechanic.stop()
        m.stop_engine.assert_called_once_with()
        assert node_mechanic.mechanic is None
        assert node_mechanic.health() is None

    @pytest.mark.asyncio
    async def test_reports_failures_while_flushing_metrics(self, monkeypatch):
        monkeypatch.setattr(mechanic, "METRIC_FLUSH_INTERVAL_SECONDS", 0.01)
        node_mechanic = mechanic.NodeMechanicActor(config.Config())
        node_mechanic.mechanic = mock.create_autospec(mechanic.Mechanic, instance=True)
        node_mechanic.mechanic.flush_metrics.side_effect = OSError("metrics store unavailable")

        await node_mechanic._flush_metrics_periodically()

        failure = node_mechanic.health()
        assert isinstance(failure, actor.BenchmarkFailure)
        assert failure.message == "Error in mechanic"
