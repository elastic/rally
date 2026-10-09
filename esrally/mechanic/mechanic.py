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

import asyncio
import contextlib
import json
import logging
import os
import pickle
from dataclasses import dataclass
from typing import Optional

from esrally import PROGRAM_NAME, actor, config, exceptions, metrics, paths, types
from esrally.mechanic import launcher, provisioner, supplier, team
from esrally.utils import console, net

METRIC_FLUSH_INTERVAL_SECONDS = 30


def build(cfg: types.Config):
    car, plugins = load_team(cfg, external=False)

    s = supplier.create(cfg, sources=True, distribution=False, car=car, plugins=plugins)
    binaries = s()
    console.println(json.dumps(binaries, indent=2), force=True)


def download(cfg: types.Config):
    car, plugins = load_team(cfg, external=False)

    s = supplier.create(cfg, sources=False, distribution=True, car=car, plugins=plugins)
    binaries = s()
    console.println(json.dumps(binaries, indent=2), force=True)


def install(cfg: types.Config):
    root_path = paths.install_root(cfg)
    car, plugins = load_team(cfg, external=False)

    # A non-empty distribution-version is provided
    distribution = bool(cfg.opts("mechanic", "distribution.version", mandatory=False))
    sources = not distribution
    build_type = cfg.opts("mechanic", "build.type")
    ip = cfg.opts("mechanic", "network.host")
    http_port = int(cfg.opts("mechanic", "network.http.port"))
    node_name = cfg.opts("mechanic", "node.name")
    master_nodes = cfg.opts("mechanic", "master.nodes")
    seed_hosts = cfg.opts("mechanic", "seed.hosts")

    if build_type == "tar":
        binary_supplier = supplier.create(cfg, sources, distribution, car, plugins)
        p = provisioner.local(
            cfg=cfg,
            car=car,
            plugins=plugins,
            ip=ip,
            http_port=http_port,
            all_node_ips=seed_hosts,
            all_node_names=master_nodes,
            target_root=root_path,
            node_name=node_name,
        )
        node_config = p.prepare(binary=binary_supplier())
    elif build_type == "docker":
        if len(plugins) > 0:
            raise exceptions.SystemSetupError(
                'You cannot specify any plugins for Docker clusters. Please remove "--elasticsearch-plugins" and try again.'
            )
        p = provisioner.docker(cfg=cfg, car=car, ip=ip, http_port=http_port, target_root=root_path, node_name=node_name)
        # there is no binary for Docker that can be downloaded / built upfront
        node_config = p.prepare(binary=None)
    else:
        raise exceptions.SystemSetupError(f"Unknown build type [{build_type}]")

    provisioner.save_node_configuration(root_path, node_config)
    console.println(json.dumps({"installation-id": cfg.opts("system", "install.id")}, indent=2), force=True)


def start(cfg: types.Config):
    root_path = paths.install_root(cfg)
    race_id = cfg.opts("system", "race.id")
    # avoid double-launching - we expect that the node file is absent
    with contextlib.suppress(FileNotFoundError):
        _load_node_file(root_path)
        install_id = cfg.opts("system", "install.id")
        raise exceptions.SystemSetupError(
            "A node with this installation id is already running. Please stop it first "
            "with {} stop --installation-id={}".format(PROGRAM_NAME, install_id)
        )

    node_config = provisioner.load_node_configuration(root_path)

    if node_config.build_type == "tar":
        node_launcher = launcher.ProcessLauncher(cfg)
    elif node_config.build_type == "docker":
        node_launcher = launcher.DockerLauncher(cfg)
    else:
        raise exceptions.SystemSetupError(f"Unknown build type [{node_config.build_type}]")
    nodes = node_launcher.start([node_config])
    _store_node_file(root_path, (nodes, race_id))


def stop(cfg: types.Config):
    root_path = paths.install_root(cfg)
    node_config = provisioner.load_node_configuration(root_path)
    if node_config.build_type == "tar":
        node_launcher = launcher.ProcessLauncher(cfg)
    elif node_config.build_type == "docker":
        node_launcher = launcher.DockerLauncher(cfg)
    else:
        raise exceptions.SystemSetupError(f"Unknown build type [{node_config.build_type}]")

    nodes, race_id = _load_node_file(root_path)
    skip_telemetry = cfg.opts("mechanic", "skip.telemetry", default_value=False, mandatory=False)
    metrics_store = None
    if not skip_telemetry:
        cls = metrics.metrics_store_class(cfg)
        metrics_store = cls(cfg)
        race_store = metrics.race_store(cfg)
        try:
            current_race = race_store.find_by_race_id(race_id)
            metrics_store.open(
                race_id=current_race.race_id,
                race_timestamp=current_race.race_timestamp,
                track_name=current_race.track_name,
                challenge_name=current_race.challenge_name,
            )
        except exceptions.NotFound:
            logging.getLogger(__name__).info("Could not find race [%s] and will thus not persist system metrics.", race_id)
            # Don't persist system metrics if we can't retrieve the race as we cannot derive the required meta-data.
            current_race = None
            metrics_store = None

    node_launcher.stop(nodes, metrics_store)
    _delete_node_file(root_path)

    if metrics_store is not None and current_race:
        metrics_store.flush(refresh=True)
        for node in nodes:
            results = metrics.calculate_system_results(metrics_store, node.node_name)
            current_race.add_results(results)
            metrics.results_store(cfg).store_results(current_race)

        metrics_store.close()

    provisioner.cleanup(
        preserve=cfg.opts("mechanic", "preserve.install"), install_dir=node_config.binary_path, data_paths=node_config.data_paths
    )


def _load_node_file(root_path):
    with open(os.path.join(root_path, "node"), "rb") as f:
        return pickle.load(f)


def _store_node_file(root_path, data):
    with open(os.path.join(root_path, "node"), "wb") as f:
        pickle.dump(data, f)


def _delete_node_file(root_path):
    os.remove(os.path.join(root_path, "node"))


##############################
# Data exchanged with node mechanics
##############################


@dataclass(frozen=True)
class StartNodes:
    """
    Parameters for starting the nodes of the benchmark candidate on one host.
    """

    cfg: types.Config
    open_metrics_context: dict
    sources: bool
    distribution: bool
    external: bool
    docker: bool
    all_node_ips: set
    all_node_ids: set
    ip: str
    port: int
    node_ids: list


def to_ip_port(hosts):
    ip_port_pairs = []
    for host in hosts:
        host = host.copy()
        host_or_ip = host.pop("host")
        port = host.pop("port", 9200)
        if host:
            raise exceptions.SystemSetupError(
                "When specifying nodes to be managed by Rally you can only supply "
                "hostname:port pairs (e.g. 'localhost:9200'), any additional options cannot "
                "be supported."
            )
        ip = net.resolve(host_or_ip)
        ip_port_pairs.append((ip, port))
    return ip_port_pairs


def extract_all_node_ips(ip_port_pairs):
    all_node_ips = set()
    for ip, _ in ip_port_pairs:
        all_node_ips.add(ip)
    return all_node_ips


def extract_all_node_ids(all_nodes_by_host):
    all_node_ids = set()
    for node_ids_per_host in all_nodes_by_host.values():
        all_node_ids.update(node_ids_per_host)
    return all_node_ids


def nodes_by_host(ip_port_pairs):
    nodes = {}
    node_id = 0
    for ip_port in ip_port_pairs:
        if ip_port not in nodes:
            nodes[ip_port] = []
        nodes[ip_port].append(node_id)
        node_id += 1
    return nodes


class MechanicCoordinator:
    """
    Coordinates the mechanics on all target hosts (which do the actual work). Runs in the process of race control.
    """

    def __init__(self, cfg: types.Config):
        self.cfg = cfg
        self.logger = logging.getLogger(__name__)
        self.externally_provisioned = False
        # handles of NodeMechanicActor instances
        self.node_mechanics: list = []

    async def start_engine(self, open_metrics_context, sources=False, distribution=False, external=False, docker=False):
        """
        Starts the benchmark candidate, unless it is provisioned externally.

        :return: The revision of the team repository.
        """
        self.logger.info("Starting engine.")
        load_team(self.cfg, external)
        # TODO: This is implicitly set by #load_team() - can we gather this elsewhere?
        team_revision = self.cfg.opts("mechanic", "repository.revision")

        hosts = self.cfg.opts("client", "hosts").default_or_first
        if len(hosts) == 0:
            raise exceptions.LaunchError("No target hosts are configured.")

        self.externally_provisioned = external
        if self.externally_provisioned:
            self.logger.info("Cluster will not be provisioned by Rally.")
            return team_revision

        console.info("Preparing for race ...", flush=True)
        self.logger.info("Cluster consisting of %s will be provisioned by Rally.", hosts)
        all_ips_and_ports = to_ip_port(hosts)
        all_node_ips = extract_all_node_ips(all_ips_and_ports)
        all_nodes_by_host = nodes_by_host(all_ips_and_ports)
        all_node_ids = extract_all_node_ids(all_nodes_by_host)

        # In our startup procedure we first create all mechanics. Only if this succeeds we'll continue.
        starting = []
        for (ip, port), node_ids in all_nodes_by_host.items():
            await actor.require_node_async(ip)
            node_mechanic = actor.create_actor(NodeMechanicActor, self.cfg, host=ip, name=f"node-mechanic-{ip}-{port}")
            self.node_mechanics.append(node_mechanic)
            start_nodes = StartNodes(
                self.cfg,
                open_metrics_context,
                sources,
                distribution,
                external,
                docker,
                all_node_ips,
                all_node_ids,
                ip,
                port,
                node_ids,
            )
            starting.append(node_mechanic.start_nodes.remote(start_nodes))
        await asyncio.gather(*starting)
        return team_revision

    async def check_health(self):
        """
        Raises ``BenchmarkFailure`` if a node mechanic has failed in the background.
        """
        for failure in await asyncio.gather(*[m.health.remote() for m in self.node_mechanics]):
            if failure is not None:
                raise failure

    async def stop_engine(self):
        """
        Stops the benchmark candidate. Stopping is allowed from any state because the benchmark might have been cancelled
        or failed.
        """
        node_mechanics = self.node_mechanics
        self.node_mechanics = []
        if self.externally_provisioned or not node_mechanics:
            return
        try:
            results = await asyncio.gather(*[m.stop_nodes.remote() for m in node_mechanics], return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    self.logger.error("Could not stop nodes: %s", result)
        finally:
            for m in node_mechanics:
                actor.kill_actor(m)


class NodeMechanicActor(actor.RallyActorBase):
    """
    One instance of this actor is run on each target host and coordinates the actual work of starting / stopping all nodes that should run
    on this host.
    """

    def __init__(self, cfg: types.Config):
        super().__init__(name="node-mechanic", cfg=cfg)
        self.mechanic = None
        self.host = None
        self._flush_task: Optional[asyncio.Task] = None

    @actor.convert_failures("mechanic")
    async def start_nodes(self, msg: StartNodes):
        self.host = msg.ip
        if msg.external:
            self.logger.info("Connecting to externally provisioned nodes on [%s].", msg.ip)
        else:
            self.logger.info("Starting node(s) %s on [%s].", msg.node_ids, msg.ip)

        # Load node-specific configuration
        cfg = config.auto_load_local_config(
            msg.cfg,
            additional_sections=[
                # only copy the relevant bits
                "track",
                "mechanic",
                "client",
                "telemetry",
                # allow metrics store to extract race meta-data
                "race",
                "source",
            ],
        )
        # set root path (normally done by the main entry point)
        cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        if not msg.external:
            cfg.add(config.Scope.benchmark, "provisioning", "node.ids", msg.node_ids)

        cls = metrics.metrics_store_class(cfg)
        metrics_store = cls(cfg)
        metrics_store.open(ctx=msg.open_metrics_context)

        self.mechanic = create(
            cfg,
            metrics_store,
            msg.ip,
            msg.port,
            msg.all_node_ips,
            msg.all_node_ids,
            msg.sources,
            msg.distribution,
            msg.external,
            msg.docker,
        )
        self.mechanic.start_engine()
        self._flush_task = asyncio.create_task(self._flush_metrics_periodically())

    def reset_relative_time(self):
        if self.mechanic:
            self.mechanic.reset_relative_time()

    def health(self):
        """
        :return: A failure that happened in the background, if any.
        """
        return self._failure

    @actor.convert_failures("mechanic")
    async def stop_nodes(self):
        if self._flush_task is not None:
            self._flush_task.cancel()
            self._flush_task = None
        if self.mechanic:
            mechanic = self.mechanic
            self.mechanic = None
            mechanic.stop_engine()

    async def stop(self):
        await self.stop_nodes()

    @actor.report_failures("mechanic")
    async def _flush_metrics_periodically(self):
        while True:
            await asyncio.sleep(METRIC_FLUSH_INTERVAL_SECONDS)
            if self.mechanic:
                self.mechanic.flush_metrics()


#####################################################
# Internal API (only used by the actor and for tests)
#####################################################


def load_team(cfg: types.Config, external):
    # externally provisioned clusters do not support cars / plugins
    if external:
        car = None
        plugins = []
    else:
        team_path = team.team_path(cfg)
        car = team.load_car(team_path, cfg.opts("mechanic", "car.names"), cfg.opts("mechanic", "car.params"))
        plugins = team.load_plugins(
            team_path, cfg.opts("mechanic", "car.plugins", mandatory=False), cfg.opts("mechanic", "plugin.params", mandatory=False)
        )
    return car, plugins


def create(
    cfg: types.Config,
    metrics_store,
    node_ip,
    node_http_port,
    all_node_ips,
    all_node_ids,
    sources=False,
    distribution=False,
    external=False,
    docker=False,
):
    race_root_path = paths.race_root(cfg)
    node_ids = cfg.opts("provisioning", "node.ids", mandatory=False)
    node_name_prefix = cfg.opts("provisioning", "node.name.prefix")
    car, plugins = load_team(cfg, external)

    if sources or distribution:
        s = supplier.create(cfg, sources, distribution, car, plugins)
        p = []
        all_node_names = ["%s-%s" % (node_name_prefix, n) for n in all_node_ids]
        for node_id in node_ids:
            node_name = "%s-%s" % (node_name_prefix, node_id)
            p.append(provisioner.local(cfg, car, plugins, node_ip, node_http_port, all_node_ips, all_node_names, race_root_path, node_name))
        l = launcher.ProcessLauncher(cfg)
    elif external:
        raise exceptions.RallyAssertionError("Externally provisioned clusters should not need to be managed by Rally's mechanic")
    elif docker:
        if len(plugins) > 0:
            raise exceptions.SystemSetupError(
                'You cannot specify any plugins for Docker clusters. Please remove "--elasticsearch-plugins" and try again.'
            )

        def s():
            return None

        p = []
        for node_id in node_ids:
            node_name = "%s-%s" % (node_name_prefix, node_id)
            p.append(provisioner.docker(cfg, car, node_ip, node_http_port, race_root_path, node_name))
        l = launcher.DockerLauncher(cfg)
    else:
        # It is a programmer error (and not a user error) if this function is called with wrong parameters
        raise RuntimeError("One of sources, distribution, docker or external must be True")

    return Mechanic(cfg, metrics_store, s, p, l)


class Mechanic:
    """
    Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
    running the benchmark).
    """

    def __init__(self, cfg: types.Config, metrics_store, supply, provisioners, launcher):
        self.cfg = cfg
        self.preserve_install = cfg.opts("mechanic", "preserve.install")
        self.metrics_store = metrics_store
        self.supply = supply
        self.provisioners = provisioners
        self.launcher = launcher
        self.nodes = []
        self.node_configs = []
        self.logger = logging.getLogger(__name__)

    def start_engine(self):
        binaries = self.supply()
        self.node_configs = []
        for p in self.provisioners:
            self.node_configs.append(p.prepare(binaries))
        self.nodes = self.launcher.start(self.node_configs)
        return self.nodes

    def reset_relative_time(self):
        self.logger.info("Resetting relative time of system metrics store.")
        self.metrics_store.reset_relative_time()

    def flush_metrics(self, refresh=False):
        self.logger.debug("Flushing system metrics.")
        self.metrics_store.flush(refresh=refresh)

    def stop_engine(self):
        self.logger.info("Stopping nodes %s.", self.nodes)
        self.launcher.stop(self.nodes, self.metrics_store)
        self.flush_metrics(refresh=True)
        try:
            current_race = self._current_race()
            for node in self.nodes:
                self._add_results(current_race, node)
        except exceptions.NotFound as e:
            self.logger.warning("Cannot store system metrics: %s.", str(e))

        self.metrics_store.close()
        self.nodes = []
        for node_config in self.node_configs:
            provisioner.cleanup(preserve=self.preserve_install, install_dir=node_config.binary_path, data_paths=node_config.data_paths)
        self.node_configs = []

    def _current_race(self):
        race_id = self.cfg.opts("system", "race.id")
        return metrics.race_store(self.cfg).find_by_race_id(race_id)

    def _add_results(self, current_race, node):
        results = metrics.calculate_system_results(self.metrics_store, node.node_name)
        current_race.add_results(results)
        metrics.results_store(self.cfg).store_results(current_race)
