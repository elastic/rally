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

import contextlib
import json
import logging
import os
import pickle
import sys
import traceback
from collections import defaultdict

import thespian.actors

from esrally import PROGRAM_NAME, actor, client, config, exceptions, metrics, paths
from esrally.mechanic import launcher, provisioner, supplier, team
from esrally.utils import console, net

METRIC_FLUSH_INTERVAL_SECONDS = 30


def build(cfg):
    car, plugins = load_team(cfg, external=False)

    s = supplier.create(cfg, sources=True, distribution=False, car=car, plugins=plugins)
    binaries = s()
    console.println(json.dumps(binaries, indent=2), force=True)


def download(cfg):
    car, plugins = load_team(cfg, external=False)

    s = supplier.create(cfg, sources=False, distribution=True, car=car, plugins=plugins)
    binaries = s()
    console.println(json.dumps(binaries, indent=2), force=True)


def install(cfg):
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


def start(cfg):
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


def stop(cfg):
    root_path = paths.install_root(cfg)
    node_config = provisioner.load_node_configuration(root_path)
    if node_config.build_type == "tar":
        node_launcher = launcher.ProcessLauncher(cfg)
    elif node_config.build_type == "docker":
        node_launcher = launcher.DockerLauncher(cfg)
    else:
        raise exceptions.SystemSetupError(f"Unknown build type [{node_config.build_type}]")

    nodes, race_id = _load_node_file(root_path)

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

    if current_race:
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
# Public Messages
##############################


class StartEngine:
    def __init__(self, cfg, open_metrics_context, sources, distribution, external, docker, ip=None, port=None, node_id=None):
        self.cfg = cfg
        self.open_metrics_context = open_metrics_context
        self.sources = sources
        self.distribution = distribution
        self.external = external
        self.docker = docker
        self.ip = ip
        self.port = port
        self.node_id = node_id

    def for_nodes(self, all_node_ips=None, all_node_ids=None, ip=None, port=None, node_ids=None):
        """

        Creates a StartNodes instance for a concrete IP, port and their associated node_ids.

        :param all_node_ips: The IPs of all nodes in the cluster (including the current one).
        :param all_node_ids: The numeric id of all nodes in the cluster (including the current one).
        :param ip: The IP to set.
        :param port: The port number to set.
        :param node_ids: A list of node id to set.
        :return: A corresponding ``StartNodes`` message with the specified IP, port number and node ids.
        """
        return StartNodes(
            self.cfg,
            self.open_metrics_context,
            self.sources,
            self.distribution,
            self.external,
            self.docker,
            all_node_ips,
            all_node_ids,
            ip,
            port,
            node_ids,
        )


class EngineStarted:
    def __init__(self, team_revision):
        self.team_revision = team_revision


class StopEngine:
    pass


class EngineStopped:
    pass


class ResetRelativeTime:
    def __init__(self, reset_in_seconds):
        self.reset_in_seconds = reset_in_seconds


##############################
# Mechanic internal messages
##############################


class StartNodes:
    def __init__(self, cfg, open_metrics_context, sources, distribution, external, docker, all_node_ips, all_node_ids, ip, port, node_ids):
        self.cfg = cfg
        self.open_metrics_context = open_metrics_context
        self.sources = sources
        self.distribution = distribution
        self.external = external
        self.docker = docker
        self.all_node_ips = all_node_ips
        self.all_node_ids = all_node_ids
        self.ip = ip
        self.port = port
        self.node_ids = node_ids


class NodesStarted:
    pass


class StopNodes:
    pass


class NodesStopped:
    pass


def cluster_distribution_version(cfg, client_factory=client.EsClientFactory):
    """
    Attempt to get the cluster's distribution version even before it is actually started (which makes only sense for externally
    provisioned clusters).

    :param cfg: The current config object.
    :param client_factory: Factory class that creates the Elasticsearch client.
    :return: The distribution version.
    """
    hosts = cfg.opts("client", "hosts").default
    client_options = cfg.opts("client", "options").default
    es = client_factory(hosts, client_options).create()
    # unconditionally wait for the REST layer - if it's not up by then, we'll intentionally raise the original error
    client.wait_for_rest_layer(es)
    return es.info()["version"]["number"]


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


class MechanicActor(actor.RallyActor):
    WAKEUP_RESET_RELATIVE_TIME = "relative_time"

    """
    This actor coordinates all associated mechanics on remote hosts (which do the actual work).
    """

    def __init__(self):
        super().__init__()
        self.cfg = None
        self.race_control = None
        self.cluster_launcher = None
        self.cluster = None
        self.car = None
        self.team_revision = None
        self.externally_provisioned = False

    def receiveUnrecognizedMessage(self, msg, sender):
        self.logger.debug("MechanicActor#receiveMessage unrecognized(msg = [%s] sender = [%s])", str(type(msg)), str(sender))

    def receiveMsg_ChildActorExited(self, msg, sender):
        if self.is_current_status_expected(["cluster_stopping", "cluster_stopped"]):
            self.logger.info("Child actor exited while engine is stopping or stopped: [%s]", msg)
            return
        failmsg = "Child actor exited with [%s] while in status [%s]." % (msg, self.status)
        self.logger.error(failmsg)
        self.send(self.race_control, actor.BenchmarkFailure(failmsg))

    def receiveMsg_PoisonMessage(self, msg, sender):
        self.logger.info("MechanicActor#receiveMessage poison(msg = [%s] sender = [%s])", str(msg.poisonMessage), str(sender))
        # something went wrong with a child actor (or another actor with which we have communicated)
        if isinstance(msg.poisonMessage, StartEngine):
            failmsg = "Could not start benchmark candidate. Are Rally daemons on all targeted machines running?"
        else:
            failmsg = msg.details
        self.logger.error(failmsg)
        self.send(self.race_control, actor.BenchmarkFailure(failmsg))

    @actor.no_retry("mechanic")  # pylint: disable=no-value-for-parameter
    def receiveMsg_StartEngine(self, msg, sender):
        self.logger.info("Received signal from race control to start engine.")
        self.race_control = sender
        self.cfg = msg.cfg
        self.car, _ = load_team(self.cfg, msg.external)
        # TODO: This is implicitly set by #load_team() - can we gather this elsewhere?
        self.team_revision = self.cfg.opts("mechanic", "repository.revision")

        # In our startup procedure we first create all mechanics. Only if this succeeds we'll continue.
        hosts = self.cfg.opts("client", "hosts").default
        if len(hosts) == 0:
            raise exceptions.LaunchError("No target hosts are configured.")

        self.externally_provisioned = msg.external
        if self.externally_provisioned:
            self.logger.info("Cluster will not be provisioned by Rally.")
            self.status = "nodes_started"
            self.received_responses = []
            self.on_all_nodes_started()
            self.status = "cluster_started"
        else:
            console.info("Preparing for race ...", flush=True)
            self.logger.info("Cluster consisting of %s will be provisioned by Rally.", hosts)
            msg.hosts = hosts
            # Initialize the children array to have the right size to
            # ensure waiting for all responses
            self.children = [None] * len(nodes_by_host(to_ip_port(hosts)))
            self.send(self.createActor(Dispatcher), msg)
            self.status = "starting"
            self.received_responses = []

    @actor.no_retry("mechanic")  # pylint: disable=no-value-for-parameter
    def receiveMsg_NodesStarted(self, msg, sender):
        # Initially the addresses of the children are not
        # known and there is just a None placeholder in the
        # array.  As addresses become known, fill them in.
        if sender not in self.children:
            # Length-limited FIFO characteristics:
            self.children.insert(0, sender)
            self.children.pop()

        self.transition_when_all_children_responded(sender, msg, "starting", "cluster_started", self.on_all_nodes_started)

    @actor.no_retry("mechanic")  # pylint: disable=no-value-for-parameter
    def receiveMsg_ResetRelativeTime(self, msg, sender):
        if msg.reset_in_seconds > 0:
            self.wakeupAfter(msg.reset_in_seconds, payload=MechanicActor.WAKEUP_RESET_RELATIVE_TIME)
        else:
            self.reset_relative_time()

    def receiveMsg_WakeupMessage(self, msg, sender):
        if msg.payload == MechanicActor.WAKEUP_RESET_RELATIVE_TIME:
            self.reset_relative_time()
        else:
            raise exceptions.RallyAssertionError(f"Unknown wakeup reason [{msg.payload}]")

    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.send(self.race_control, msg)

    @actor.no_retry("mechanic")  # pylint: disable=no-value-for-parameter
    def receiveMsg_StopEngine(self, msg, sender):
        # we might have experienced a launch error or the user has cancelled the benchmark. Hence we need to allow to stop the
        # cluster from various states and we don't check here for a specific one.
        if self.externally_provisioned:
            self.on_all_nodes_stopped()
        else:
            self.send_to_children_and_transition(sender, StopNodes(), [], "cluster_stopping")

    @actor.no_retry("mechanic")  # pylint: disable=no-value-for-parameter
    def receiveMsg_NodesStopped(self, msg, sender):
        self.transition_when_all_children_responded(sender, msg, "cluster_stopping", "cluster_stopped", self.on_all_nodes_stopped)

    def on_all_nodes_started(self):
        self.send(self.race_control, EngineStarted(self.team_revision))

    def reset_relative_time(self):
        for m in self.children:
            self.send(m, ResetRelativeTime(0))

    def on_all_nodes_stopped(self):
        self.send(self.race_control, EngineStopped())
        # clear all state as the mechanic might get reused later
        for m in self.children:
            self.send(m, thespian.actors.ActorExitRequest())
        self.children = []
        # do not self-terminate, let the parent actor handle this


@thespian.actors.requireCapability("coordinator")
class Dispatcher(actor.RallyActor):
    """This Actor receives a copy of the startmsg (with the computed hosts
    attached) and creates a NodeMechanicActor on each targeted
    remote host.  It uses Thespian SystemRegistration to get
    notification of when remote nodes are available.  As a special
    case, if an IP address is localhost, the NodeMechanicActor is
    immediately created locally.  Once All NodeMechanicActors are
    started, it will send them all their startup message, with a
    reply-to back to the actor that made the request of the
    Dispatcher.
    """

    def __init__(self):
        super().__init__()
        self.start_sender = None
        self.pending = None
        self.remotes = None

    @actor.no_retry("mechanic dispatcher")  # pylint: disable=no-value-for-parameter
    def receiveMsg_StartEngine(self, startmsg, sender):
        self.start_sender = sender
        self.pending = []
        self.remotes = defaultdict(list)
        all_ips_and_ports = to_ip_port(startmsg.hosts)
        all_node_ips = extract_all_node_ips(all_ips_and_ports)
        all_nodes_by_host = nodes_by_host(all_ips_and_ports)
        all_node_ids = extract_all_node_ids(all_nodes_by_host)

        for (ip, port), node in all_nodes_by_host.items():
            submsg = startmsg.for_nodes(all_node_ips, all_node_ids, ip, port, node)
            submsg.reply_to = sender
            if ip == "127.0.0.1":
                m = self.createActor(NodeMechanicActor, targetActorRequirements={"coordinator": True})
                self.pending.append((m, submsg))
            else:
                self.remotes[ip].append(submsg)

        if self.remotes:
            # Now register with the ActorSystem to be told about all
            # remote nodes (via the ActorSystemConventionUpdate below).
            self.notifyOnSystemRegistrationChanges(True)
        else:
            self.send_all_pending()

        # Could also initiate a wakeup message to fail this if not all
        # remotes come online within the expected amount of time... TBD

    def receiveMsg_ActorSystemConventionUpdate(self, convmsg, sender):
        if not convmsg.remoteAdded:
            self.logger.warning("Remote Rally node [%s] exited during NodeMechanicActor startup process.", convmsg.remoteAdminAddress)
            self.start_sender(actor.BenchmarkFailure("Remote Rally node [%s] has been shutdown prematurely." % convmsg.remoteAdminAddress))
        else:
            remote_ip = convmsg.remoteCapabilities.get("ip", None)
            self.logger.info("Remote Rally node [%s] has started.", remote_ip)

            for eachmsg in self.remotes[remote_ip]:
                self.pending.append((self.createActor(NodeMechanicActor, targetActorRequirements={"ip": remote_ip}), eachmsg))
            if remote_ip in self.remotes:
                del self.remotes[remote_ip]
            if not self.remotes:
                # Notifications are no longer needed
                self.notifyOnSystemRegistrationChanges(False)
                self.send_all_pending()

    def send_all_pending(self):
        # Invoked when all remotes have checked in and self.pending is
        # the list of remote NodeMechanic actors and messages to send.
        for each in self.pending:
            self.send(*each)
        self.pending = []

    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.send(self.start_sender, msg)

    def receiveMsg_PoisonMessage(self, msg, sender):
        self.send(self.start_sender, actor.BenchmarkFailure(msg.details))

    def receiveUnrecognizedMessage(self, msg, sender):
        self.logger.info("mechanic.Dispatcher#receiveMessage unrecognized(msg = [%s] sender = [%s])", str(type(msg)), str(sender))


class NodeMechanicActor(actor.RallyActor):
    """
    One instance of this actor is run on each target host and coordinates the actual work of starting / stopping all nodes that should run
    on this host.
    """

    def __init__(self):
        super().__init__()
        self.mechanic = None
        self.host = None

    def receiveMsg_StartNodes(self, msg, sender):
        try:
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
            # avoid follow-up errors in case we receive an unexpected ActorExitRequest due to an early failure in a parent actor.

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
            self.wakeupAfter(METRIC_FLUSH_INTERVAL_SECONDS)
            self.send(getattr(msg, "reply_to", sender), NodesStarted())
        except Exception:
            self.logger.exception("Cannot process message [%s]", msg)
            # avoid "can't pickle traceback objects"
            _, ex_value, _ = sys.exc_info()
            self.send(getattr(msg, "reply_to", sender), actor.BenchmarkFailure(ex_value, traceback.format_exc()))

    def receiveMsg_PoisonMessage(self, msg, sender):
        if sender != self.myAddress:
            self.send(sender, actor.BenchmarkFailure(msg.details))

    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.send(getattr(msg, "reply_to", sender), msg)

    def receiveUnrecognizedMessage(self, msg, sender):
        # at the moment, we implement all message handling blocking. This is not ideal but simple to get started with. Besides, the caller
        # needs to block anyway. The only reason we implement mechanic as an actor is to distribute them.
        # noinspection PyBroadException
        try:
            self.logger.debug("NodeMechanicActor#receiveMessage(msg = [%s] sender = [%s])", str(type(msg)), str(sender))
            if isinstance(msg, ResetRelativeTime) and self.mechanic:
                self.mechanic.reset_relative_time()
            elif isinstance(msg, thespian.actors.WakeupMessage) and self.mechanic:
                self.mechanic.flush_metrics()
                self.wakeupAfter(METRIC_FLUSH_INTERVAL_SECONDS)
            elif isinstance(msg, StopNodes):
                self.mechanic.stop_engine()
                self.send(sender, NodesStopped())
                self.mechanic = None
            elif isinstance(msg, thespian.actors.ActorExitRequest):
                if self.mechanic:
                    self.mechanic.stop_engine()
                    self.mechanic = None
        except BaseException as e:
            self.logger.exception("Cannot process message [%s]", msg)
            self.send(getattr(msg, "reply_to", sender), actor.BenchmarkFailure("Error on host %s" % str(self.host), e))


#####################################################
# Internal API (only used by the actor and for tests)
#####################################################


def load_team(cfg, external):
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
    cfg, metrics_store, node_ip, node_http_port, all_node_ips, all_node_ids, sources=False, distribution=False, external=False, docker=False
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

    def __init__(self, cfg, metrics_store, supply, provisioners, launcher):
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
