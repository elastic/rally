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

import json
import logging
import sys
from collections import defaultdict

import thespian.actors

from esrally import actor, client, paths, config, metrics, exceptions
from esrally.mechanic import supplier, provisioner, launcher, team
from esrally.utils import net, console

METRIC_FLUSH_INTERVAL_SECONDS = 30


def download(cfg):
    car, plugins = load_team(cfg, external=False)

    s = supplier.create(cfg, sources=False, distribution=True, build=False, car=car, plugins=plugins)
    binaries = s()
    console.println(json.dumps(binaries, indent=2), force=True)


##############################
# Public Messages
##############################

class StartEngine:
    def __init__(self, cfg, open_metrics_context, cluster_settings, sources, build, distribution, external, docker, ip=None, port=None,
                 node_id=None):
        self.cfg = cfg
        self.open_metrics_context = open_metrics_context
        self.cluster_settings = cluster_settings
        self.sources = sources
        self.build = build
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
        return StartNodes(self.cfg, self.open_metrics_context, self.cluster_settings, self.sources, self.build, self.distribution,
                          self.external, self.docker, all_node_ips, all_node_ids, ip, port, node_ids)


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
    def __init__(self, cfg, open_metrics_context, cluster_settings, sources, build, distribution, external, docker,
                 all_node_ips, all_node_ids, ip, port, node_ids):
        self.cfg = cfg
        self.open_metrics_context = open_metrics_context
        self.cluster_settings = cluster_settings
        self.sources = sources
        self.build = build
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
    # noinspection PyBroadException
    try:
        return es.info()["version"]["number"]
    except BaseException:
        logging.getLogger(__name__).exception("Could not retrieve cluster distribution version")
        return None


def to_ip_port(hosts):
    ip_port_pairs = []
    for host in hosts:
        host = host.copy()
        host_or_ip = host.pop("host")
        port = host.pop("port", 9200)
        if host:
            raise exceptions.SystemSetupError("When specifying nodes to be managed by Rally you can only supply "
                                              "hostname:port pairs (e.g. 'localhost:9200'), any additional options cannot "
                                              "be supported.")
        ip = net.resolve(host_or_ip)
        ip_port_pairs.append((ip, port))
    return ip_port_pairs


def extract_all_node_ips(ip_port_pairs):
    all_node_ips = set()
    for ip, port in ip_port_pairs:
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
        self.logger.info("MechanicActor#receiveMessage unrecognized(msg = [%s] sender = [%s])", str(type(msg)), str(sender))

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

    @actor.no_retry("mechanic")
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
            # TODO: This needs to be handled later - we should probably disallow this entirely
            if msg.cluster_settings:
                pretty_settings = json.dumps(msg.cluster_settings, indent=2)
                warning = "Ensure that these settings are defined in elasticsearch.yml:\n\n{}\n\nIf they are absent, running this track " \
                          "will fail or lead to unexpected results.".format(pretty_settings)
                console.warn(warning, logger=self.logger)
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

    @actor.no_retry("mechanic")
    def receiveMsg_NodesStarted(self, msg, sender):
        # Initially the addresses of the children are not
        # known and there is just a None placeholder in the
        # array.  As addresses become known, fill them in.
        if sender not in self.children:
            # Length-limited FIFO characteristics:
            self.children.insert(0, sender)
            self.children.pop()

        self.transition_when_all_children_responded(sender, msg, "starting", "cluster_started", self.on_all_nodes_started)

    @actor.no_retry("mechanic")
    def receiveMsg_ResetRelativeTime(self, msg, sender):
        if msg.reset_in_seconds > 0:
            self.wakeupAfter(msg.reset_in_seconds, payload=MechanicActor.WAKEUP_RESET_RELATIVE_TIME)
        else:
            self.reset_relative_time()

    def receiveMsg_WakeupMessage(self, msg, sender):
        if msg.payload == MechanicActor.WAKEUP_RESET_RELATIVE_TIME:
            self.reset_relative_time()
        else:
            raise exceptions.RallyAssertionError("Unknown wakeup reason [{}]".format(msg.payload))

    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.send(self.race_control, msg)

    @actor.no_retry("mechanic")
    def receiveMsg_StopEngine(self, msg, sender):
        # we might have experienced a launch error or the user has cancelled the benchmark. Hence we need to allow to stop the
        # cluster from various states and we don't check here for a specific one.
        if self.externally_provisioned:
            self.on_all_nodes_stopped()
        else:
            self.send_to_children_and_transition(sender, StopNodes(), [], "cluster_stopping")

    @actor.no_retry("mechanic")
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


@thespian.actors.requireCapability('coordinator')
class Dispatcher(actor.RallyActor):
    def __init__(self):
        super().__init__()
        self.start_sender = None
        self.pending = None
        self.remotes = None

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

    @actor.no_retry("mechanic dispatcher")
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
            if '127.0.0.1' == ip:
                m = self.createActor(NodeMechanicActor,
                                     targetActorRequirements={"coordinator": True})
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
            remote_ip = convmsg.remoteCapabilities.get('ip', None)
            self.logger.info("Remote Rally node [%s] has started.", remote_ip)

            for eachmsg in self.remotes[remote_ip]:
                self.pending.append((self.createActor(NodeMechanicActor,
                                                      targetActorRequirements={"ip": remote_ip}),
                                     eachmsg))
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
            cfg = config.auto_load_local_config(msg.cfg, additional_sections=[
                # only copy the relevant bits
                "track", "mechanic", "client", "telemetry",
                # allow metrics store to extract race meta-data
                "race",
                "source"
            ])
            # set root path (normally done by the main entry point)
            cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
            if not msg.external:
                cfg.add(config.Scope.benchmark, "provisioning", "node.ip", msg.ip)
                # we need to override the port with the value that the user has specified instead of using the default value (39200)
                cfg.add(config.Scope.benchmark, "provisioning", "node.http.port", msg.port)
                cfg.add(config.Scope.benchmark, "provisioning", "node.ids", msg.node_ids)

            cls = metrics.metrics_store_class(cfg)
            metrics_store = cls(cfg)
            metrics_store.open(ctx=msg.open_metrics_context)
            # avoid follow-up errors in case we receive an unexpected ActorExitRequest due to an early failure in a parent actor.

            self.mechanic = create(cfg, metrics_store, msg.all_node_ips, msg.all_node_ids, msg.cluster_settings,
                                   msg.sources, msg.build, msg.distribution, msg.external, msg.docker)
            self.mechanic.start_engine()
            self.wakeupAfter(METRIC_FLUSH_INTERVAL_SECONDS)
            self.send(getattr(msg, "reply_to", sender), NodesStarted())
        except Exception:
            self.logger.exception("Cannot process message [%s]", msg)
            # avoid "can't pickle traceback objects"
            import traceback
            ex_type, ex_value, ex_traceback = sys.exc_info()
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
        plugins = team.load_plugins(team_path, cfg.opts("mechanic", "car.plugins"), cfg.opts("mechanic", "plugin.params"))
    return car, plugins


def create(cfg, metrics_store, all_node_ips, all_node_ids, cluster_settings=None, sources=False, build=False, distribution=False, external=False,
           docker=False):
    races_root = paths.races_root(cfg)
    race_root_path = paths.race_root(cfg)
    node_ids = cfg.opts("provisioning", "node.ids", mandatory=False)
    car, plugins = load_team(cfg, external)

    if sources or distribution:
        s = supplier.create(cfg, sources, distribution, build, car, plugins)
        p = []
        for node_id in node_ids:
            p.append(provisioner.local_provisioner(cfg, car, plugins, cluster_settings, all_node_ips, all_node_ids, race_root_path, node_id))
        l = launcher.ProcessLauncher(cfg, metrics_store, races_root)
    elif external:
        raise exceptions.RallyAssertionError("Externally provisioned clusters should not need to be managed by Rally's mechanic")
    elif docker:
        if len(plugins) > 0:
            raise exceptions.SystemSetupError("You cannot specify any plugins for Docker clusters. Please remove "
                                              "\"--elasticsearch-plugins\" and try again.")
        s = lambda: None
        p = []
        for node_id in node_ids:
            p.append(provisioner.docker_provisioner(cfg, car, cluster_settings, race_root_path, node_id))
        l = launcher.DockerLauncher(cfg, metrics_store)
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
        self.metrics_store = metrics_store
        self.supply = supply
        self.provisioners = provisioners
        self.launcher = launcher
        self.nodes = []
        self.logger = logging.getLogger(__name__)

    def start_engine(self):
        binaries = self.supply()
        node_configs = []
        for p in self.provisioners:
            node_configs.append(p.prepare(binaries))
        self.nodes = self.launcher.start(node_configs)
        return self.nodes

    def reset_relative_time(self):
        self.logger.info("Resetting relative time of system metrics store.")
        self.metrics_store.reset_relative_time()

    def flush_metrics(self, refresh=False):
        self.logger.debug("Flushing system metrics.")
        self.metrics_store.flush(refresh=refresh)

    def stop_engine(self):
        self.logger.info("Stopping nodes %s.", self.nodes)
        self.launcher.stop(self.nodes)
        self.flush_metrics(refresh=True)
        try:
            current_race = self._current_race()
            for node in self.nodes:
                self._add_results(current_race, node)
        except exceptions.NotFound as e:
            self.logger.warning("Cannot store system metrics: %s.", str(e))

        self.metrics_store.close()
        self.nodes = []
        for p in self.provisioners:
            p.cleanup()

    def _current_race(self):
        race_id = self.cfg.opts("system", "race.id")
        return metrics.race_store(self.cfg).find_by_race_id(race_id)

    def _add_results(self, current_race, node):
        results = metrics.calculate_system_results(self.metrics_store, node.node_name)
        current_race.add_results(results)
        metrics.results_store(self.cfg).store_results(current_race)
