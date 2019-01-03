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

import sys
import json
from collections import defaultdict

import thespian.actors

from esrally import actor, client, paths, config, metrics, exceptions
from esrally.utils import net, console
from esrally.mechanic import supplier, provisioner, launcher, team


METRIC_FLUSH_INTERVAL_SECONDS = 30


##############################
# Public Messages
##############################

class ClusterMetaInfo:
    def __init__(self, nodes, revision, distribution_version):
        self.nodes = nodes
        self.revision = revision
        self.distribution_version = distribution_version

    def as_dict(self):
        return {
            "nodes": [n.as_dict() for n in self.nodes],
            "node-count": len(self.nodes),
            "revision": self.revision,
            "distribution-version": self.distribution_version
        }


class NodeMetaInfo:
    def __init__(self, n):
        self.host_name = n.host_name
        self.node_name = n.node_name
        self.ip = n.ip
        self.os = n.os
        self.jvm = n.jvm
        self.cpu = n.cpu
        self.memory = n.memory
        self.fs = n.fs
        self.plugins = n.plugins

    def as_dict(self):
        return self.__dict__


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

    def for_nodes(self, all_node_ips=None, ip=None, port=None, node_ids=None):
        """

        Creates a StartNodes instance for a concrete IP, port and their associated node_ids.

        :param all_node_ips: The IPs of all nodes in the cluster (including the current one).
        :param ip: The IP to set.
        :param port: The port number to set.
        :param node_ids: A list of node id to set.
        :return: A corresponding ``StartNodes`` message with the specified IP, port number and node ids.
        """
        return StartNodes(self.cfg, self.open_metrics_context, self.cluster_settings, self.sources, self.build, self.distribution,
                          self.external, self.docker, all_node_ips, ip, port, node_ids)


class EngineStarted:
    def __init__(self, cluster_meta_info, system_meta_info):
        self.cluster_meta_info = cluster_meta_info
        self.system_meta_info = system_meta_info


class StopEngine:
    pass


class EngineStopped:
    def __init__(self, system_metrics):
        self.system_metrics = system_metrics


class OnBenchmarkStart:
    def __init__(self, lap):
        self.lap = lap


class BenchmarkStarted:
    pass


class OnBenchmarkStop:
    pass


class BenchmarkStopped:
    def __init__(self, system_metrics):
        self.system_metrics = system_metrics


class ResetRelativeTime:
    def __init__(self, reset_in_seconds):
        self.reset_in_seconds = reset_in_seconds


##############################
# Mechanic internal messages
##############################

class StartNodes:
    def __init__(self, cfg, open_metrics_context, cluster_settings, sources, build, distribution, external, docker,
                 all_node_ips, ip, port, node_ids):
        self.cfg = cfg
        self.open_metrics_context = open_metrics_context
        self.cluster_settings = cluster_settings
        self.sources = sources
        self.build = build
        self.distribution = distribution
        self.external = external
        self.docker = docker
        self.all_node_ips = all_node_ips
        self.ip = ip
        self.port = port
        self.node_ids = node_ids


class NodesStarted:
    def __init__(self, node_meta_infos, system_meta_info):
        """
        Creates a new NodeStarted message.

        :param node_meta_infos: A list of ``NodeMetaInfo`` instances.
        :param system_meta_info:
        """
        self.node_meta_infos = node_meta_infos
        self.system_meta_info = system_meta_info


class StopNodes:
    pass


class NodesStopped:
    def __init__(self, system_metrics):
        self.system_metrics = system_metrics


class ApplyMetricsMetaInfo:
    def __init__(self, meta_info):
        self.meta_info = meta_info


class MetricsMetaInfoApplied:
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
    return es.info()["version"]["number"]


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

    WAKEUP_FLUSH_METRICS = "flush_metrics"

    """
    This actor coordinates all associated mechanics on remote hosts (which do the actual work).
    """

    def __init__(self):
        super().__init__()
        self.cfg = None
        self.metrics_store = None
        self.race_control = None
        self.cluster_launcher = None
        self.cluster = None
        self.car = None

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
        cls = metrics.metrics_store_class(self.cfg)
        self.metrics_store = cls(self.cfg)
        self.metrics_store.open(ctx=msg.open_metrics_context)
        self.car, _ = load_team(self.cfg, msg.external)

        # In our startup procedure we first create all mechanics. Only if this succeeds we'll continue.
        hosts = self.cfg.opts("client", "hosts").default
        if len(hosts) == 0:
            raise exceptions.LaunchError("No target hosts are configured.")

        if msg.external:
            self.logger.info("Cluster will not be provisioned by Rally.")
            if msg.cluster_settings:
                pretty_settings = json.dumps(msg.cluster_settings, indent=2)
                warning = "Ensure that these settings are defined in elasticsearch.yml:\n\n{}\n\nIf they are absent, running this track " \
                          "will fail or lead to unexpected results.".format(pretty_settings)
                console.warn(warning, logger=self.logger)
            # just create one actor for this special case and run it on the coordinator node (i.e. here)
            m = self.createActor(NodeMechanicActor,
                                 targetActorRequirements={"coordinator": True})
            self.children.append(m)
            self.send(m, msg.for_nodes(ip=hosts))
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
        self.metrics_store.merge_meta_info(msg.system_meta_info)

        # Initially the addresses of the children are not
        # known and there is just a None placeholder in the
        # array.  As addresses become known, fill them in.
        if sender not in self.children:
            # Length-limited FIFO characteristics:
            self.children.insert(0, sender)
            self.children.pop()

        self.transition_when_all_children_responded(sender, msg, "starting", "nodes_started", self.on_all_nodes_started)

    @actor.no_retry("mechanic")
    def receiveMsg_MetricsMetaInfoApplied(self, msg, sender):
        self.transition_when_all_children_responded(sender, msg, "apply_meta_info", "cluster_started", self.on_cluster_started)

    @actor.no_retry("mechanic")
    def receiveMsg_OnBenchmarkStart(self, msg, sender):
        self.metrics_store.lap = msg.lap
        # in the first lap, we are in state "cluster_started", after that in "benchmark_stopped"
        self.send_to_children_and_transition(sender, msg, ["cluster_started", "benchmark_stopped"], "benchmark_starting")

    @actor.no_retry("mechanic")
    def receiveMsg_BenchmarkStarted(self, msg, sender):
        self.transition_when_all_children_responded(
            sender, msg, "benchmark_starting", "benchmark_started", self.on_benchmark_started)

    @actor.no_retry("mechanic")
    def receiveMsg_ResetRelativeTime(self, msg, sender):
        if msg.reset_in_seconds > 0:
            self.wakeupAfter(msg.reset_in_seconds, payload=MechanicActor.WAKEUP_RESET_RELATIVE_TIME)
        else:
            self.reset_relative_time()

    def receiveMsg_WakeupMessage(self, msg, sender):
        if msg.payload == MechanicActor.WAKEUP_RESET_RELATIVE_TIME:
            self.reset_relative_time()
        elif msg.payload == MechanicActor.WAKEUP_FLUSH_METRICS:
            self.logger.debug("Flushing cluster-wide system metrics store.")
            self.metrics_store.flush(refresh=False)
            self.wakeupAfter(METRIC_FLUSH_INTERVAL_SECONDS, payload=MechanicActor.WAKEUP_FLUSH_METRICS)
        else:
            raise exceptions.RallyAssertionError("Unknown wakeup reason [{}]".format(msg.payload))

    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.send(self.race_control, msg)

    @actor.no_retry("mechanic")
    def receiveMsg_OnBenchmarkStop(self, msg, sender):
        self.send_to_children_and_transition(sender, msg, "benchmark_started", "benchmark_stopping")

    @actor.no_retry("mechanic")
    def receiveMsg_BenchmarkStopped(self, msg, sender):
        self.metrics_store.bulk_add(msg.system_metrics)
        self.transition_when_all_children_responded(
            sender, msg, "benchmark_stopping", "benchmark_stopped", self.on_benchmark_stopped)

    @actor.no_retry("mechanic")
    def receiveMsg_StopEngine(self, msg, sender):
        if self.cluster.preserve:
            console.info("Keeping benchmark candidate including index at (may need several GB).")
        # detach from cluster and gather all system metrics
        self.cluster_launcher.stop(self.cluster)
        # we might have experienced a launch error or the user has cancelled the benchmark. Hence we need to allow to stop the
        # cluster from various states and we don't check here for a specific one.
        self.send_to_children_and_transition(sender, StopNodes(), [], "cluster_stopping")

    @actor.no_retry("mechanic")
    def receiveMsg_NodesStopped(self, msg, sender):
        self.metrics_store.bulk_add(msg.system_metrics)
        self.transition_when_all_children_responded(sender, msg, "cluster_stopping", "cluster_stopped", self.on_all_nodes_stopped)

    def on_all_nodes_started(self):
        self.cluster_launcher = launcher.ClusterLauncher(self.cfg, self.metrics_store)
        # Workaround because we could raise a LaunchError here and thespian will attempt to retry a failed message.
        # In that case, we will get a followup RallyAssertionError because on the second attempt, Rally will check
        # the status which is now "nodes_started" but we expected the status to be "nodes_starting" previously.
        try:
            self.cluster = self.cluster_launcher.start()
        except BaseException as e:
            self.send(self.race_control, actor.BenchmarkFailure("Could not launch cluster", e))
        else:
            # push down all meta data again
            self.send_to_children_and_transition(self.myAddress,
                                                 ApplyMetricsMetaInfo(self.metrics_store.meta_info), "nodes_started", "apply_meta_info")

    def on_cluster_started(self):
        # We don't need to store the original node meta info when the node started up (NodeStarted message) because we actually gather it
        # in ``on_all_nodes_started`` via the ``ClusterLauncher``.
        self.send(self.race_control,
                  EngineStarted(ClusterMetaInfo([NodeMetaInfo(n) for n in self.cluster.nodes],
                                                self.cluster.source_revision,
                                                self.cluster.distribution_version),
                                self.metrics_store.meta_info))
        self.wakeupAfter(METRIC_FLUSH_INTERVAL_SECONDS, payload=MechanicActor.WAKEUP_FLUSH_METRICS)

    def on_benchmark_started(self):
        self.cluster.on_benchmark_start()
        self.send(self.race_control, BenchmarkStarted())

    def reset_relative_time(self):
        self.logger.info("Resetting relative time of cluster system metrics store.")
        self.metrics_store.reset_relative_time()
        for m in self.children:
            self.send(m, ResetRelativeTime(0))

    def on_benchmark_stopped(self):
        self.cluster.on_benchmark_stop()
        self.metrics_store.flush(refresh=False)
        self.send(self.race_control, BenchmarkStopped(self.metrics_store.to_externalizable(clear=True)))

    def on_all_nodes_stopped(self):
        self.metrics_store.flush(refresh=False)
        self.send(self.race_control, EngineStopped(self.metrics_store.to_externalizable()))
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

        for (ip, port), node in nodes_by_host(all_ips_and_ports).items():
            submsg = startmsg.for_nodes(all_node_ips, ip, port, node)
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
        self.config = None
        self.metrics_store = None
        self.mechanic = None
        self.running = False
        self.host = None

    def receiveMsg_StartNodes(self, msg, sender):
        try:
            self.host = msg.ip
            if msg.external:
                self.logger.info("Connecting to externally provisioned nodes on [%s].", msg.ip)
            else:
                self.logger.info("Starting node(s) %s on [%s].", msg.node_ids, msg.ip)

            # Load node-specific configuration
            self.config = config.auto_load_local_config(msg.cfg, additional_sections=[
                # only copy the relevant bits
                "track", "mechanic", "client",
                # allow metrics store to extract race meta-data
                "race",
                "source"
            ])
            # set root path (normally done by the main entry point)
            self.config.add(config.Scope.application, "node", "rally.root", paths.rally_root())
            if not msg.external:
                self.config.add(config.Scope.benchmark, "provisioning", "node.ip", msg.ip)
                # we need to override the port with the value that the user has specified instead of using the default value (39200)
                self.config.add(config.Scope.benchmark, "provisioning", "node.http.port", msg.port)
                self.config.add(config.Scope.benchmark, "provisioning", "node.ids", msg.node_ids)

            cls = metrics.metrics_store_class(self.config)
            self.metrics_store = cls(self.config)
            self.metrics_store.open(ctx=msg.open_metrics_context)
            # avoid follow-up errors in case we receive an unexpected ActorExitRequest due to an early failure in a parent actor.
            self.metrics_store.lap = 0

            self.mechanic = create(self.config, self.metrics_store, msg.all_node_ips, msg.cluster_settings, msg.sources, msg.build,
                                   msg.distribution, msg.external, msg.docker)
            nodes = self.mechanic.start_engine()
            self.running = True
            self.send(getattr(msg, "reply_to", sender), NodesStarted([NodeMetaInfo(node) for node in nodes], self.metrics_store.meta_info))
        except Exception:
            self.logger.exception("Cannot process message [%s]", msg)
            # avoid "can't pickle traceback objects"
            import traceback
            ex_type, ex_value, ex_traceback = sys.exc_info()
            self.send(getattr(msg, "reply_to", sender), actor.BenchmarkFailure(ex_value, traceback.format_exc()))

    def receiveMsg_ApplyMetricsMetaInfo(self, msg, sender):
        self.metrics_store.merge_meta_info(msg.meta_info)
        self.send(sender, MetricsMetaInfoApplied())

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
            if isinstance(msg, ResetRelativeTime):
                self.logger.info("Resetting relative time of system metrics store on host [%s].", self.host)
                self.metrics_store.reset_relative_time()
            elif isinstance(msg, OnBenchmarkStart):
                self.metrics_store.lap = msg.lap
                self.mechanic.on_benchmark_start()
                self.wakeupAfter(METRIC_FLUSH_INTERVAL_SECONDS)
                self.send(sender, BenchmarkStarted())
            elif isinstance(msg, thespian.actors.WakeupMessage):
                if self.running:
                    self.logger.debug("Flushing system metrics store on host [%s].", self.host)
                    self.metrics_store.flush(refresh=False)
                    self.wakeupAfter(METRIC_FLUSH_INTERVAL_SECONDS)
            elif isinstance(msg, OnBenchmarkStop):
                self.mechanic.on_benchmark_stop()
                self.metrics_store.flush(refresh=False)
                # clear metrics store data to not send duplicate system metrics data
                self.send(sender, BenchmarkStopped(self.metrics_store.to_externalizable(clear=True)))
            elif isinstance(msg, StopNodes):
                self.logger.info("Stopping nodes %s.", self.mechanic.nodes)
                self.mechanic.stop_engine()
                self.send(sender, NodesStopped(self.metrics_store.to_externalizable()))
                # clear all state as the mechanic might get reused later
                self.metrics_store.close()
                self.running = False
                self.config = None
                self.mechanic = None
                self.metrics_store = None
            elif isinstance(msg, thespian.actors.ActorExitRequest):
                if self.running:
                    self.logger.info("Stopping nodes %s (due to ActorExitRequest)", self.mechanic.nodes)
                    self.mechanic.stop_engine()
                    self.running = False
        except BaseException as e:
            self.running = False
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


def create(cfg, metrics_store, all_node_ips, cluster_settings=None, sources=False, build=False, distribution=False, external=False,
           docker=False):
    races_root = paths.races_root(cfg)
    challenge_root_path = paths.race_root(cfg)
    node_ids = cfg.opts("provisioning", "node.ids", mandatory=False)
    car, plugins = load_team(cfg, external)

    if sources or distribution:
        s = supplier.create(cfg, sources, distribution, build, challenge_root_path, car, plugins)
        p = []
        for node_id in node_ids:
            p.append(provisioner.local_provisioner(cfg, car, plugins, cluster_settings, all_node_ips, challenge_root_path, node_id))
        l = launcher.InProcessLauncher(cfg, metrics_store, races_root)
    elif external:
        if len(plugins) > 0:
            raise exceptions.SystemSetupError("You cannot specify any plugins for externally provisioned clusters. Please remove "
                                              "\"--elasticsearch-plugins\" and try again.")

        s = lambda: None
        p = [provisioner.no_op_provisioner()]
        l = launcher.ExternalLauncher(cfg, metrics_store)
    elif docker:
        if len(plugins) > 0:
            raise exceptions.SystemSetupError("You cannot specify any plugins for Docker clusters. Please remove "
                                              "\"--elasticsearch-plugins\" and try again.")
        s = lambda: None
        p = []
        for node_id in node_ids:
            p.append(provisioner.docker_provisioner(cfg, car, cluster_settings, challenge_root_path, node_id))
        l = launcher.DockerLauncher(cfg, metrics_store)
    else:
        # It is a programmer error (and not a user error) if this function is called with wrong parameters
        raise RuntimeError("One of sources, distribution, docker or external must be True")

    return Mechanic(s, p, l)


class Mechanic:
    """
    Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
    running the benchmark).
    """

    def __init__(self, supply, provisioners, l):
        self.supply = supply
        self.provisioners = provisioners
        self.launcher = l
        self.nodes = []

    def start_engine(self):
        binaries = self.supply()
        node_configs = []
        for p in self.provisioners:
            node_configs.append(p.prepare(binaries))
        self.nodes = self.launcher.start(node_configs)
        return self.nodes

    def on_benchmark_start(self):
        for node in self.nodes:
            node.on_benchmark_start()

    def on_benchmark_stop(self):
        for node in self.nodes:
            node.on_benchmark_stop()

    def stop_engine(self):
        self.launcher.stop(self.nodes)
        self.nodes = []
        for p in self.provisioners:
            p.cleanup()
