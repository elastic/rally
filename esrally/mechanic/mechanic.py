import sys
import logging

import thespian.actors

from esrally import actor, paths, config, metrics, exceptions, time, PROGRAM_NAME
from esrally.utils import console, io
from esrally.mechanic import supplier, provisioner, launcher

logger = logging.getLogger("rally.mechanic")


##########
# Messages
##########

class ClusterMetaInfo:
    def __init__(self, nodes, revision, distribution_version):
        self.nodes = nodes
        self.revision = revision
        self.distribution_version = distribution_version

    def as_dict(self):
        return {
            "nodes": [n.as_dict() for n in self.nodes],
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

    def as_dict(self):
        return self.__dict__


class StartEngine:
    def __init__(self, cfg, open_metrics_context, cluster_settings, sources, build, distribution, external, docker, port=None):
        self.cfg = cfg
        self.open_metrics_context = open_metrics_context
        self.cluster_settings = cluster_settings
        self.sources = sources
        self.build = build
        self.distribution = distribution
        self.external = external
        self.docker = docker
        self.port = port

    def with_port(self, port):
        """

        Creates a copy of this StartEngine instance but with a modified port argument. This simplifies sending a customized ``StartEngine``
        message to each of the worker mechanics.

        :param port: The port number to set in the copy.
        :return: A shallow copy of this message with the specified port number.
        """
        return StartEngine(self.cfg, self.open_metrics_context, self.cluster_settings, self.sources, self.build, self.distribution,
                           self.external, self.docker, port)


class EngineStarted:
    def __init__(self, cluster_meta_info, system_meta_info):
        self.cluster_meta_info = cluster_meta_info
        self.system_meta_info = system_meta_info


class StopEngine:
    pass


class EngineStopped:
    def __init__(self, system_metrics):
        self.system_metrics = system_metrics


class Success:
    pass


class Failure:
    def __init__(self, message, cause):
        self.message = message
        self.cause = cause


class OnBenchmarkStart:
    def __init__(self, lap):
        self.lap = lap


class OnBenchmarkStop:
    pass


class BenchmarkStopped:
    def __init__(self, system_metrics):
        self.system_metrics = system_metrics


class MechanicActor(actor.RallyActor):
    """
    This actor coordinates all associated mechanics on remote hosts (which do the actual work).
    """
    def __init__(self):
        super().__init__()
        actor.RallyActor.configure_logging(logger)
        self.mechanics = []
        self.race_control = None

    # TODO #71: When we use this for multi-machine clusters, we need to implement a state-machine (wait for all nodes) and also need
    # TODO #71: more context (e.g. "Success" / "Failure" maybe won't cut it).
    def receiveMessage(self, msg, sender):
        try:
            logger.debug("MechanicActor#receiveMessage(msg = [%s] sender = [%s])" % (str(type(msg)), str(sender)))
            if isinstance(msg, StartEngine):
                logger.info("Received signal from race control to start engine.")
                self.race_control = sender
                # In our startup procedure we first create all mechanics. Only if this succeeds
                mechanics_and_start_message = []

                if msg.external:
                    logger.info("Target node(s) will not be provisioned by Rally.")
                    # just create one actor for this special case and run it on the coordinator node (i.e. here)
                    m = self.createActor(LocalNodeMechanicActor,
                                         globalName="/rally/mechanic/worker/external",
                                         targetActorRequirements={"coordinator": True})
                    self.mechanics.append(m)
                    # we can use the original message in this case
                    mechanics_and_start_message.append((m, msg))
                else:
                    hosts = msg.cfg.opts("client", "hosts")
                    logger.info("Target node(s) %s will be provisioned by Rally." % hosts)
                    if len(hosts) == 0:
                        raise exceptions.LaunchError("No target hosts are configured.")
                    for host in hosts:
                        ip = host["host"]
                        port = int(host["port"])
                        # user may specify "localhost" on the command line but the problem is that we auto-register the actor system
                        # with "ip": "127.0.0.1" so we convert this special case automatically. In all other cases the user needs to
                        # start the actor system on the other host and is aware that the parameter for the actor system and the
                        # --target-hosts parameter need to match.
                        if ip == "localhost" or ip == "127.0.0.1":
                            m = self.createActor(LocalNodeMechanicActor,
                                                 globalName="/rally/mechanic/worker/localhost",
                                                 targetActorRequirements={"coordinator": True})
                            self.mechanics.append(m)
                            mechanics_and_start_message.append((m, msg.with_port(port)))
                        else:
                            if msg.cfg.opts("system", "remote.benchmarking.supported"):
                                logger.info("Benchmarking against %s with external Rally daemon." % hosts)
                            else:
                                logger.error("User tried to benchmark against %s but no external Rally daemon has been started." % hosts)
                                raise exceptions.SystemSetupError("To benchmark remote hosts (e.g. %s) you need to start the Rally daemon "
                                                                  "on each machine including this one." % ip)
                            already_running = actor.actor_system_already_running(ip=ip)
                            logger.info("Actor system on [%s] already running? [%s]" % (ip, str(already_running)))
                            if not already_running:
                                console.println("Waiting for Rally daemon on [%s] " % ip, end="", flush=True)
                            while not actor.actor_system_already_running(ip=ip):
                                console.println(".", end="", flush=True)
                                time.sleep(3)
                            if not already_running:
                                console.println(" [OK]")
                            m = self.createActor(RemoteNodeMechanicActor,
                                                 globalName="/rally/mechanic/worker/%s" % ip,
                                                 targetActorRequirements={"ip": ip})
                            mechanics_and_start_message.append((m, msg.with_port(port)))
                            self.mechanics.append(m)
                for mechanic_actor, start_message in mechanics_and_start_message:
                    self.send(mechanic_actor, start_message)
            elif isinstance(msg, EngineStarted):
                self.send(self.race_control, msg)
            elif isinstance(msg, OnBenchmarkStart):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, Success):
                self.send(self.race_control, msg)
            elif isinstance(msg, Failure):
                self.send(self.race_control, msg)
            elif isinstance(msg, OnBenchmarkStop):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, BenchmarkStopped):
                # TODO dm: Actually we need to wait for all BenchmarkStopped messages from all our mechanic actors
                # TODO dm: We will actually duplicate cluster level metrics if each of our mechanic actors gathers these...
                self.send(self.race_control, msg)
            elif isinstance(msg, StopEngine):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, EngineStopped):
                self.send(self.race_control, msg)
                # clear all state as the mechanic might get reused later
                for m in self.mechanics:
                    self.send(m, thespian.actors.ActorExitRequest())
                self.mechanics = []
                # self terminate + slave nodes
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
            elif isinstance(msg, thespian.actors.ChildActorExited):
                # TODO dm: Depending on our state model this can be fine (e.g. when it exited due to our ActorExitRequest message
                # or it could be problematic and mean that an exception has occured.
                pass
            elif isinstance(msg, thespian.actors.PoisonMessage):
                # something went wrong with a child actor
                if isinstance(msg.poisonMessage, StartEngine):
                    raise exceptions.LaunchError("Could not start benchmark candidate. Are Rally daemons on all targeted machines running?")
                else:
                    logger.error("[%s] sent to a child actor has resulted in PoisonMessage" % str(msg.poisonMessage))
                    raise exceptions.RallyError("Could not communicate with benchmark candidate (unknown reason)")
        except BaseException:
            logger.exception("Cannot process message [%s]" % msg)
            # usually, we'll notify the sender but in case a child sent something that caused an exception we'd rather
            # have it bubble up to race control. Otherwise, we could play ping-pong with our child actor.
            recipient = self.race_control if sender in self.mechanics else sender
            ex_type, ex_value, ex_traceback = sys.exc_info()
            # avoid "can't pickle traceback objects"
            import traceback
            self.send(recipient, Failure("Could not execute command (%s)" % ex_value, traceback.format_exc()))


class NodeMechanicActor(actor.RallyActor):
    """
    One instance of this actor is run on each target host and coordinates the actual work of starting / stopping nodes.
    """
    def __init__(self, single_machine):
        super().__init__()
        actor.RallyActor.configure_logging(logger)
        self.config = None
        self.metrics_store = None
        self.mechanic = None
        self.single_machine = single_machine

    def receiveMessage(self, msg, sender):
        # at the moment, we implement all message handling blocking. This is not ideal but simple to get started with. Besides, the caller
        # needs to block anyway. The only reason we implement mechanic as an actor is to distribute them.
        # noinspection PyBroadException
        try:
            logger.debug("NodeMechanicActor#receiveMessage(msg = [%s] sender = [%s])" % (str(type(msg)), str(sender)))
            if isinstance(msg, StartEngine):
                logger.info("Starting engine")
                # Load node-specific configuration
                self.config = config.Config(config_name=msg.cfg.name)
                self.config.load_config()
                self.config.add(config.Scope.application, "node", "rally.root", paths.rally_root())
                # copy only the necessary configuration sections
                self.config.add_all(msg.cfg, "system")
                self.config.add_all(msg.cfg, "client")
                self.config.add_all(msg.cfg, "track")
                self.config.add_all(msg.cfg, "mechanic")
                if msg.port is not None:
                    # we need to override the port with the value that the user has specified instead of using the default value (39200)
                    self.config.add(config.Scope.benchmark, "provisioning", "node.http.port", msg.port)

                self.metrics_store = metrics.InMemoryMetricsStore(self.config)
                self.metrics_store.open(ctx=msg.open_metrics_context)

                self.mechanic = create(self.config, self.metrics_store, self.single_machine, msg.cluster_settings, msg.sources, msg.build,
                                       msg.distribution, msg.external, msg.docker)
                cluster = self.mechanic.start_engine()
                self.send(sender, EngineStarted(
                    ClusterMetaInfo([NodeMetaInfo(node) for node in cluster.nodes], cluster.source_revision, cluster.distribution_version),
                    self.metrics_store.meta_info))
            elif isinstance(msg, OnBenchmarkStart):
                self.metrics_store.lap = msg.lap
                self.mechanic.on_benchmark_start()
                self.send(sender, Success())
            elif isinstance(msg, OnBenchmarkStop):
                self.mechanic.on_benchmark_stop()
                # clear metrics store data to not send duplicate system metrics data
                self.send(sender, BenchmarkStopped(self.metrics_store.to_externalizable(clear=True)))
            elif isinstance(msg, StopEngine):
                logger.info("Stopping engine")
                self.mechanic.stop_engine()
                self.send(sender, EngineStopped(self.metrics_store.to_externalizable()))
                # clear all state as the mechanic might get reused later
                self.config = None
                self.mechanic = None
                self.metrics_store = None
        except BaseException:
            logger.exception("Cannot process message [%s]" % msg)
            # avoid "can't pickle traceback objects"
            import traceback
            ex_type, ex_value, ex_traceback = sys.exc_info()
            self.send(sender, Failure(ex_value, traceback.format_exc()))


class LocalNodeMechanicActor(NodeMechanicActor):
    def __init__(self):
        super().__init__(single_machine=True)


class RemoteNodeMechanicActor(NodeMechanicActor):
    def __init__(self):
        super().__init__(single_machine=False)


#####################################################
# Internal API (only used by the actor and for tests)
#####################################################

def create(cfg, metrics_store, single_machine=True, cluster_settings=None, sources=False, build=False, distribution=False, external=False, docker=False):
    challenge_root_path = paths.race_root(cfg)
    install_dir = "%s/install" % challenge_root_path
    log_dir = "%s/logs" % challenge_root_path
    io.ensure_dir(log_dir)

    if sources:
        try:
            src_dir = cfg.opts("source", "local.src.dir")
        except config.ConfigError:
            logger.exception("Cannot determine source directory")
            raise exceptions.SystemSetupError("You cannot benchmark Elasticsearch from sources. Are you missing Gradle? Please install"
                                              " all prerequisites and reconfigure Rally with %s configure" % PROGRAM_NAME)

        remote_url = cfg.opts("source", "remote.repo.url")
        revision = cfg.opts("mechanic", "source.revision")
        gradle = cfg.opts("build", "gradle.bin")
        java_home = cfg.opts("runtime", "java8.home")

        s = lambda: supplier.from_sources(remote_url, src_dir, revision, gradle, java_home, log_dir, build)
        p = provisioner.local_provisioner(cfg, cluster_settings, install_dir, single_machine)
        l = launcher.InProcessLauncher(cfg, metrics_store, challenge_root_path, log_dir)
    elif distribution:
        version = cfg.opts("mechanic", "distribution.version")
        repo_name = cfg.opts("mechanic", "distribution.repository")
        distributions_root = "%s/%s" % (cfg.opts("node", "root.dir"), cfg.opts("source", "distribution.dir"))

        s = lambda: supplier.from_distribution(version=version, repo_name=repo_name, distributions_root=distributions_root)
        p = provisioner.local_provisioner(cfg, cluster_settings, install_dir, single_machine)
        l = launcher.InProcessLauncher(cfg, metrics_store, challenge_root_path, log_dir)
    elif external:
        if cluster_settings:
            logger.warning("Cannot apply challenge-specific cluster settings [%s] for an externally provisioned cluster. Please ensure "
                           "that the cluster settings are present or the benchmark may fail or behave unexpectedly." % cluster_settings)
        s = lambda: None
        p = provisioner.no_op_provisioner(cfg)
        l = launcher.ExternalLauncher(cfg, metrics_store)
    elif docker:
        s = lambda: None
        p = provisioner.docker_provisioner(cfg, cluster_settings, install_dir)
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

    def __init__(self, supply, p, l):
        self.supply = supply
        self.provisioner = p
        self.launcher = l
        self.cluster = None

    def start_engine(self):
        binary = self.supply()
        self.provisioner.prepare(binary)
        logger.info("Starting engine.")
        self.cluster = self.launcher.start(self.provisioner.car, self.provisioner.binary_path, self.provisioner.data_paths)
        return self.cluster

    def on_benchmark_start(self):
        logger.info("Notifying cluster of benchmark start.")
        self.cluster.on_benchmark_start()

    def on_benchmark_stop(self):
        logger.info("Notifying cluster of benchmark stop.")
        self.cluster.on_benchmark_stop()

    def stop_engine(self):
        logger.info("Stopping engine.")
        self.launcher.stop(self.cluster)
        self.cluster = None
        self.provisioner.cleanup()
