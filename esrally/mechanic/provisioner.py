import os
import glob
import shutil
import logging
from enum import Enum

import jinja2

from esrally import exceptions
from esrally.utils import io, console, process, modules

logger = logging.getLogger("rally.provisioner")


def local_provisioner(cfg, car, plugins, cluster_settings, all_node_ips, target_root, node_id):
    ip = cfg.opts("provisioning", "node.ip")
    http_port = cfg.opts("provisioning", "node.http.port")
    node_name_prefix = cfg.opts("provisioning", "node.name.prefix")
    preserve = cfg.opts("mechanic", "preserve.install")
    data_root_paths = cfg.opts("mechanic", "node.datapaths")

    node_name = "%s-%d" % (node_name_prefix, node_id)
    node_root_dir = "%s/%s" % (target_root, node_name)

    es_installer = ElasticsearchInstaller(car, node_name, node_root_dir, data_root_paths, all_node_ips, ip, http_port)
    plugin_installers = [PluginInstaller(plugin) for plugin in plugins]

    return BareProvisioner(cluster_settings, es_installer, plugin_installers, preserve)


def no_op_provisioner():
    return NoOpProvisioner()


def docker_provisioner(cfg, car, cluster_settings, target_root, node_id):
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    ip = cfg.opts("provisioning", "node.ip")
    http_port = cfg.opts("provisioning", "node.http.port")
    rally_root = cfg.opts("node", "rally.root")
    node_name_prefix = cfg.opts("provisioning", "node.name.prefix")
    preserve = cfg.opts("mechanic", "preserve.install")

    node_name = "%s-%d" % (node_name_prefix, node_id)
    node_root_dir = "%s/%s" % (target_root, node_name)

    return DockerProvisioner(car, node_name, cluster_settings, ip, http_port, node_root_dir, distribution_version,
                             rally_root, preserve)


class NodeConfiguration:
    def __init__(self, car, ip, node_name, node_root_path, binary_path, log_path, data_paths):
        self.car = car
        self.ip = ip
        self.node_name = node_name
        self.node_root_path = node_root_path
        self.binary_path = binary_path
        self.log_path = log_path
        self.data_paths = data_paths


class ConfigLoader:
    def __init__(self):
        pass

    def load(self):
        pass


def _render_template(env, variables, file_name):
    template = env.get_template(io.basename(file_name))
    # force a new line at the end. Jinja seems to remove it.
    return template.render(variables) + "\n"


def plain_text(file):
    _, ext = io.splitext(file)
    return ext in [".ini", ".txt", ".json", ".yml", ".yaml", ".options", ".properties"]


def cleanup(preserve, install_dir, data_paths):
    if preserve:
        logger.info("Preserving benchmark candidate installation at [%s]." % install_dir)
        console.info("Keeping benchmark candidate including index at [%s] (will need several GB)." % install_dir)
    else:
        logger.info("Wiping benchmark candidate installation at [%s]." % install_dir)
        for path in data_paths:
            if os.path.exists(path):
                try:
                    shutil.rmtree(path)
                except OSError:
                    logger.exception("Could not delete [%s]. Skipping..." % path)

        if os.path.exists(install_dir):
            try:
                shutil.rmtree(install_dir)
            except OSError:
                logger.exception("Could not delete [%s]. Skipping..." % install_dir)


class ProvisioningPhase(Enum):
    post_install = 10

    @classmethod
    def valid(cls, name):
        for n in ProvisioningPhase.names():
            if n == name:
                return True
        return False

    @classmethod
    def names(cls):
        return [p.name for p in list(ProvisioningPhase)]


def _apply_config(source_root_path, target_root_path, config_vars):
    for root, dirs, files in os.walk(source_root_path):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))

        relative_root = root[len(source_root_path) + 1:]
        absolute_target_root = os.path.join(target_root_path, relative_root)
        io.ensure_dir(absolute_target_root)

        for name in files:
            source_file = os.path.join(root, name)
            target_file = os.path.join(absolute_target_root, name)
            if plain_text(source_file):
                logger.info("Reading config template file [%s] and writing to [%s]." % (source_file, target_file))
                # automatically merge config snippets from plugins (e.g. if they want to add config to elasticsearch.yml)
                with open(target_file, "a") as f:
                    f.write(_render_template(env, config_vars, source_file))
            else:
                logger.info("Treating [%s] as binary and copying as is to [%s]." % (source_file, target_file))
                shutil.copy(source_file, target_file)


class BareProvisioner:
    """
    The provisioner prepares the runtime environment for running the benchmark. It prepares all configuration files and copies the binary
    of the benchmark candidate to the appropriate place.
    """

    def __init__(self, cluster_settings, es_installer, plugin_installers, preserve, apply_config=_apply_config):
        self.preserve = preserve
        self._cluster_settings = cluster_settings
        self.es_installer = es_installer
        self.plugin_installers = plugin_installers
        self.apply_config = apply_config

    def prepare(self, binary):
        self._install_binary(binary)
        return self._configure()

    def cleanup(self):
        self.es_installer.cleanup(self.preserve)

    def _install_binary(self, binaries):
        if not self.preserve:
            console.info("Rally will delete the benchmark candidate after the benchmark")
        self.es_installer.install(binaries["elasticsearch"])
        # we need to immediately delete it as plugins may copy their configuration during installation.
        self.es_installer.delete_pre_bundled_configuration()

        for installer in self.plugin_installers:
            installer.install(self.es_installer.es_home_path, binaries.get(installer.plugin_name))

    def _configure(self):
        target_root_path = self.es_installer.es_home_path
        provisioner_vars = self._provisioner_variables()

        for p in self.es_installer.config_source_paths:
            self.apply_config(p, target_root_path, provisioner_vars)

        for installer in self.plugin_installers:
            for plugin_config_path in installer.config_source_paths:
                self.apply_config(plugin_config_path, target_root_path, provisioner_vars)

        for installer in self.plugin_installers:
            # Never let install hooks modify our original provisioner variables and just provide a copy!
            installer.invoke_install_hook(ProvisioningPhase.post_install, provisioner_vars.copy())

        return NodeConfiguration(self.es_installer.car, self.es_installer.node_ip, self.es_installer.node_name,
                                 self.es_installer.node_root_dir, self.es_installer.es_home_path, self.es_installer.node_log_dir,
                                 self.es_installer.data_paths)

    def _provisioner_variables(self):
        plugin_variables = {}
        mandatory_plugins = []
        for installer in self.plugin_installers:
            mandatory_plugins.append(installer.plugin_name)
            plugin_variables.update(installer.variables)

        cluster_settings = {}
        # Merge cluster config from the track. These may not be dynamically updateable so we need to define them in the config file.
        cluster_settings.update(self._cluster_settings)
        if mandatory_plugins:
            # as a safety measure, prevent the cluster to startup if something went wrong during plugin installation which
            # we did not detect already here. This ensures we fail fast.
            #
            # https://www.elastic.co/guide/en/elasticsearch/plugins/current/_plugins_directory.html#_mandatory_plugins
            cluster_settings["plugin.mandatory"] = mandatory_plugins

        provisioner_vars = {}
        provisioner_vars.update(self.es_installer.variables)
        provisioner_vars.update(plugin_variables)
        provisioner_vars["cluster_settings"] = cluster_settings

        return provisioner_vars


class ElasticsearchInstaller:
    def __init__(self, car, node_name, node_root_dir, data_root_paths, all_node_ips, ip, http_port):
        self.car = car
        self.node_name = node_name
        self.node_root_dir = node_root_dir
        self.install_dir = "%s/install" % node_root_dir
        self.node_log_dir = "%s/logs/server" % node_root_dir
        self.data_root_paths = data_root_paths
        self.all_node_ips = all_node_ips
        self.node_ip = ip
        self.http_port = http_port
        self.es_home_path = None
        self.data_paths = None

    def install(self, binary):
        logger.info("Preparing candidate locally in [%s]." % self.install_dir)
        io.ensure_dir(self.install_dir)
        io.ensure_dir(self.node_log_dir)

        logger.info("Unzipping %s to %s" % (binary, self.install_dir))
        io.decompress(binary, self.install_dir)
        self.es_home_path = glob.glob("%s/elasticsearch*" % self.install_dir)[0]
        self.data_paths = self._data_paths()

    def delete_pre_bundled_configuration(self):
        config_path = os.path.join(self.es_home_path, "config")
        logger.info("Deleting pre-bundled Elasticsearch configuration at [%s]" % config_path)
        shutil.rmtree(config_path)

    def cleanup(self, preserve):
        cleanup(preserve, self.install_dir, self.data_paths)

    @property
    def variables(self):
        # bind as specifically as possible
        network_host = self.node_ip

        defaults = {
            "cluster_name": "rally-benchmark",
            "node_name": self.node_name,
            "data_paths": self.data_paths,
            "log_path": self.node_log_dir,
            # this is the node's IP address as specified by the user when invoking Rally
            "node_ip": self.node_ip,
            # this is the IP address that the node will be bound to. Rally will bind to the node's IP address (but not to 0.0.0.0). The
            # reason is that we use the node's IP address as subject alternative name in x-pack.
            "network_host": network_host,
            "http_port": "%d-%d" % (self.http_port, self.http_port + 100),
            "transport_port": "%d-%d" % (self.http_port + 100, self.http_port + 200),
            "all_node_ips": "[\"%s\"]" % "\",\"".join(self.all_node_ips),
            # at the moment we are strict and enforce that all nodes are master eligible nodes
            "minimum_master_nodes": len(self.all_node_ips),
            # We allow multiple nodes per host but we do not allow that they share their data directories
            "node_count_per_host": 1,
            "install_root_path": self.es_home_path
        }
        variables = {}
        variables.update(self.car.variables)
        variables.update(defaults)
        return variables

    @property
    def config_source_paths(self):
        return self.car.config_paths

    def _data_paths(self):
        roots = self.data_root_paths if self.data_root_paths else [self.es_home_path]
        return [os.path.join(root, "data") for root in roots]


class InstallHookHandler:
    def __init__(self, plugin, loader_class=modules.ComponentLoader):
        self.plugin = plugin
        # Don't allow the loader to recurse. The subdirectories may contain Elasticsearch specific files which we do not want to add to
        # Rally's Python load path. We may need to define a more advanced strategy in the future.
        self.loader = loader_class(root_path=self.plugin.root_path, component_entry_point="plugin", recurse=False)
        self.hooks = {}

    def can_load(self):
        return self.loader.can_load()

    def load(self):
        root_module = self.loader.load()
        try:
            # every module needs to have a register() method
            root_module.register(self)
        except exceptions.RallyError:
            # just pass our own exceptions transparently.
            raise
        except BaseException:
            msg = "Could not load install hooks in [%s]" % self.loader.root_path
            logger.exception(msg)
            raise exceptions.SystemSetupError(msg)

    def register(self, phase, hook):
        logger.info("Registering install hook [%s] for phase [%s] in plugin [%s]" % (hook.__name__, phase, self.plugin.name))
        if not ProvisioningPhase.valid(phase):
            raise exceptions.SystemSetupError("Provisioning phase [%s] is unknown. Valid phases are: %s." %
                                              (phase, ProvisioningPhase.names()))
        if phase not in self.hooks:
            self.hooks[phase] = []
        self.hooks[phase].append(hook)

    def invoke(self, phase, variables):
        if phase in self.hooks:
            logger.info("Invoking phase [%s] for plugin [%s] in config [%s]" % (phase, self.plugin.name, self.plugin.config))
            for hook in self.hooks[phase]:
                logger.info("Invoking hook [%s]." % hook.__name__)
                # hooks should only take keyword arguments to be forwards compatible with Rally!
                hook(config_names=self.plugin.config, variables=variables)
        else:
            logger.debug("Plugin [%s] in config [%s] has no hook registered for phase [%s]." % (self.plugin.name, self.plugin.config, phase))


class PluginInstaller:
    def __init__(self, plugin, hook_handler_class=InstallHookHandler):
        self.plugin = plugin
        self.hook_handler = hook_handler_class(self.plugin)
        if self.hook_handler.can_load():
            self.hook_handler.load()

    def install(self, es_home_path, plugin_url=None):
        installer_binary_path = os.path.join(es_home_path, "bin", "elasticsearch-plugin")
        if plugin_url:
            logger.info("Installing [%s] into [%s] from [%s]" % (self.plugin_name, es_home_path, plugin_url))
            install_cmd = '%s install --batch "%s"' % (installer_binary_path, plugin_url)
        else:
            logger.info("Installing [%s] into [%s]" % (self.plugin_name, es_home_path))
            install_cmd = '%s install --batch "%s"' % (installer_binary_path, self.plugin_name)

        return_code = process.run_subprocess_with_logging(install_cmd)
        # see: https://www.elastic.co/guide/en/elasticsearch/plugins/current/_other_command_line_parameters.html
        if return_code == 0:
            logger.info("Successfully installed [%s]." % self.plugin_name)
        elif return_code == 64:
            # most likely this is an unknown plugin
            raise exceptions.SystemSetupError("Unknown plugin [%s]" % self.plugin_name)
        elif return_code == 74:
            raise exceptions.SupplyError("I/O error while trying to install [%s]" % self.plugin_name)
        else:
            raise exceptions.RallyError("Unknown error while trying to install [%s] (installer return code [%s]). Please check the logs." %
                                        (self.plugin_name, str(return_code)))

    def invoke_install_hook(self, phase, variables):
        self.hook_handler.invoke(phase.name, variables)

    @property
    def variables(self):
        return self.plugin.variables

    @property
    def config_source_paths(self):
        return self.plugin.config_paths

    @property
    def plugin_name(self):
        return self.plugin.name


class NoOpProvisioner:
    def __init__(self, *args):
        pass

    def prepare(self, *args):
        return None

    def cleanup(self):
        pass


class DockerProvisioner:
    def __init__(self, car, node_name, cluster_settings, ip, http_port, node_root_dir, distribution_version, rally_root, preserve):
        self.car = car
        self.node_name = node_name
        self.node_ip = ip
        self.http_port = http_port
        self.node_root_dir = node_root_dir
        self.node_log_dir = "%s/logs/server" % node_root_dir
        self.distribution_version = distribution_version
        self.rally_root = rally_root
        self.install_dir = "%s/install" % node_root_dir
        self.data_paths = ["%s/data" % self.install_dir]
        self.preserve = preserve
        self.binary_path = "%s/docker-compose.yml" % self.install_dir

        # Merge cluster config from the track. These may not be dynamically updateable so we need to define them in the config file.
        merged_cluster_settings = cluster_settings.copy()
        # disable x-pack features as we don't use them and want to compare with plain vanilla ES
        merged_cluster_settings["xpack.security.enabled"] = "false"
        merged_cluster_settings["xpack.ml.enabled"] = "false"
        merged_cluster_settings["xpack.monitoring.enabled"] = "false"
        merged_cluster_settings["xpack.watcher.enabled"] = "false"

        provisioner_defaults = {
            "cluster_name": "rally-benchmark",
            "node_name": self.node_name,
            # we bind-mount the directories below on the host to these ones.
            "data_paths": ["/usr/share/elasticsearch/data"],
            "log_path": "/var/log/elasticsearch",
            # Docker container needs to expose service on external interfaces
            "network_host": "0.0.0.0",
            "http_port": "%d-%d" % (self.http_port, self.http_port + 100),
            "transport_port": "%d-%d" % (self.http_port + 100, self.http_port + 200),
            "node_count_per_host": 1,
            "cluster_settings": merged_cluster_settings
        }

        self.config_vars = {}
        self.config_vars.update(self.car.variables)
        self.config_vars.update(provisioner_defaults)

    def prepare(self, binaries):
        # we need to allow other users to write to these directories due to Docker.
        #
        # Although os.mkdir passes 0o777 by default, mkdir(2) uses `mode & ~umask & 0777` to determine the final flags and
        # hence we need to modify the process' umask here. For details see https://linux.die.net/man/2/mkdir.
        previous_umask = os.umask(0)
        try:
            io.ensure_dir(self.install_dir)
            io.ensure_dir(self.node_log_dir)
            io.ensure_dir(self.data_paths[0])
        finally:
            os.umask(previous_umask)

        mounts = {}

        for car_config_path in self.car.config_paths:
            for root, dirs, files in os.walk(car_config_path):
                env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))

                relative_root = root[len(car_config_path) + 1:]
                absolute_target_root = os.path.join(self.install_dir, relative_root)
                io.ensure_dir(absolute_target_root)

                for name in files:
                    source_file = os.path.join(root, name)
                    target_file = os.path.join(absolute_target_root, name)
                    mounts[target_file] = os.path.join("/usr/share/elasticsearch", relative_root, name)
                    if plain_text(source_file):
                        logger.info("Reading config template file [%s] and writing to [%s]." % (source_file, target_file))
                        with open(target_file, "a") as f:
                            f.write(_render_template(env, self.config_vars, source_file))
                    else:
                        logger.info("Treating [%s] as binary and copying as is to [%s]." % (source_file, target_file))
                        shutil.copy(source_file, target_file)

        docker_cfg = self._render_template_from_file(self.docker_vars(mounts))
        logger.info("Starting Docker container with configuration:\n%s" % docker_cfg)

        with open(self.binary_path, "wt") as f:
            f.write(docker_cfg)

        return NodeConfiguration(self.car, self.node_ip, self.node_name, self.node_root_dir, self.binary_path,
                                 self.node_log_dir, self.data_paths)

    def cleanup(self):
        cleanup(self.preserve, self.install_dir, self.data_paths)

    def docker_vars(self, mounts):
        return {
            "es_version": self.distribution_version,
            "http_port": self.http_port,
            "es_data_dir": self.data_paths[0],
            "es_log_dir": self.node_log_dir,
            "mounts": mounts
        }

    def _render_template(self, loader, template_name, variables):
        env = jinja2.Environment(loader=loader)
        for k, v in variables.items():
            env.globals[k] = v
        template = env.get_template(template_name)

        return template.render()

    def _render_template_from_file(self, variables):
        compose_file = "%s/resources/docker-compose.yml" % self.rally_root
        return self._render_template(loader=jinja2.FileSystemLoader(io.dirname(compose_file)),
                                     template_name=io.basename(compose_file),
                                     variables=variables)
