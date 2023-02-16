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

import glob
import json
import logging
import os
import shutil
import uuid

import jinja2

from esrally import exceptions
from esrally.mechanic import java_resolver, team
from esrally.utils import console, convert, io, process


def local(cfg, car, plugins, ip, http_port, all_node_ips, all_node_names, target_root, node_name):
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    cluster_name = cfg.opts("mechanic", "cluster.name")

    node_root_dir = os.path.join(target_root, node_name)

    runtime_jdk_bundled = convert.to_bool(car.mandatory_var("runtime.jdk.bundled"))
    runtime_jdk = car.mandatory_var("runtime.jdk")
    _, java_home = java_resolver.java_home(runtime_jdk, cfg.opts("mechanic", "runtime.jdk"), runtime_jdk_bundled)

    es_installer = ElasticsearchInstaller(
        car, java_home, node_name, cluster_name, node_root_dir, all_node_ips, all_node_names, ip, http_port
    )
    plugin_installers = [PluginInstaller(plugin, java_home) for plugin in plugins]

    return BareProvisioner(es_installer, plugin_installers, distribution_version=distribution_version)


def docker(cfg, car, ip, http_port, target_root, node_name):
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    cluster_name = cfg.opts("mechanic", "cluster.name")
    rally_root = cfg.opts("node", "rally.root")

    node_root_dir = os.path.join(target_root, node_name)

    return DockerProvisioner(car, node_name, cluster_name, ip, http_port, node_root_dir, distribution_version, rally_root)


class NodeConfiguration:
    def __init__(self, build_type, car_runtime_jdks, car_provides_bundled_jdk, ip, node_name, node_root_path, binary_path, data_paths):
        self.build_type = build_type
        self.car_runtime_jdks = car_runtime_jdks
        self.car_provides_bundled_jdk = car_provides_bundled_jdk
        self.ip = ip
        self.node_name = node_name
        self.node_root_path = node_root_path
        self.binary_path = binary_path
        self.data_paths = data_paths

    def as_dict(self):
        return {
            "build-type": self.build_type,
            "car-runtime-jdks": self.car_runtime_jdks,
            "car-provides-bundled-jdk": self.car_provides_bundled_jdk,
            "ip": self.ip,
            "node-name": self.node_name,
            "node-root-path": self.node_root_path,
            "binary-path": self.binary_path,
            "data-paths": self.data_paths,
        }

    @staticmethod
    def from_dict(d):
        return NodeConfiguration(
            d["build-type"],
            d["car-runtime-jdks"],
            d["car-provides-bundled-jdk"],
            d["ip"],
            d["node-name"],
            d["node-root-path"],
            d["binary-path"],
            d["data-paths"],
        )


def save_node_configuration(path, n):
    with open(os.path.join(path, "node-config.json"), "w") as f:
        json.dump(n.as_dict(), f, indent=2)


def load_node_configuration(path):
    with open(os.path.join(path, "node-config.json")) as f:
        return NodeConfiguration.from_dict(json.load(f))


class ConfigLoader:
    def __init__(self):
        pass

    def load(self):
        pass


def _render_template(env, variables, file_name):
    try:
        template = env.get_template(io.basename(file_name))
        # force a new line at the end. Jinja seems to remove it.
        return template.render(variables) + "\n"
    except jinja2.exceptions.TemplateSyntaxError as e:
        raise exceptions.InvalidSyntax("%s in %s" % (str(e), file_name))
    except BaseException as e:
        raise exceptions.SystemSetupError("%s in %s" % (str(e), file_name))


def plain_text(file):
    _, ext = io.splitext(file)
    return ext in [".ini", ".txt", ".json", ".yml", ".yaml", ".options", ".properties"]


def cleanup(preserve, install_dir, data_paths):
    def delete_path(p):
        if os.path.exists(p):
            try:
                logger.debug("Deleting [%s].", p)
                shutil.rmtree(p)
            except OSError:
                logger.exception("Could not delete [%s]. Skipping...", p)

    logger = logging.getLogger(__name__)
    if preserve:
        console.info(f"Preserving benchmark candidate installation at [{install_dir}].", logger=logger)
    else:
        logger.info("Wiping benchmark candidate installation at [%s].", install_dir)
        for path in data_paths:
            delete_path(path)

        delete_path(install_dir)


def _apply_config(source_root_path, target_root_path, config_vars):
    logger = logging.getLogger(__name__)
    for root, _, files in os.walk(source_root_path):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))

        relative_root = root[len(source_root_path) + 1 :]
        absolute_target_root = os.path.join(target_root_path, relative_root)
        io.ensure_dir(absolute_target_root)

        for name in files:
            source_file = os.path.join(root, name)
            target_file = os.path.join(absolute_target_root, name)
            if plain_text(source_file):
                logger.info("Reading config template file [%s] and writing to [%s].", source_file, target_file)
                # automatically merge config snippets from plugins (e.g. if they want to add config to elasticsearch.yml)
                with open(target_file, mode="a", encoding="utf-8") as f:
                    f.write(_render_template(env, config_vars, source_file))
            else:
                logger.info("Treating [%s] as binary and copying as is to [%s].", source_file, target_file)
                shutil.copy(source_file, target_file)


class BareProvisioner:
    """
    The provisioner prepares the runtime environment for running the benchmark. It prepares all configuration files and copies the binary
    of the benchmark candidate to the appropriate place.
    """

    def __init__(self, es_installer, plugin_installers, distribution_version=None, apply_config=_apply_config):
        self.es_installer = es_installer
        self.plugin_installers = plugin_installers
        self.distribution_version = distribution_version
        self.apply_config = apply_config
        self.logger = logging.getLogger(__name__)

    def prepare(self, binary):
        self.es_installer.install(binary["elasticsearch"])
        # we need to immediately delete it as plugins may copy their configuration during installation.
        self.es_installer.delete_pre_bundled_configuration()

        # determine after installation because some variables will depend on the install directory
        target_root_path = self.es_installer.es_home_path
        provisioner_vars = self._provisioner_variables()
        for p in self.es_installer.config_source_paths:
            self.apply_config(p, target_root_path, provisioner_vars)

        for installer in self.plugin_installers:
            installer.install(target_root_path, binary.get(installer.plugin_name))
            for plugin_config_path in installer.config_source_paths:
                self.apply_config(plugin_config_path, target_root_path, provisioner_vars)

        # Never let install hooks modify our original provisioner variables and just provide a copy!
        self.es_installer.invoke_install_hook(team.BootstrapPhase.post_install, provisioner_vars.copy())
        for installer in self.plugin_installers:
            installer.invoke_install_hook(team.BootstrapPhase.post_install, provisioner_vars.copy())

        return NodeConfiguration(
            "tar",
            self.es_installer.car.mandatory_var("runtime.jdk"),
            convert.to_bool(self.es_installer.car.mandatory_var("runtime.jdk.bundled")),
            self.es_installer.node_ip,
            self.es_installer.node_name,
            self.es_installer.node_root_dir,
            self.es_installer.es_home_path,
            self.es_installer.data_paths,
        )

    def _provisioner_variables(self):
        plugin_variables = {}
        mandatory_plugins = []
        for installer in self.plugin_installers:
            plugin_variables.update(installer.variables)
            if installer.plugin.moved_to_module:
                self.logger.info(
                    "Skipping adding plugin [%s] to cluster setting 'plugin.mandatory' as it has been moved to a module",
                    installer.plugin_name,
                )
            else:
                mandatory_plugins.append(installer.plugin_name)

        cluster_settings = {}
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
    def __init__(
        self,
        car,
        java_home,
        node_name,
        cluster_name,
        node_root_dir,
        all_node_ips,
        all_node_names,
        ip,
        http_port,
        hook_handler_class=team.BootstrapHookHandler,
    ):
        self.car = car
        self.java_home = java_home
        self.node_name = node_name
        self.cluster_name = cluster_name
        self.node_root_dir = node_root_dir
        self.install_dir = os.path.join(node_root_dir, "install")
        self.node_log_dir = os.path.join(node_root_dir, "logs", "server")
        self.heap_dump_dir = os.path.join(node_root_dir, "heapdump")
        self.all_node_ips = all_node_ips
        self.all_node_names = all_node_names
        self.node_ip = ip
        self.http_port = http_port
        self.hook_handler = hook_handler_class(self.car)
        if self.hook_handler.can_load():
            self.hook_handler.load()
        self.es_home_path = None
        self.data_paths = None
        self.logger = logging.getLogger(__name__)

    def install(self, binary):
        self.logger.info("Preparing candidate locally in [%s].", self.install_dir)
        io.ensure_dir(self.install_dir)
        io.ensure_dir(self.node_log_dir)
        io.ensure_dir(self.heap_dump_dir)

        self.logger.info("Unzipping %s to %s", binary, self.install_dir)
        io.decompress(binary, self.install_dir)
        self.es_home_path = glob.glob(os.path.join(self.install_dir, "elasticsearch*"))[0]
        self.data_paths = self._data_paths()

    def delete_pre_bundled_configuration(self):
        config_path = os.path.join(self.es_home_path, "config")
        self.logger.info("Deleting pre-bundled Elasticsearch configuration at [%s]", config_path)
        shutil.rmtree(config_path)

    def invoke_install_hook(self, phase, variables):
        env = {}
        if self.java_home:
            env["JAVA_HOME"] = self.java_home
        self.hook_handler.invoke(phase.name, variables=variables, env=env)

    @property
    def variables(self):
        # bind as specifically as possible
        network_host = self.node_ip

        defaults = {
            "cluster_name": self.cluster_name,
            "node_name": self.node_name,
            "data_paths": self.data_paths,
            "log_path": self.node_log_dir,
            "heap_dump_path": self.heap_dump_dir,
            # this is the node's IP address as specified by the user when invoking Rally
            "node_ip": self.node_ip,
            # this is the IP address that the node will be bound to. Rally will bind to the node's IP address (but not to 0.0.0.0). The
            # reason is that we use the node's IP address as subject alternative name in x-pack.
            "network_host": network_host,
            "http_port": str(self.http_port),
            "transport_port": str(self.http_port + 100),
            "all_node_ips": '["%s"]' % '","'.join(self.all_node_ips),
            "all_node_names": '["%s"]' % '","'.join(self.all_node_names),
            "all_node_ips_count": len(self.all_node_ips),
            # at the moment we are strict and enforce that all nodes are master eligible nodes
            "minimum_master_nodes": len(self.all_node_ips),
            "install_root_path": self.es_home_path,
        }
        variables = {}
        variables.update(self.car.variables)
        variables.update(defaults)
        return variables

    @property
    def config_source_paths(self):
        return self.car.config_paths

    def _data_paths(self):
        if "data_paths" in self.car.variables:
            data_paths = self.car.variables["data_paths"]
            if isinstance(data_paths, str):
                return [data_paths]
            elif isinstance(data_paths, list):
                return data_paths
            else:
                raise exceptions.SystemSetupError("Expected [data_paths] to be either a string or a list but was [%s]." % type(data_paths))
        else:
            return [os.path.join(self.es_home_path, "data")]


class PluginInstaller:
    def __init__(self, plugin, java_home, hook_handler_class=team.BootstrapHookHandler):
        self.plugin = plugin
        self.java_home = java_home
        self.hook_handler = hook_handler_class(self.plugin)
        if self.hook_handler.can_load():
            self.hook_handler.load()
        self.logger = logging.getLogger(__name__)

    def install(self, es_home_path, plugin_url=None):
        installer_binary_path = os.path.join(es_home_path, "bin", "elasticsearch-plugin")
        if plugin_url:
            self.logger.info("Installing [%s] into [%s] from [%s]", self.plugin_name, es_home_path, plugin_url)
            install_cmd = '%s install --batch "%s"' % (installer_binary_path, plugin_url)
        else:
            self.logger.info("Installing [%s] into [%s]", self.plugin_name, es_home_path)
            install_cmd = '%s install --batch "%s"' % (installer_binary_path, self.plugin_name)

        return_code = process.run_subprocess_with_logging(install_cmd, env=self.env())
        # see: https://www.elastic.co/guide/en/elasticsearch/plugins/current/_other_command_line_parameters.html
        if return_code == 0:
            self.logger.info("Successfully installed [%s].", self.plugin_name)
        elif return_code == 64:
            # most likely this is an unknown plugin
            raise exceptions.SystemSetupError("Unknown plugin [%s]" % self.plugin_name)
        elif return_code == 74:
            raise exceptions.SupplyError("I/O error while trying to install [%s]" % self.plugin_name)
        else:
            raise exceptions.RallyError(
                "Unknown error while trying to install [%s] (installer return code [%s]). Please check the logs."
                % (self.plugin_name, str(return_code))
            )

    def invoke_install_hook(self, phase, variables):
        self.hook_handler.invoke(phase.name, variables=variables, env=self.env())

    def env(self):
        env = {}
        if self.java_home:
            env["JAVA_HOME"] = self.java_home
        return env

    @property
    def variables(self):
        return self.plugin.variables

    @property
    def config_source_paths(self):
        return self.plugin.config_paths

    @property
    def plugin_name(self):
        return self.plugin.name

    @property
    def sub_plugin_name(self):
        # if a plugin consists of multiple plugins (e.g. x-pack) we're interested in that name
        return self.variables.get("plugin_name", self.plugin_name)


class DockerProvisioner:
    def __init__(self, car, node_name, cluster_name, ip, http_port, node_root_dir, distribution_version, rally_root):
        self.car = car
        self.node_name = node_name
        self.cluster_name = cluster_name
        self.node_ip = ip
        self.http_port = http_port
        self.node_root_dir = node_root_dir
        self.node_log_dir = os.path.join(node_root_dir, "logs", "server")
        self.heap_dump_dir = os.path.join(node_root_dir, "heapdump")
        self.distribution_version = distribution_version
        self.rally_root = rally_root
        self.binary_path = os.path.join(node_root_dir, "install")
        # use a random subdirectory to isolate multiple runs because an external (non-root) user cannot clean it up.
        self.data_paths = [os.path.join(node_root_dir, "data", str(uuid.uuid4()))]
        self.logger = logging.getLogger(__name__)

        provisioner_defaults = {
            "cluster_name": self.cluster_name,
            "node_name": self.node_name,
            # we bind-mount the directories below on the host to these ones.
            "install_root_path": "/usr/share/elasticsearch",
            "data_paths": ["/usr/share/elasticsearch/data"],
            "log_path": "/var/log/elasticsearch",
            "heap_dump_path": "/usr/share/elasticsearch/heapdump",
            # Docker container needs to expose service on external interfaces
            "network_host": "0.0.0.0",
            "discovery_type": "single-node",
            "http_port": str(self.http_port),
            "transport_port": str(self.http_port + 100),
            "cluster_settings": {},
        }

        self.config_vars = {}
        self.config_vars.update(self.car.variables)
        self.config_vars.update(provisioner_defaults)

    def prepare(self, binary):
        # we need to allow other users to write to these directories due to Docker.
        #
        # Although os.mkdir passes 0o777 by default, mkdir(2) uses `mode & ~umask & 0777` to determine the final flags and
        # hence we need to modify the process' umask here. For details see https://linux.die.net/man/2/mkdir.
        previous_umask = os.umask(0)
        try:
            io.ensure_dir(self.binary_path)
            io.ensure_dir(self.node_log_dir)
            io.ensure_dir(self.heap_dump_dir)
            io.ensure_dir(self.data_paths[0])
        finally:
            os.umask(previous_umask)

        mounts = {}

        for car_config_path in self.car.config_paths:
            for root, _, files in os.walk(car_config_path):
                env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))

                relative_root = root[len(car_config_path) + 1 :]
                absolute_target_root = os.path.join(self.binary_path, relative_root)
                io.ensure_dir(absolute_target_root)

                for name in files:
                    source_file = os.path.join(root, name)
                    target_file = os.path.join(absolute_target_root, name)
                    mounts[target_file] = os.path.join("/usr/share/elasticsearch", relative_root, name)
                    if plain_text(source_file):
                        self.logger.info("Reading config template file [%s] and writing to [%s].", source_file, target_file)
                        with open(target_file, mode="a", encoding="utf-8") as f:
                            f.write(_render_template(env, self.config_vars, source_file))
                    else:
                        self.logger.info("Treating [%s] as binary and copying as is to [%s].", source_file, target_file)
                        shutil.copy(source_file, target_file)

        docker_cfg = self._render_template_from_file(self.docker_vars(mounts))
        self.logger.info("Starting Docker container with configuration:\n%s", docker_cfg)

        with open(os.path.join(self.binary_path, "docker-compose.yml"), mode="w", encoding="utf-8") as f:
            f.write(docker_cfg)

        return NodeConfiguration(
            "docker",
            self.car.mandatory_var("runtime.jdk"),
            convert.to_bool(self.car.mandatory_var("runtime.jdk.bundled")),
            self.node_ip,
            self.node_name,
            self.node_root_dir,
            self.binary_path,
            self.data_paths,
        )

    def docker_vars(self, mounts):
        v = {
            "es_version": self.distribution_version,
            "docker_image": self.car.mandatory_var("docker_image"),
            "node_ip": self.node_ip,
            "http_port": self.http_port,
            "es_data_dir": self.data_paths[0],
            "es_log_dir": self.node_log_dir,
            "es_heap_dump_dir": self.heap_dump_dir,
            "mounts": mounts,
        }
        self._add_if_defined_for_car(v, "docker_mem_limit")
        self._add_if_defined_for_car(v, "docker_cpu_count")
        return v

    def _add_if_defined_for_car(self, variables, key):
        if key in self.car.variables:
            variables[key] = self.car.variables[key]

    def _render_template(self, loader, template_name, variables):
        try:
            env = jinja2.Environment(loader=loader)
            for k, v in variables.items():
                env.globals[k] = v
            template = env.get_template(template_name)

            return template.render()
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise exceptions.InvalidSyntax("%s in %s" % (str(e), template_name))
        except BaseException as e:
            raise exceptions.SystemSetupError("%s in %s" % (str(e), template_name))

    def _render_template_from_file(self, variables):
        compose_file = os.path.join(self.rally_root, "resources", "docker-compose.yml.j2")
        return self._render_template(
            loader=jinja2.FileSystemLoader(io.dirname(compose_file)), template_name=io.basename(compose_file), variables=variables
        )
