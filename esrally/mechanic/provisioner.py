import os
import glob
import shutil
import logging

import jinja2

from esrally.mechanic import car
from esrally.utils import io, console

logger = logging.getLogger("rally.provisioner")


def local_provisioner(cfg, cluster_settings, install_dir, node_log_dir, single_machine):
    return Provisioner(cfg, cluster_settings, install_dir, node_log_dir, single_machine)


def no_op_provisioner(cfg):
    return NoOpProvisioner(cfg.opts("mechanic", "car.name"))


def docker_provisioner(cfg, cluster_settings, install_dir, node_log_dir):
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    http_port = cfg.opts("provisioning", "node.http.port")
    rally_root = cfg.opts("node", "rally.root")
    node_name_prefix = cfg.opts("provisioning", "node.name.prefix")

    c = car.load_car(cfg, cfg.opts("mechanic", "car.name"))

    return DockerProvisioner(c, node_name_prefix, cluster_settings, http_port, install_dir, node_log_dir, distribution_version, rally_root)


class NodeConfiguration:
    def __init__(self, car, node_name, binary_path, data_paths):
        self.car = car
        self.node_name = node_name
        self.binary_path = binary_path
        self.data_paths = data_paths


class ConfigLoader:
    def __init__(self):
        pass

    def load(self):
        pass


def _render_template(env, variables, file_name):
    template = env.get_template(io.basename(file_name))
    return template.render(variables)


class Provisioner:
    """
    The provisioner prepares the runtime environment for running the benchmark. It prepares all configuration files and copies the binary
    of the benchmark candidate to the appropriate place.
    """

    def __init__(self, cfg, cluster_settings, install_dir, node_log_dir, single_machine):
        self._config = cfg
        self._cluster_settings = cluster_settings
        self.preserve = self._config.opts("mechanic", "preserve.install")
        self.car = None
        self.car_name = self._config.opts("mechanic", "car.name")
        self.http_port = self._config.opts("provisioning", "node.http.port")
        self.data_root_paths = self._config.opts("mechanic", "node.datapaths")
        self.node_name_prefix = self._config.opts("provisioning", "node.name.prefix")
        self.node_log_dir = node_log_dir
        self.data_paths = None
        self.binary_path = None
        self.install_dir = install_dir
        self.single_machine = single_machine

    def prepare(self, binary):
        self._install_binary(binary)
        return self._configure()

    def cleanup(self):
        if self.preserve:
            logger.info("Preserving benchmark candidate installation at [%s]." % self.install_dir)
            console.info("Keeping benchmark candidate including index at [%s] (will need several GB)." % self.install_dir)
        else:
            logger.info("Wiping benchmark candidate installation at [%s]." % self.install_dir)
            for path in self.data_paths:
                if os.path.exists(path):
                    shutil.rmtree(path)

            if os.path.exists(self.install_dir):
                shutil.rmtree(self.install_dir)

    def _install_binary(self, binary):
        logger.info("Preparing candidate locally in [%s]." % self.install_dir)
        io.ensure_dir(self.install_dir)
        if not self.preserve:
            console.info("Rally will delete the benchmark candidate after the benchmark")

        logger.info("Unzipping %s to %s" % (binary, self.install_dir))
        io.decompress(binary, self.install_dir)
        self.binary_path = glob.glob("%s/elasticsearch*" % self.install_dir)[0]

    def _configure(self):
        config_path = os.path.join(self.binary_path, "config")
        logger.info("Deleting pre-bundled Elasticsearch configuration at [%s]" % config_path)
        shutil.rmtree(config_path)

        self.car = car.load_car(self._config, self.car_name)

        self.data_paths = self._data_paths()
        network_host = "127.0.0.1" if self.single_machine else "0.0.0.0"

        # TODO #196: For now we assume that there is only one node. Provide this as parameter already (preparation for #71)
        node_name = self._node_name(0)

        provisioner_defaults = {
            "cluster_name": "rally-benchmark",
            "node_name": node_name,
            "data_paths": self.data_paths,
            "log_path": self.node_log_dir,
            "network_host": network_host,
            "http_port": "%d-%d" % (self.http_port, self.http_port + 100),
            "transport_port": "%d-%d" % (self.http_port + 100, self.http_port + 200),
            # TODO dm: At the moment we will not allow multiple nodes per host - may change later again (the "problem" is that we need
            # to change the structure here: one provisioner per node, one launcher per node and we're not there yet)
            "node_count_per_host": 1,
            # Merge cluster config from the track. These may not be dynamically updateable so we need to define them in the config file.
            "cluster_settings": self._cluster_settings
        }

        variables = {}
        variables.update(self.car.variables)
        variables.update(provisioner_defaults)

        car_config_path = self.car.config_path
        for root, dirs, files in os.walk(car_config_path):
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))

            relative_root = root[len(car_config_path) + 1:]
            absolute_target_root = os.path.join(self.binary_path, relative_root)
            io.ensure_dir(absolute_target_root)

            for name in files:
                source_file = os.path.join(root, name)
                target_file = os.path.join(absolute_target_root, name)
                logger.info("Writing config file [%s]" % target_file)
                with open(target_file, "w") as f:
                    f.write(_render_template(env, variables, source_file))

        return NodeConfiguration(self.car, node_name, self.binary_path, self.data_paths)

    def _node_name(self, node):
        return "%s%d" % (self.node_name_prefix, node)

    def _data_paths(self):
        if self.data_root_paths is None:
            return ["%s/data" % self.binary_path]
        else:
            return ["%s/data" % path for path in self.data_root_paths]


class NoOpProvisioner:
    def __init__(self, *args):
        pass

    def prepare(self, *args):
        return None

    def cleanup(self):
        pass


class DockerProvisioner:
    def __init__(self, car, node_name_prefix, cluster_settings, http_port, install_dir, node_log_dir, distribution_version, rally_root):
        self.car = car
        self._cluster_settings = cluster_settings
        self.http_port = http_port
        self.install_dir = install_dir
        self.node_log_dir = node_log_dir
        self.distribution_version = distribution_version
        self.rally_root = rally_root
        self.binary_path = "%s/docker-compose.yml" % self.install_dir
        self.data_path = "%s/data" % self.install_dir
        self.data_paths = [self.data_path]
        self.node_name = "%s0" % node_name_prefix

    def prepare(self, binary):
        io.ensure_dir(self.install_dir)
        node_name = "rally-docker"
        provisioner_defaults = {
            "cluster_name": "rally-benchmark",
            "node_name": node_name,
            # we bind-mount the directories below on the host to these ones.
            "data_paths": ["/usr/share/elasticsearch/data"],
            "log_path": "/var/log/elasticsearch",
            # Docker container needs to expose service on external interfaces
            "network_host": "0.0.0.0",
            "http_port": "%d-%d" % (self.http_port, self.http_port + 100),
            "transport_port": "%d-%d" % (self.http_port + 100, self.http_port + 200),
            "node_count_per_host": 1,
            # Merge cluster config from the track. These may not be dynamically updateable so we need to define them in the config file.
            "cluster_settings": self._cluster_settings
        }

        variables = {}
        variables.update(self.car.variables)
        variables.update(provisioner_defaults)

        mounts = {}

        car_config_path = self.car.config_path
        for root, dirs, files in os.walk(car_config_path):
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))

            relative_root = root[len(car_config_path) + 1:]
            absolute_target_root = os.path.join(self.install_dir, relative_root)
            io.ensure_dir(absolute_target_root)

            for name in files:
                source_file = os.path.join(root, name)
                target_file = os.path.join(absolute_target_root, name)
                mounts[target_file] = os.path.join("/usr/share/elasticsearch", relative_root, name)
                logger.info("Writing config file [%s]" % target_file)
                with open(target_file, "w") as f:
                    f.write(_render_template(env, variables, source_file))

        docker_cfg = self._render_template_from_file(self.docker_vars(mounts))
        logger.info("Starting Docker container with configuration:\n%s" % docker_cfg)

        with open(self.binary_path, "wt") as f:
            f.write(docker_cfg)

        return NodeConfiguration(self.car, self.node_name, self.binary_path, self.data_paths)

    def docker_vars(self, mounts):
        return {
            "es_version": self.distribution_version,
            "http_port": self.http_port,
            "es_data_dir": self.data_path,
            "es_log_dir": self.node_log_dir,
            "mounts": mounts
        }

    def cleanup(self):
        # TODO dm: We can remove the compose file and the data paths here (just as the normal provisioner does)
        pass

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
