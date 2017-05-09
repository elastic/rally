import os
import glob
import shutil
import logging

import jinja2

from esrally import exceptions
from esrally.mechanic import car
from esrally.utils import io, versions, console, convert, sysstats

logger = logging.getLogger("rally.provisioner")


def local_provisioner(cfg, cluster_settings, install_dir, single_machine):
    return Provisioner(cfg, cluster_settings, install_dir, single_machine)


def no_op_provisioner(cfg):
    return NoOpProvisioner(cfg.opts("mechanic", "car.name"))


def docker_provisioner(cfg, cluster_settings, install_dir):
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    http_port = cfg.opts("provisioning", "node.http.port")
    rally_root = cfg.opts("node", "rally.root")
    return DockerProvisioner(cfg.opts("mechanic", "car.name"), cluster_settings, http_port, install_dir, distribution_version, rally_root)


class Provisioner:
    """
    The provisioner prepares the runtime environment for running the benchmark. It prepares all configuration files and copies the binary
    of the benchmark candidate to the appropriate place.
    """

    def __init__(self, cfg, cluster_settings, install_dir, single_machine):
        self._config = cfg
        self._cluster_settings = cluster_settings
        self.preserve = self._config.opts("mechanic", "preserve.install")
        car_name = self._config.opts("mechanic", "car.name")
        self.car = car.select_car(car_name)
        self.http_port = self._config.opts("provisioning", "node.http.port")
        self.data_root_paths = self._config.opts("mechanic", "node.datapaths")
        self.data_paths = None
        self.binary_path = None
        self.install_dir = install_dir
        self.single_machine = single_machine

    def prepare(self, binary):
        self._install_binary(binary)
        self._configure()

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
        self._configure_logging()
        self._configure_node()

    def _configure_logging(self):
        log_cfg = self.car.custom_logging_config
        if log_cfg:
            log_config_type, log_config_path = self._es_log_config()
            logger.info("Replacing pre-bundled ES log configuration at [%s] with custom config: [%s]" %
                        (log_config_path, log_cfg[log_config_type]))
            with open(log_config_path, "w") as log_config:
                log_config.write(log_cfg[log_config_type])

    def _es_log_config(self):
        logging_yml_path = "%s/config/logging.yml" % self.binary_path
        log4j2_properties_path = "%s/config/log4j2.properties" % self.binary_path

        if os.path.isfile(logging_yml_path):
            return "logging.yml", logging_yml_path
        elif os.path.isfile(log4j2_properties_path):
            distribution_version = self._config.opts("mechanic", "distribution.version", mandatory=False)
            if versions.is_version_identifier(distribution_version):
                if versions.major_version(distribution_version) == 5:
                    return "log4j2.properties.5", log4j2_properties_path
            else:
                return "log4j2.properties", log4j2_properties_path
        else:
            raise exceptions.SystemSetupError("Unrecognized Elasticsearch log config file format")

    def _configure_node(self):
        node_cfg = self._node_configuration(open("%s/config/elasticsearch.yml" % self.binary_path, "r").read())
        open("%s/config/elasticsearch.yml" % self.binary_path, "w").write(node_cfg)

    def _node_configuration(self, initial_config=""):
        logger.info("Using port [%d]" % self.http_port)
        additional_config = self.car.custom_config_snippet
        self.data_paths = self._data_paths()
        logger.info("Using data paths [%s]" % self.data_paths)
        s = initial_config
        s += "\ncluster.name: rally-benchmark"
        s += self.number_of_nodes()
        if self.single_machine:
            logger.info("Binding node to 127.0.0.1 (single-machine benchmark).")
        else:
            logger.info("Binding node to 0.0.0.0 (multi-machine benchmark).")
            s += "\nnetwork.host: 0.0.0.0"
        s += "\npath.data: %s" % ", ".join(self.data_paths)
        s += "\nhttp.port: %d-%d" % (self.http_port, self.http_port + 100)
        s += "\ntransport.tcp.port: %d-%d" % (self.http_port + 100, self.http_port + 200)
        if additional_config:
            s += "\n%s" % additional_config
        if self._cluster_settings:
            for k, v in self._cluster_settings.items():
                s += "\n%s: %s" % (str(k), str(v))
        return s

    def number_of_nodes(self):
        distribution_version = self._config.opts("mechanic", "distribution.version", mandatory=False)
        configure = False
        if versions.is_version_identifier(distribution_version):
            major = versions.major_version(distribution_version)
            if major >= 2:
                configure = True
        else:
            # we're very likely benchmarking from sources which is ES 5+
            configure = True
        return "\nnode.max_local_storage_nodes: %d" % self.car.nodes if configure else ""

    def _data_paths(self):
        if self.data_root_paths is None:
            return ["%s/data" % self.binary_path]
        else:
            return ["%s/data" % path for path in self.data_root_paths]


class NoOpProvisioner:
    def __init__(self, car_name):
        try:
            self.car = car.select_car(car_name)
        except exceptions.SystemSetupError:
            self.car = None
        self.binary_path = None
        self.data_paths = None

    def prepare(self, binary):
        return self.car

    def cleanup(self):
        pass


class DockerProvisioner:
    def __init__(self, car_name, cluster_settings, http_port, install_dir, distribution_version, rally_root):
        self.car = car.select_car(car_name)
        self._cluster_settings = cluster_settings
        self.http_port = http_port
        self.install_dir = install_dir
        self.distribution_version = distribution_version
        self.rally_root = rally_root
        self.binary_path = "%s/docker-compose.yml" % self.install_dir
        self.data_paths = [self.install_dir]

    def prepare(self, binary):
        io.ensure_dir(self.install_dir)

        docker_cfg = self._render_template_from_file(self.docker_vars)
        logger.info("Starting Docker container with configuration:\n%s" % docker_cfg)

        with open(self.binary_path, "wt") as f:
            f.write(docker_cfg)

        return self.car

    @property
    def docker_vars(self):
        java_opts = ""
        if self.car.heap:
            java_opts += "-Xms%s -Xmx%s " % (self.car.heap, self.car.heap)
        if self.car.java_opts:
            java_opts += self.car.java_opts

        return {
            "es_java_opts": java_opts,
            "container_memory_gb": "%dg" % (convert.bytes_to_gb(sysstats.total_memory()) // 2),
            "es_data_dir": "%s/data" % self.install_dir,
            "es_version": self.distribution_version,
            "http_port": self.http_port,
            "cluster_settings": self._cluster_settings
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
