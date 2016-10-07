import os
import glob
import shutil
import logging

from esrally import config, exceptions
from esrally.mechanic import car
from esrally.utils import io, versions, console

logger = logging.getLogger("rally.provisioner")


def local_provisioner(cfg):
    return Provisioner(cfg)


def no_op_provisioner():
    return NoOpProvisioner()


class Provisioner:
    """
    The provisioner prepares the runtime environment for running the benchmark. It prepares all configuration files and copies the binary
    of the benchmark candidate to the appropriate place.
    """

    def __init__(self, cfg):
        self._config = cfg
        self.preserve = self._config.opts("provisioning", "install.preserve")

    def prepare(self):
        selected_car = car.select_car(self._config)
        http_port = self._config.opts("provisioning", "node.http.port")
        self._install_binary()
        self._configure(selected_car, http_port)
        return selected_car

    def cleanup(self):
        install_dir = self._install_dir()
        if self.preserve:
            logger.info("Preserving benchmark candidate installation at [%s]." % install_dir)
            console.println("\nRally will keep the benchmark candidate including all data at [%s]." % install_dir)
            console.println("Remember to delete it when you don't need it anymore as it will take up a significant amount of disk space.")
        else:
            logger.info("Wiping benchmark candidate installation at [%s]." % install_dir)
            if os.path.exists(install_dir):
                shutil.rmtree(install_dir)
            data_paths = self._config.opts("provisioning", "datapaths", mandatory=False)
            if data_paths is not None:
                for path in data_paths:
                    if os.path.exists(path):
                        shutil.rmtree(path)

    def _install_binary(self):
        binary = self._config.opts("builder", "candidate.bin.path")
        install_dir = self._install_dir()
        logger.info("Preparing candidate locally in %s." % install_dir)
        io.ensure_dir(install_dir)
        if not self.preserve:
            console.println("Rally will wipe the benchmark candidate directory [%s] after the benchmark.\n" % install_dir)

        logger.info("Unzipping %s to %s" % (binary, install_dir))
        io.decompress(binary, install_dir)
        binary_path = glob.glob("%s/elasticsearch*" % install_dir)[0]
        self._config.add(config.Scope.benchmark, "provisioning", "local.binary.path", binary_path)

    def _configure(self, car, http_port):
        self._configure_logging(car)
        self._configure_cluster(car, http_port)

    def _configure_logging(self, car):
        log_cfg = car.custom_logging_config
        if log_cfg:
            log_config_type, log_config_path = self._es_log_config()
            logger.info("Replacing pre-bundled ES log configuration at [%s] with custom config: [%s]" %
                        (log_config_path, log_cfg[log_config_type]))
            with open(log_config_path, "w") as log_config:
                log_config.write(log_cfg[log_config_type])

    def _es_log_config(self):
        binary_path = self._config.opts("provisioning", "local.binary.path")
        logging_yml_path = "%s/config/logging.yml" % binary_path
        log4j2_properties_path = "%s/config/log4j2.properties" % binary_path

        if os.path.isfile(logging_yml_path):
            return "logging.yml", logging_yml_path
        elif os.path.isfile(log4j2_properties_path):
            return "log4j2.properties", log4j2_properties_path
        else:
            raise exceptions.SystemSetupError("Unrecognized Elasticsearch log config file format")

    def _configure_cluster(self, car, http_port):
        binary_path = self._config.opts("provisioning", "local.binary.path")
        logger.info("Using port [%d]" % http_port)
        env_name = self._config.opts("system", "env.name")
        additional_config = car.custom_config_snippet
        data_paths = self._data_paths(car)
        logger.info("Using data paths [%s]" % data_paths)
        self._config.add(config.Scope.challenge, "provisioning", "local.data.paths", data_paths)
        s = open("%s/config/elasticsearch.yml" % binary_path, "r").read()
        s += "\ncluster.name: %s\n" % "benchmark.%s" % env_name
        s += self.number_of_nodes(car)
        s += "\npath.data: %s" % ", ".join(data_paths)
        s += "\nhttp.port: %d-%d" % (http_port, http_port + 100)
        s += "\ntransport.tcp.port: %d-%d" % (http_port + 100, http_port + 200)
        if additional_config:
            s += "\n%s" % additional_config
        open("%s/config/elasticsearch.yml" % binary_path, "w").write(s)

    def number_of_nodes(self, car):
        distribution_version = self._config.opts("source", "distribution.version", mandatory=False)
        configure = False
        if versions.is_version_identifier(distribution_version):
            major_version = int(versions.components(distribution_version)["major"])
            if major_version >= 2:
                configure = True
        else:
            # we're very likely benchmarking from sources which is ES 5+
            configure = True
        return "\nnode.max_local_storage_nodes: %d" % car.nodes if configure else ""

    def _data_paths(self, car):
        binary_path = self._config.opts("provisioning", "local.binary.path")
        data_paths = self._config.opts("provisioning", "datapaths")
        if data_paths is None:
            return ["%s/data" % binary_path]
        else:
            # we have to add the car name here as we need to preserve data potentially across runs
            return ["%s/%s" % (path, car.name) for path in data_paths]

    def _install_dir(self):
        root = self._config.opts("system", "challenge.root.dir")
        install = self._config.opts("provisioning", "local.install.dir")
        return "%s/%s" % (root, install)


class NoOpProvisioner:
    def prepare(self):
        pass

    def cleanup(self):
        pass