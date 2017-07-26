import os
import sys
import logging
import configparser

import tabulate

from esrally import exceptions, PROGRAM_NAME
from esrally.utils import console, git, versions, io

logger = logging.getLogger("rally.team")


def list_cars(cfg):
    cars = CarLoader(team_repo(cfg)).car_names()
    console.println("Available cars:\n")
    console.println(tabulate.tabulate([[str(c)] for c in cars], headers=["Name"]))


def load_car(repo, name):
    return CarLoader(repo).load_car(name)


def list_plugins(cfg):
    plugins = PluginLoader(team_repo(cfg)).plugins()
    if plugins:
        console.println("Available Elasticsearch plugins:\n")
        console.println(tabulate.tabulate([[p.name, p.config] for p in plugins], headers=["Name", "Configuration"]))
    else:
        console.println("No Elasticsearch plugins are available.\n")


def load_plugin(repo, name, config):
    if config is not None:
        logger.info("Loading plugin [%s] with configuration(s) [%s]." % (name, config))
    else:
        logger.info("Loading plugin [%s] with default configuration." % name)
    return PluginLoader(repo).load_plugin(name, config)


def load_plugins(repo, plugin_names):
    def name_and_config(p):
        plugin_spec = p.split(":")
        if len(plugin_spec) == 1:
            return plugin_spec[0], None
        elif len(plugin_spec) == 2:
            return plugin_spec[0], plugin_spec[1].split("+")
        else:
            raise ValueError("Unrecognized plugin specification [%s]. Use either 'PLUGIN_NAME' or 'PLUGIN_NAME:PLUGIN_CONFIG'." % plugin)

    plugins = []
    for plugin in plugin_names:
        plugin_name, plugin_config = name_and_config(plugin)
        plugins.append(load_plugin(repo, plugin_name, plugin_config))
    return plugins


def team_repo(cfg, update=True):
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    repo = TeamRepository(cfg)
    if update:
        repo.update(distribution_version)
    return repo


# TODO #308: This is now generic enough to be merged with the track repo.
class TeamRepository:
    """
    Manages teams (consisting of cars and their plugins).
    """

    def __init__(self, cfg, fetch=True):
        self.cfg = cfg
        self.name = cfg.opts("mechanic", "repository.name")
        self.offline = cfg.opts("system", "offline.mode")
        # If no URL is found, we consider this a local only repo (but still require that it is a git repo)
        self.url = cfg.opts("teams", "%s.url" % self.name, mandatory=False)
        self.remote = self.url is not None and self.url.strip() != ""
        root = cfg.opts("node", "root.dir")
        team_repositories = cfg.opts("mechanic", "team.repository.dir")
        self.teams_dir = os.path.join(root, team_repositories, self.name)
        if self.remote and not self.offline and fetch:
            # a normal git repo with a remote
            if not git.is_working_copy(self.teams_dir):
                git.clone(src=self.teams_dir, remote=self.url)
            else:
                try:
                    git.fetch(src=self.teams_dir)
                except exceptions.SupplyError:
                    console.warn("Could not update teams. Continuing with your locally available state.", logger=logger)
        else:
            if not git.is_working_copy(self.teams_dir):
                raise exceptions.SystemSetupError("[{src}] must be a git repository.\n\nPlease run:\ngit -C {src} init"
                                                  .format(src=self.teams_dir))

    def update(self, distribution_version):
        try:
            if self.remote and not self.offline:
                branch = versions.best_match(git.branches(self.teams_dir, remote=self.remote), distribution_version)
                if branch:
                    # Allow uncommitted changes iff we do not have to change the branch
                    logger.info(
                        "Checking out [%s] in [%s] for distribution version [%s]." % (branch, self.teams_dir, distribution_version))
                    git.checkout(self.teams_dir, branch=branch)
                    logger.info("Rebasing on [%s] in [%s] for distribution version [%s]." % (branch, self.teams_dir, distribution_version))
                    try:
                        git.rebase(self.teams_dir, branch=branch)
                    except exceptions.SupplyError:
                        logger.exception("Cannot rebase due to local changes in [%s]" % self.teams_dir)
                        console.warn(
                            "Local changes in [%s] prevent team update from remote. Please commit your changes." % self.teams_dir)
                    return
                else:
                    msg = "Could not find team data remotely for distribution version [%s]. " \
                          "Trying to find team data locally." % distribution_version
                    logger.warning(msg)
            branch = versions.best_match(git.branches(self.teams_dir, remote=False), distribution_version)
            if branch:
                logger.info("Checking out [%s] in [%s] for distribution version [%s]." % (branch, self.teams_dir, distribution_version))
                git.checkout(self.teams_dir, branch=branch)
            else:
                raise exceptions.SystemSetupError("Cannot find team data for distribution version %s" % distribution_version)
        except exceptions.SupplyError:
            tb = sys.exc_info()[2]
            raise exceptions.DataError("Cannot update team data in [%s]." % self.teams_dir).with_traceback(tb)


class CarLoader:
    def __init__(self, repo):
        self.repo = repo
        self.cars_dir = os.path.join(self.repo.teams_dir, "cars")

    def car_names(self):
        def __car_name(path):
            p, _ = io.splitext(path)
            return io.basename(p)

        def __is_car(path):
            _, extension = io.splitext(path)
            return extension == ".ini"
        return map(__car_name, filter(__is_car, os.listdir(self.cars_dir)))

    def _car_file(self, name):
        return os.path.join(self.cars_dir, "%s.ini" % name)

    def load_car(self, name):
        car_config_file = self._car_file(name)
        if not io.exists(car_config_file):
            raise exceptions.SystemSetupError("Unknown car [%s]. List the available cars with %s list cars." % (name, PROGRAM_NAME))
        config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        # Do not modify the case of option keys but read them as is
        config.optionxform = lambda option: option
        config.read(car_config_file)
        config_paths = []
        if "config" in config and "base" in config["config"]:
            config_bases = config["config"]["base"].split(",")
            for base in config_bases:
                if base:
                    config_paths.append(os.path.join(self.cars_dir, base))

        if len(config_paths) == 0:
            raise exceptions.SystemSetupError("At least one config base is required for car [%s]" % name)

        variables = {}
        if "variables" in config.sections():
            for k, v in config["variables"].items():
                variables[k] = v
        env = {}
        if "env" in config.sections():
            for k, v in config["env"].items():
                env[k] = v
        return Car(name, config_paths, variables, env)


class Car:
    def __init__(self, name, config_paths, variables=None, env=None):
        """
        Creates new settings for a benchmark candidate.

        :param name: A descriptive name for this car.
        :param config_paths: A non-empty list of paths where the raw config can be found.
        :param variables: A dict containing variable definitions that need to be replaced.
        :param env: Environment variables that should be set when launching the benchmark candidate.
        """
        if env is None:
            env = {}
        if variables is None:
            variables = {}
        self.name = name
        self.config_paths = config_paths
        # for convenience as long as we do not allow more complex setups, e.g. with plugins
        self.config_path = self.config_paths[0]
        self.variables = variables
        self.env = env
        # for backwards-compatibility - but we allow only one node at the moment
        self.nodes = 1

    def __str__(self):
        return self.name


class PluginLoader:
    def __init__(self, repo):
        self.repo = repo
        self.plugins_root_path = os.path.join(self.repo.teams_dir, "plugins")

    def plugins(self):
        known_plugins = self._official_plugins() + self._configured_plugins()
        sorted(known_plugins, key=lambda p: p.name)
        return known_plugins

    def _official_plugins(self):
        official_plugins = []
        official_plugins_path = os.path.join(self.plugins_root_path, "official-plugins.txt")
        if os.path.exists(official_plugins_path):
            with open(official_plugins_path, "rt") as f:
                for line in f:
                    if not line.startswith("#"):
                        official_plugins.append(PluginDescriptor(line.strip()))
        return official_plugins

    def _configured_plugins(self):
        configured_plugins = []
        # each directory is a plugin, each .ini is a config (just go one level deep)
        for entry in os.listdir(self.plugins_root_path):
            plugin_path = os.path.join(self.plugins_root_path, entry)
            if os.path.isdir(plugin_path):
                for child_entry in os.listdir(plugin_path):
                    if os.path.isfile(os.path.join(plugin_path, child_entry)) and io.has_extension(child_entry, ".ini"):
                        f, _ = io.splitext(child_entry)
                        plugin_name = self._file_to_plugin_name(entry)
                        config = io.basename(f)
                        configured_plugins.append(PluginDescriptor(plugin_name, config))
        return configured_plugins

    def _plugin_file(self, name, config):
        return os.path.join(self._plugin_root_path(name), "%s.ini" % config)

    def _plugin_root_path(self, name):
        return os.path.join(self.plugins_root_path, self._plugin_name_to_file(name))

    # As we allow to store Python files in the plugin directory and the plugin directory also serves as the root path of the corresponding
    # module, we need to adhere to the Python restrictions here. For us, this is that hyphens in module names are not allowed. Hence, we
    # need to switch from underscores to hyphens and vice versa.
    #
    # We are implicitly assuming that plugin names stick to the convention of hyphen separation to simplify implementation and usage a bit.
    def _file_to_plugin_name(self, file_name):
        return file_name.replace("_", "-")

    def _plugin_name_to_file(self, plugin_name):
        return plugin_name.replace("-", "_")

    def _official_plugin(self, name):
        return next((p for p in self._official_plugins() if p.name == name and p.config is None), None)

    def load_plugin(self, name, config_names):
        root_path = self._plugin_root_path(name)
        if not config_names:
            # maybe we only have a config folder but nothing else (e.g. if there is only an install hook)
            if io.exists(root_path):
                return PluginDescriptor(name, config_names, root_path)
            else:
                official_plugin = self._official_plugin(name)
                if official_plugin:
                    return official_plugin
                # If we just have a plugin name then we assume that this is a community plugin and the user has specified a download URL
                else:
                    logger.info("The plugin [%s] is neither a configured nor an official plugin. Assuming that this is a community "
                                "plugin not requiring any configuration and you have set a proper download URL." % name)
                    return PluginDescriptor(name)
        else:
            variables = {}
            config_paths = []
            # used for deduplication
            known_config_bases = set()

            for config_name in config_names:
                config_file = self._plugin_file(name, config_name)
                # Do we have an explicit configuration for this plugin?
                if not io.exists(config_file):
                    official_plugin = self._official_plugin(name)
                    if official_plugin:
                        raise exceptions.SystemSetupError("Plugin [%s] does not provide configuration [%s]. List the available plugins "
                                                          "and configurations with %s list elasticsearch-plugins "
                                                          "--distribution-version=VERSION." % (name, config_name, PROGRAM_NAME))
                    else:
                        raise exceptions.SystemSetupError("Unknown plugin [%s]. List the available plugins with %s list "
                                                          "elasticsearch-plugins --distribution-version=VERSION." % (name, PROGRAM_NAME))

                config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
                # Do not modify the case of option keys but read them as is
                config.optionxform = lambda option: option
                config.read(config_file)
                if "config" in config and "base" in config["config"]:
                    config_bases = config["config"]["base"].split(",")
                    for base in config_bases:
                        if base and base not in known_config_bases:
                            config_paths.append(os.path.join(root_path, base))
                        known_config_bases.add(base)

                if "variables" in config.sections():
                    for k, v in config["variables"].items():
                        variables[k] = v

            # maybe one of the configs is really just for providing variables. However, we still require one config base overall.
            if len(config_paths) == 0:
                raise exceptions.SystemSetupError("At least one config base is required for plugin [%s]" % name)
            return PluginDescriptor(name, config_names, root_path, config_paths, variables)


class PluginDescriptor:
    def __init__(self, name, config=None, root_path=None, config_paths=None, variables=None):
        if config_paths is None:
            config_paths = []
        if variables is None:
            variables = {}
        self.name = name
        self.config = config
        self.root_path = root_path
        self.config_paths = config_paths
        self.variables = variables

    def __str__(self):
        return "Plugin descriptor for [%s]" % self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.name) ^ hash(self.config)

    def __eq__(self, other):
        return isinstance(other, type(self)) and (self.name, self.config) == (other.name, other.config)


