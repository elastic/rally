import os
import logging
import configparser

import tabulate

from esrally import exceptions, PROGRAM_NAME
from esrally.utils import console, repo, io

logger = logging.getLogger("rally.team")


def list_cars(cfg):
    loader = CarLoader(team_repo(cfg))
    cars = []
    for name in loader.car_names():
        cars.append(loader.load_car(name))
    # first by type, then by name (we need to run the sort in reverse for that)
    # idiomatic way according to https://docs.python.org/3/howto/sorting.html#sort-stability-and-complex-sorts
    cars = sorted(sorted(cars, key=lambda c: c.name), key=lambda c: c.type)
    console.println("Available cars:\n")
    console.println(tabulate.tabulate([[c.name, c.type, c.description] for c in cars], headers=["Name", "Type", "Description"]))


def load_car(repo, name):
    # preserve order as we append to existing config files later during provisioning.
    all_config_paths = []
    all_variables = {}
    all_env = {}

    for n in name:
        descriptor = CarLoader(repo).load_car(n)
        for p in descriptor.config_paths:
            if p not in all_config_paths:
                all_config_paths.append(p)
        all_variables.update(descriptor.variables)
        # env needs to be merged individually, consider ES_JAVA_OPTS="-Xms1G" and ES_JAVA_OPTS="-ea".
        # We want it to be ES_JAVA_OPTS="-Xms1G -ea" in the end.
        for k, v in descriptor.env.items():
            # merge
            if k not in all_env:
                all_env[k] = v
            else:  # merge
                # assume we need to separate with a space
                all_env[k] = all_env[k] + " " + v

    if len(all_config_paths) == 0:
        raise exceptions.SystemSetupError("At least one config base is required for car %s" % name)
    return Car("+".join(name), all_config_paths, all_variables, all_env)


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
    repo_name = cfg.opts("mechanic", "repository.name")
    offline = cfg.opts("system", "offline.mode")
    remote_url = cfg.opts("teams", "%s.url" % repo_name, mandatory=False)
    root = cfg.opts("node", "root.dir")
    team_repositories = cfg.opts("mechanic", "team.repository.dir")
    teams_dir = os.path.join(root, team_repositories)

    current_team_repo = repo.RallyRepository(remote_url, teams_dir, repo_name, "teams", offline)
    if update:
        current_team_repo.update(distribution_version)
    return current_team_repo


class CarLoader:
    def __init__(self, repo):
        self.repo = repo
        self.cars_dir = os.path.join(self.repo.repo_dir, "cars")

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
        description = ""
        car_type = "car"
        if "meta" in config:
            description = config["meta"].get("description", description)
            car_type = config["meta"].get("type", car_type)

        if "config" in config and "base" in config["config"]:
            config_bases = config["config"]["base"].split(",")
            for base in config_bases:
                if base:
                    config_paths.append(os.path.join(self.cars_dir, base))

        # it's possible that some cars don't have a config base, e.g. mixins which only override variables
        if len(config_paths) == 0:
            logger.info("Car [%s] does not define any config paths. Assuming that it is used as a mixin." % name)

        variables = {}
        if "variables" in config.sections():
            for k, v in config["variables"].items():
                variables[k] = v
        env = {}
        if "env" in config.sections():
            for k, v in config["env"].items():
                env[k] = v
        return CarDescriptor(name, description, car_type, config_paths, variables, env)


class CarDescriptor:
    def __init__(self, name, description, type, config_paths, variables, env):
        self.name = name
        self.description = description
        self.type = type
        self.config_paths = config_paths
        self.variables = variables
        self.env = env

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.name == other.name


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
        self.variables = variables
        self.env = env

    def __str__(self):
        return self.name


class PluginLoader:
    def __init__(self, repo):
        self.repo = repo
        self.plugins_root_path = os.path.join(self.repo.repo_dir, "plugins")

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


