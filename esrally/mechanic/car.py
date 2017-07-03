import os
import sys
import logging
import configparser

import tabulate

from esrally import exceptions, PROGRAM_NAME
from esrally.utils import console, git, versions, io

logger = logging.getLogger("rally.car")


def list_cars(cfg):
    console.println("Available cars:\n")
    console.println(tabulate.tabulate([[str(c)] for c in cars(cfg)], headers=["Name"]))


def load_car(cfg, name):
    repo = TeamRepository(cfg)
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    return repo.load_car(distribution_version, name)


def cars(cfg):
    repo = TeamRepository(cfg)
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    return repo.car_names(distribution_version)


class TeamRepository:
    """
    Manages teams (consisting of cars).
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
        self.cars_dir = os.path.join(self.teams_dir, "cars")
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

    def car_names(self, distribution_version):
        def __car_name(path):
            p, _ = io.splitext(path)
            return io.basename(p)

        def __is_car(path):
            _, extension = io.splitext(path)
            return extension == ".ini"

        self._update(distribution_version)
        return map(__car_name, filter(__is_car, os.listdir(self.cars_dir)))

    def _car_file(self, name):
        return os.path.join(self.cars_dir, "%s.ini" % name)

    def load_car(self, distribution_version, name, needs_update=True):
        if needs_update:
            self._update(distribution_version)

        car_config_file = self._car_file(name)
        if not io.exists(car_config_file):
            raise exceptions.SystemSetupError("Unknown car [%s]. List the available cars with %s list cars." % (name, PROGRAM_NAME))
        config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        # Do not modify the case of option keys but read them as is
        config.optionxform = lambda option: option
        config.read(car_config_file)
        config_bases = config["config"]["base"].split(",")
        config_paths = []
        for base in config_bases:
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

    def _update(self, distribution_version):
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
                raise exceptions.SystemSetupError("Cannot find track data for distribution version %s" % distribution_version)
        except exceptions.SupplyError:
            tb = sys.exc_info()[2]
            raise exceptions.DataError("Cannot update team data in [%s]." % self.teams_dir).with_traceback(tb)


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
