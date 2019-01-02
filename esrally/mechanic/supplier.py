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

import os
import re
import glob
import logging
import urllib.error

from esrally import exceptions, PROGRAM_NAME
from esrally.utils import git, io, process, net, jvm, convert
from esrally.exceptions import BuildError, SystemSetupError

# e.g. my-plugin:current - we cannot simply use String#split(":") as this would not work for timestamp-based revisions
REVISION_PATTERN = r"(\w.*?):(.*)"


def create(cfg, sources, distribution, build, challenge_root_path, car, plugins=None):
    logger = logging.getLogger(__name__)
    if plugins is None:
        plugins = []
    revisions = _extract_revisions(cfg.opts("mechanic", "source.revision"))
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    supply_requirements = _supply_requirements(sources, distribution, build, plugins, revisions, distribution_version)
    build_needed = any([build for _, _, build in supply_requirements.values()])
    src_config = cfg.all_opts("source")
    suppliers = []

    if build_needed:
        java_home = _java_home(car)
        es_src_dir = os.path.join(_src_dir(cfg), _config_value(src_config, "elasticsearch.src.subdir"))
        builder = Builder(es_src_dir, java_home, challenge_root_path)
    else:
        builder = None

    es_supplier_type, es_version, es_build = supply_requirements["elasticsearch"]
    if es_supplier_type == "source":
        es_src_dir = os.path.join(_src_dir(cfg), _config_value(src_config, "elasticsearch.src.subdir"))
        suppliers.append(
            ElasticsearchSourceSupplier(es_version, es_src_dir, remote_url=cfg.opts("source", "remote.repo.url"), car=car, builder=builder))
        repo = None
    else:
        es_src_dir = None
        distributions_root = os.path.join(cfg.opts("node", "root.dir"), cfg.opts("source", "distribution.dir"))

        dist_cfg = {}
        # car / plugin defines defaults...
        dist_cfg.update(car.variables)
        for plugin in plugins:
            for k, v in plugin.variables.items():
                dist_cfg["plugin_{}_{}".format(plugin.name, k)] = v
        # ... but the user can override it in rally.ini
        dist_cfg.update(cfg.all_opts("distributions"))
        repo = DistributionRepository(name=cfg.opts("mechanic", "distribution.repository"),
                                      distribution_config=dist_cfg,
                                      version=es_version)
        suppliers.append(ElasticsearchDistributionSupplier(repo, distributions_root))

    for plugin in plugins:
        supplier_type, plugin_version, build_plugin = supply_requirements[plugin.name]

        if supplier_type == "source":
            if CorePluginSourceSupplier.can_handle(plugin):
                logger.info("Adding core plugin source supplier for [%s].", plugin.name)
                assert es_src_dir is not None, "Cannot build core plugin %s when Elasticsearch is not built from source." % plugin.name
                suppliers.append(CorePluginSourceSupplier(plugin, es_src_dir, builder))
            elif ExternalPluginSourceSupplier.can_handle(plugin):
                logger.info("Adding external plugin source supplier for [%s].", plugin.name)
                suppliers.append(ExternalPluginSourceSupplier(plugin, plugin_version, _src_dir(cfg, mandatory=False), src_config, builder))
            else:
                raise exceptions.RallyError("Plugin %s can neither be treated as core nor as external plugin. Requirements: %s" %
                                            (plugin.name, supply_requirements[plugin.name]))
        else:
            logger.info("Adding plugin distribution supplier for [%s].", plugin.name)
            assert repo is not None, "Cannot benchmark plugin %s from a distribution version but Elasticsearch from sources" % plugin.name
            suppliers.append(PluginDistributionSupplier(repo, plugin))

    return CompositeSupplier(suppliers)


def _java_home(car):
    build_jdk = car.mandatory_var("build.jdk")
    try:
        _, path = jvm.resolve_path(int(build_jdk))
        return path
    except ValueError:
        raise exceptions.SystemSetupError("Car config key \"build.jdk\" is invalid: \"{}\" (must be int)".format(build_jdk))


def _required_version(version):
    if not version or version.strip() == "":
        raise exceptions.SystemSetupError("Could not determine version. Please specify the Elasticsearch distribution "
                                          "to download with the command line parameter --distribution-version. "
                                          "E.g. --distribution-version=5.0.0")
    else:
        return version


def _required_revision(revisions, key, name=None):
    try:
        return revisions[key]
    except KeyError:
        n = name if name is not None else key
        raise exceptions.SystemSetupError("No revision specified for %s" % n)


def _supply_requirements(sources, distribution, build, plugins, revisions, distribution_version):
    # per artifact (elasticsearch or a specific plugin):
    #   * key: artifact
    #   * value: ("source" | "distribution", distribution_version | revision, build = True | False)
    supply_requirements = {}

    # can only build Elasticsearch with source-related pipelines -> ignore revision in that case
    if "elasticsearch" in revisions and sources:
        supply_requirements["elasticsearch"] = ("source", _required_revision(revisions, "elasticsearch", "Elasticsearch"), build)
    else:
        # no revision given or explicitly specified that it's from a distribution -> must use a distribution
        supply_requirements["elasticsearch"] = ("distribution", _required_version(distribution_version), False)

    for plugin in plugins:
        if plugin.core_plugin:
            # core plugins are entirely dependent upon Elasticsearch.
            supply_requirements[plugin.name] = supply_requirements["elasticsearch"]
        else:
            # allow catch-all only if we're generally building from sources. If it is mixed, the user should tell explicitly.
            if plugin.name in revisions or ("all" in revisions and sources):
                # this plugin always needs to built unless we explicitly disable it; we cannot solely rely on the Rally pipeline.
                # We either have:
                #
                # * --pipeline=from-sources-skip-build --distribution-version=X.Y.Z where the plugin should not be built but ES should be
                #   a distributed version.
                # * --distribution-version=X.Y.Z --revision="my-plugin:abcdef" where the plugin should be built from sources.
                plugin_needs_build = (sources and build) or distribution
                # be a bit more lenient when checking for plugin revisions. This allows users to specify `--revision="current"` and
                # rely on Rally to do the right thing.
                try:
                    plugin_revision = revisions[plugin.name]
                except KeyError:
                    # maybe we can use the catch-all revision (only if it's not a git revision)
                    plugin_revision = revisions.get("all")
                    if not plugin_revision or SourceRepository.is_commit_hash(plugin_revision):
                        raise exceptions.SystemSetupError("No revision specified for plugin [%s]." % plugin.name)
                    else:
                        logging.getLogger(__name__).info("Revision for [%s] is not explicitly defined. Using catch-all revision [%s]."
                                    % (plugin.name, plugin_revision))
                supply_requirements[plugin.name] = ("source", plugin_revision, plugin_needs_build)
            else:
                supply_requirements[plugin.name] = (distribution, _required_version(distribution_version), False)
    return supply_requirements


def _src_dir(cfg, mandatory=True):
    # Don't let this spread across the whole module
    from esrally import config
    try:
        return cfg.opts("node", "src.root.dir", mandatory=mandatory)
    except config.ConfigError:
        raise exceptions.SystemSetupError("You cannot benchmark Elasticsearch from sources. Did you install Gradle? Please install"
                                          " all prerequisites and reconfigure Rally with %s configure" % PROGRAM_NAME)


class CompositeSupplier:
    def __init__(self, suppliers):
        self.suppliers = suppliers

    def __call__(self, *args, **kwargs):
        binaries = {}
        try:
            for supplier in self.suppliers:
                supplier.fetch()

            for supplier in self.suppliers:
                supplier.prepare()

            for supplier in self.suppliers:
                supplier.add(binaries)
            return binaries
        except BaseException:
            raise


class ElasticsearchSourceSupplier:
    def __init__(self, revision, es_src_dir, remote_url, car, builder):
        self.revision = revision
        self.src_dir = es_src_dir
        self.remote_url = remote_url
        self.car = car
        self.builder = builder

    def fetch(self):
        SourceRepository("Elasticsearch", self.remote_url, self.src_dir).fetch(self.revision)

    def prepare(self):
        if self.builder:
            self.builder.build([self.car.mandatory_var("clean_command"), self.car.mandatory_var("build_command")])

    def add(self, binaries):
        binaries["elasticsearch"] = self.resolve_binary()

    def resolve_binary(self):
        try:
            return glob.glob("{}/{}".format(self.src_dir, self.car.mandatory_var("artifact_path_pattern")))[0]
        except IndexError:
            raise SystemSetupError("Couldn't find a tar.gz distribution. Please run Rally with the pipeline 'from-sources-complete'.")


class ExternalPluginSourceSupplier:
    def __init__(self, plugin, revision, src_dir, src_config, builder):
        assert not plugin.core_plugin, "Plugin %s is a core plugin" % plugin.name
        self.plugin = plugin
        self.revision = revision
        # may be None if and only if the user has set an absolute plugin directory
        self.src_dir = src_dir
        self.src_config = src_config
        self.builder = builder
        subdir_cfg_key = "plugin.%s.src.subdir" % self.plugin.name
        dir_cfg_key = "plugin.%s.src.dir" % self.plugin.name
        if dir_cfg_key in self.src_config and subdir_cfg_key in self.src_config:
            raise exceptions.SystemSetupError("Can only specify one of %s and %s but both are set." % (dir_cfg_key, subdir_cfg_key))
        elif dir_cfg_key in self.src_config:
            self.plugin_src_dir = _config_value(self.src_config, dir_cfg_key)
            # we must build directly in the plugin dir, not relative to Elasticsearch
            self.override_build_dir = self.plugin_src_dir
        elif subdir_cfg_key in self.src_config:
            self.plugin_src_dir = os.path.join(self.src_dir, _config_value(self.src_config, subdir_cfg_key))
            self.override_build_dir = None
        else:
            raise exceptions.SystemSetupError("Neither %s nor %s are set for plugin %s." % (dir_cfg_key, subdir_cfg_key, self.plugin.name))

    @staticmethod
    def can_handle(plugin):
        return not plugin.core_plugin

    def fetch(self):
        # optional (but then source code is assumed to be available locally)
        plugin_remote_url = self.src_config.get("plugin.%s.remote.repo.url" % self.plugin.name)
        SourceRepository(self.plugin.name, plugin_remote_url, self.plugin_src_dir).fetch(self.revision)

    def prepare(self):
        if self.builder:
            command = _config_value(self.src_config, "plugin.{}.build.command".format(self.plugin.name))
            self.builder.build([command], override_src_dir=self.override_build_dir)

    def add(self, binaries):
        binaries[self.plugin.name] = self.resolve_binary()

    def resolve_binary(self):
        artifact_path = _config_value(self.src_config, "plugin.%s.build.artifact.subdir" % self.plugin.name)
        try:
            name = glob.glob("%s/%s/*.zip" % (self.plugin_src_dir, artifact_path))[0]
            return "file://%s" % name
        except IndexError:
            raise SystemSetupError("Couldn't find a plugin zip file for [%s]. Please run Rally with the pipeline 'from-sources-complete'." %
                                   self.plugin.name)


class CorePluginSourceSupplier:
    def __init__(self, plugin, es_src_dir, builder):
        assert plugin.core_plugin, "Plugin %s is not a core plugin" % plugin.name
        self.plugin = plugin
        self.es_src_dir = es_src_dir
        self.builder = builder

    @staticmethod
    def can_handle(plugin):
        return plugin.core_plugin

    def fetch(self):
        pass

    def prepare(self):
        if self.builder:
            self.builder.build(["./gradlew :plugins:{}:assemble".format(self.plugin.name)])

    def add(self, binaries):
        binaries[self.plugin.name] = self.resolve_binary()

    def resolve_binary(self):
        try:
            name = glob.glob("%s/plugins/%s/build/distributions/*.zip" % (self.es_src_dir, self.plugin.name))[0]
            return "file://%s" % name
        except IndexError:
            raise SystemSetupError("Couldn't find a plugin zip file for [%s]. Please run Rally with the pipeline 'from-sources-complete'." %
                                   self.plugin.name)


class ElasticsearchDistributionSupplier:
    def __init__(self, repo, distributions_root):
        self.repo = repo
        self.version = repo.version
        self.distributions_root = distributions_root
        # will be defined in the prepare phase
        self.distribution_path = None
        self.logger = logging.getLogger(__name__)

    def fetch(self):
        io.ensure_dir(self.distributions_root)
        download_url = self.repo.download_url
        distribution_path = os.path.join(self.distributions_root, self.repo.file_name)
        self.logger.info("Resolved download URL [%s] for version [%s]", download_url, self.version)
        if not os.path.isfile(distribution_path) or not self.repo.cache:
            try:
                self.logger.info("Starting download of Elasticsearch [%s]", self.version)
                progress = net.Progress("[INFO] Downloading Elasticsearch %s" % self.version)
                net.download(download_url, distribution_path, progress_indicator=progress)
                progress.finish()
                self.logger.info("Successfully downloaded Elasticsearch [%s].", self.version)
            except urllib.error.HTTPError:
                self.logger.exception("Cannot download Elasticsearch distribution for version [%s] from [%s].", self.version, download_url)
                raise exceptions.SystemSetupError("Cannot download Elasticsearch distribution from [%s]. Please check that the specified "
                                                  "version [%s] is correct." % (download_url, self.version))
        else:
            self.logger.info("Skipping download for version [%s]. Found an existing binary at [%s].", self.version, distribution_path)

        self.distribution_path = distribution_path

    def prepare(self):
        pass

    def add(self, binaries):
        binaries["elasticsearch"] = self.distribution_path


class PluginDistributionSupplier:
    def __init__(self, repo, plugin):
        self.repo = repo
        self.plugin = plugin

    def fetch(self):
        pass

    def prepare(self):
        pass

    def add(self, binaries):
        # if we have multiple plugin configurations for a plugin we will override entries here but as this is always the same
        # key-value pair this is ok.
        plugin_url = self.repo.plugin_download_url(self.plugin.name)
        if plugin_url:
            binaries[self.plugin.name] = plugin_url


def _config_value(src_config, key):
    try:
        return src_config[key]
    except KeyError:
        raise exceptions.SystemSetupError("Mandatory config key [%s] is undefined. Please add it in the [source] section of the "
                                          "config file." % key)


def _extract_revisions(revision):
    revisions = revision.split(",")
    if len(revisions) == 1:
        r = revisions[0]
        if r.startswith("elasticsearch:"):
            r = r[len("elasticsearch:"):]
        # may as well be just a single plugin
        m = re.match(REVISION_PATTERN, r)
        if m:
            return {
                m.group(1): m.group(2)
            }
        else:
            return {
                "elasticsearch": r,
                # use a catch-all value
                "all": r
            }
    else:
        results = {}
        for r in revisions:
            m = re.match(REVISION_PATTERN, r)
            if m:
                results[m.group(1)] = m.group(2)
            else:
                raise exceptions.SystemSetupError("Revision [%s] does not match expected format [name:revision]." % r)
        return results


class SourceRepository:
    """
    Supplier fetches the benchmark candidate source tree from the remote repository.
    """

    def __init__(self, name, remote_url, src_dir):
        self.name = name
        self.remote_url = remote_url
        self.src_dir = src_dir
        self.logger = logging.getLogger(__name__)

    def fetch(self, revision):
        # if and only if we want to benchmark the current revision, Rally may skip repo initialization (if it is already present)
        self._try_init(may_skip_init=revision == "current")
        self._update(revision)

    def has_remote(self):
        return self.remote_url is not None

    def _try_init(self, may_skip_init=False):
        if not git.is_working_copy(self.src_dir):
            if self.has_remote():
                self.logger.info("Downloading sources for %s from %s to %s.", self.name, self.remote_url, self.src_dir)
                git.clone(self.src_dir, self.remote_url)
            elif os.path.isdir(self.src_dir) and may_skip_init:
                self.logger.info("Skipping repository initialization for %s.", self.name)
            else:
                exceptions.SystemSetupError("A remote repository URL is mandatory for %s" % self.name)

    def _update(self, revision):
        if self.has_remote() and revision == "latest":
            self.logger.info("Fetching latest sources for %s from origin.", self.name)
            git.pull(self.src_dir)
        elif revision == "current":
            self.logger.info("Skip fetching sources for %s.", self.name)
        elif self.has_remote() and revision.startswith("@"):
            # convert timestamp annotated for Rally to something git understands -> we strip leading and trailing " and the @.
            git_ts_revision = revision[1:]
            self.logger.info("Fetching from remote and checking out revision with timestamp [%s] for %s.", git_ts_revision, self.name)
            git.pull_ts(self.src_dir, git_ts_revision)
        elif self.has_remote():  # assume a git commit hash
            self.logger.info("Fetching from remote and checking out revision [%s] for %s.", revision, self.name)
            git.pull_revision(self.src_dir, revision)
        else:
            self.logger.info("Checking out local revision [%s] for %s.", revision, self.name)
            git.checkout(self.src_dir, revision)
        if git.is_working_copy(self.src_dir):
            git_revision = git.head_revision(self.src_dir)
            self.logger.info("User-specified revision [%s] for [%s] results in git revision [%s]", revision, self.name, git_revision)
        else:
            self.logger.info("Skipping git revision resolution for %s (%s is not a git repository).", self.name, self.src_dir)

    @classmethod
    def is_commit_hash(cls, revision):
        return revision != "latest" and revision != "current" and not revision.startswith("@")


class Builder:
    """
    A builder is responsible for creating an installable binary from the source files.

    It is not intended to be used directly but should be triggered by its mechanic.
    """

    def __init__(self, src_dir, java_home=None, log_dir=None):
        self.src_dir = src_dir
        self.java_home = java_home
        self.log_dir = log_dir
        self.logger = logging.getLogger(__name__)

    def build(self, commands, override_src_dir=None):
        for command in commands:
            self.run(command, override_src_dir)

    def run(self, command, override_src_dir=None):
        src_dir = self.src_dir if override_src_dir is None else override_src_dir

        io.ensure_dir(self.log_dir)
        log_file = os.path.join(self.log_dir, "build.log")

        # we capture all output to a dedicated build log file

        build_cmd = "export JAVA_HOME={}; cd {}; {} >> {} 2>&1".format(self.java_home, src_dir, command, log_file)
        self.logger.info("Running build command [%s]", build_cmd)

        if process.run_subprocess(build_cmd):
            msg = "Executing '{}' failed. The last 20 lines in the build log file are:\n".format(command)
            msg += "=========================================================================================================\n"
            with open(log_file, "r", encoding="utf-8") as f:
                msg += "\t"
                msg += "\t".join(f.readlines()[-20:])
            msg += "=========================================================================================================\n"
            msg += "The full build log is available at [{}].".format(log_file)

            raise BuildError(msg)


class DistributionRepository:
    def __init__(self, name, distribution_config, version):
        self.name = name
        self.cfg = distribution_config
        self.version = version

    @property
    def download_url(self):
        # team repo
        default_key = "{}_url".format(self.name)
        # rally.ini
        override_key = "{}.url".format(self.name)
        return self._url_for(override_key, default_key)

    @property
    def file_name(self):
        url = self.download_url
        return url[url.rfind("/") + 1:]

    def plugin_download_url(self, plugin_name):
        # team repo
        default_key = "plugin_{}_{}_url".format(plugin_name, self.name)
        # rally.ini
        override_key = "plugin.{}.{}.url".format(plugin_name, self.name)
        return self._url_for(override_key, default_key, mandatory=False)

    def _url_for(self, user_defined_key, default_key, mandatory=True):
        try:
            if user_defined_key in self.cfg:
                url_template = self.cfg[user_defined_key]
            else:
                url_template = self.cfg[default_key]
        except KeyError:
            if mandatory:
                raise exceptions.SystemSetupError("Neither config key [{}] nor [{}] is defined.".format(user_defined_key, default_key))
            else:
                return None
        return url_template.replace("{{VERSION}}", self.version)

    @property
    def cache(self):
        k = "{}.cache".format(self.name)
        try:
            raw_value = self.cfg[k]
        except KeyError:
            raise exceptions.SystemSetupError("Mandatory config key [%s] is undefined." % k)
        try:
            return convert.to_bool(raw_value)
        except ValueError:
            raise exceptions.SystemSetupError("Value [%s] for config key [%s] is not a valid boolean value." % (raw_value, k))
