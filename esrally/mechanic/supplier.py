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

import datetime
import getpass
import glob
import grp
import logging
import os
import re
import shutil
import urllib.error

import docker
from esrally import PROGRAM_NAME, exceptions, paths
from esrally.exceptions import BuildError, SystemSetupError
from esrally.utils import console, convert, git, io, jvm, net, process, sysstats

# e.g. my-plugin:current - we cannot simply use String#split(":") as this would not work for timestamp-based revisions
REVISION_PATTERN = r"(\w.*?):(.*)"


def create(cfg, sources, distribution, car, plugins=None):
    logger = logging.getLogger(__name__)
    if plugins is None:
        plugins = []
    caching_enabled = cfg.opts("source", "cache", mandatory=False, default_value=True)
    revisions = _extract_revisions(cfg.opts("mechanic", "source.revision", mandatory=sources))
    source_build_method = cfg.opts("mechanic", "source.build.method", mandatory=False, default_value="default")
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    supply_requirements = _supply_requirements(sources, distribution, plugins, revisions, distribution_version)
    build_needed = any(build for _, _, build in supply_requirements.values())
    es_supplier_type, es_version, _ = supply_requirements["elasticsearch"]
    src_config = cfg.all_opts("source")
    suppliers = []

    target_os = cfg.opts("mechanic", "target.os", mandatory=False)
    target_arch = cfg.opts("mechanic", "target.arch", mandatory=False)
    template_renderer = TemplateRenderer(version=es_version, os_name=target_os, arch=target_arch)

    if build_needed:
        es_src_dir = os.path.join(_src_dir(cfg), _config_value(src_config, "elasticsearch.src.subdir"))

        if source_build_method == "docker":
            builder = DockerBuilder(src_dir=es_src_dir, log_dir=paths.logs(), client=docker.from_env())
        else:
            raw_build_jdk = car.mandatory_var("build.jdk")
            try:
                build_jdk = int(raw_build_jdk)
            except ValueError:
                raise exceptions.SystemSetupError(f"Car config key [build.jdk] is invalid: [{raw_build_jdk}] (must be int)")
            builder = Builder(
                build_jdk=build_jdk,
                src_dir=es_src_dir,
                log_dir=paths.logs(),
            )

    else:
        builder = None

    distributions_root = os.path.join(cfg.opts("node", "root.dir"), cfg.opts("source", "distribution.dir"))
    dist_cfg = {}
    # car / plugin defines defaults...
    dist_cfg.update(car.variables)
    for plugin in plugins:
        for k, v in plugin.variables.items():
            dist_cfg[f"plugin_{plugin.name}_{k}"] = v
    # ... but the user can override it in rally.ini
    dist_cfg.update(cfg.all_opts("distributions"))

    if caching_enabled:
        logger.info("Enabling source artifact caching.")
        max_age_days = int(cfg.opts("source", "cache.days", mandatory=False, default_value=7))
        if max_age_days <= 0:
            raise exceptions.SystemSetupError(f"cache.days must be a positive number but is {max_age_days}")

        source_distributions_root = os.path.join(distributions_root, "src")
        _prune(source_distributions_root, max_age_days)
    else:
        logger.info("Disabling source artifact caching.")
        source_distributions_root = None

    if es_supplier_type == "source":
        es_src_dir = os.path.join(_src_dir(cfg), _config_value(src_config, "elasticsearch.src.subdir"))

        source_supplier = ElasticsearchSourceSupplier(
            revision=es_version,
            es_src_dir=es_src_dir,
            remote_url=cfg.opts("source", "remote.repo.url"),
            car=car,
            builder=builder,
            template_renderer=template_renderer,
        )

        if caching_enabled:
            es_file_resolver = ElasticsearchFileNameResolver(dist_cfg, template_renderer)
            source_supplier = CachedSourceSupplier(source_distributions_root, source_supplier, es_file_resolver)

        suppliers.append(source_supplier)
        repo = None
    else:
        es_src_dir = None
        repo = DistributionRepository(
            name=cfg.opts("mechanic", "distribution.repository"), distribution_config=dist_cfg, template_renderer=template_renderer
        )
        suppliers.append(ElasticsearchDistributionSupplier(repo, es_version, distributions_root))

    for plugin in plugins:
        if plugin.moved_to_module:
            # TODO: https://github.com/elastic/rally/issues/1622
            # If it is listed as a core plugin (i.e. a user has overriden the team's path or revision), then we will build
            # We still need to use the post-install hooks to configure the keystore, so don't remove from list of plugins
            logger.info("Plugin [%s] is now an Elasticsearch module and no longer needs to be built from source.", plugin.name)
            continue

        supplier_type, plugin_version, _ = supply_requirements[plugin.name]

        if supplier_type == "source":
            if CorePluginSourceSupplier.can_handle(plugin):
                logger.info("Adding core plugin source supplier for [%s].", plugin.name)
                assert es_src_dir is not None, f"Cannot build core plugin {plugin.name} when Elasticsearch is not built from source."
                plugin_supplier = CorePluginSourceSupplier(plugin, es_src_dir, builder)
            elif ExternalPluginSourceSupplier.can_handle(plugin):
                logger.info("Adding external plugin source supplier for [%s].", plugin.name)
                plugin_supplier = ExternalPluginSourceSupplier(
                    plugin,
                    plugin_version,
                    _src_dir(cfg, mandatory=False),
                    src_config,
                    Builder(
                        src_dir=es_src_dir,
                        log_dir=paths.logs(),
                    ),
                )
            else:
                raise exceptions.RallyError(
                    "Plugin %s can neither be treated as core nor as external plugin. Requirements: %s"
                    % (plugin.name, supply_requirements[plugin.name])
                )

            if caching_enabled:
                plugin_file_resolver = PluginFileNameResolver(plugin.name, plugin_version)
                plugin_supplier = CachedSourceSupplier(source_distributions_root, plugin_supplier, plugin_file_resolver)
            suppliers.append(plugin_supplier)
        else:
            logger.info("Adding plugin distribution supplier for [%s].", plugin.name)
            assert repo is not None, "Cannot benchmark plugin %s from a distribution version but Elasticsearch from sources" % plugin.name
            suppliers.append(PluginDistributionSupplier(repo, plugin))

    return CompositeSupplier(suppliers)


def _required_version(version):
    if not version or version.strip() == "":
        raise exceptions.SystemSetupError(
            "Could not determine version. Please specify the Elasticsearch distribution "
            "to download with the command line parameter --distribution-version."
        )
    return version


def _required_revision(revisions, key, name=None):
    try:
        return revisions[key]
    except KeyError:
        n = name if name is not None else key
        raise exceptions.SystemSetupError("No revision specified for %s" % n)


def _supply_requirements(sources, distribution, plugins, revisions, distribution_version):
    # per artifact (elasticsearch or a specific plugin):
    #   * key: artifact
    #   * value: ("source" | "distribution", distribution_version | revision, build = True | False)
    supply_requirements = {}

    # can only build Elasticsearch with source-related pipelines -> ignore revision in that case
    if "elasticsearch" in revisions and sources:
        supply_requirements["elasticsearch"] = ("source", _required_revision(revisions, "elasticsearch", "Elasticsearch"), True)
    else:
        # no revision given or explicitly specified that it's from a distribution -> must use a distribution
        supply_requirements["elasticsearch"] = ("distribution", _required_version(distribution_version), False)

    for plugin in plugins:
        if plugin.moved_to_module:
            # TODO: https://github.com/elastic/rally/issues/1622
            continue
        elif plugin.core_plugin:
            # core plugins are entirely dependent upon Elasticsearch.
            supply_requirements[plugin.name] = supply_requirements["elasticsearch"]
        else:
            # allow catch-all only if we're generally building from sources. If it is mixed, the user should tell explicitly.
            if plugin.name in revisions or ("all" in revisions and sources):
                # be a bit more lenient when checking for plugin revisions. This allows users to specify `--revision="current"` and
                # rely on Rally to do the right thing.
                try:
                    plugin_revision = revisions[plugin.name]
                except KeyError:
                    # maybe we can use the catch-all revision (only if it's not a git revision)
                    plugin_revision = revisions.get("all")
                    if not plugin_revision or SourceRepository.is_commit_hash(plugin_revision):
                        raise exceptions.SystemSetupError("No revision specified for plugin [%s]." % plugin.name)
                    logging.getLogger(__name__).info(
                        "Revision for [%s] is not explicitly defined. Using catch-all revision [%s].", plugin.name, plugin_revision
                    )
                supply_requirements[plugin.name] = ("source", plugin_revision, True)
            else:
                supply_requirements[plugin.name] = (distribution, _required_version(distribution_version), False)
    return supply_requirements


def _src_dir(cfg, mandatory=True):
    # Don't let this spread across the whole module
    try:
        return cfg.opts("node", "src.root.dir", mandatory=mandatory)
    except exceptions.ConfigError:
        raise exceptions.SystemSetupError(
            "You cannot benchmark Elasticsearch from sources. Did you install Gradle? Please install"
            " all prerequisites and reconfigure Rally with %s configure" % PROGRAM_NAME
        )


def _prune(root_path, max_age_days):
    """
    Removes files that are older than ``max_age_days`` from ``root_path``. Subdirectories are not traversed.

    :param root_path: A directory which should be checked.
    :param max_age_days: Files that have been created more than ``max_age_days`` ago are deleted.
    """
    logger = logging.getLogger(__name__)
    if not os.path.exists(root_path):
        logger.info("[%s] does not exist. Skipping pruning.", root_path)
        return

    for f in os.listdir(root_path):
        artifact = os.path.join(root_path, f)
        if os.path.isfile(artifact):
            max_age = datetime.datetime.now() - datetime.timedelta(days=max_age_days)
            try:
                created_at = datetime.datetime.fromtimestamp(os.lstat(artifact).st_ctime)
                if created_at < max_age:
                    logger.info("Deleting [%s] from artifact cache (reached max age).", f)
                    os.remove(artifact)
                else:
                    logger.debug("Keeping [%s] (max age not yet reached)", f)
            except OSError:
                logger.exception("Could not check whether [%s] needs to be deleted from artifact cache.", artifact)
        else:
            logger.info("Skipping [%s] (not a file).", artifact)


class TemplateRenderer:
    def __init__(self, version, os_name=None, arch=None):
        self.version = version
        if os_name is not None:
            self.os = os_name
        else:
            self.os = sysstats.os_name().lower()
        if arch is not None:
            self.arch = arch
        else:
            derived_arch = sysstats.cpu_arch().lower()
            # Elasticsearch artifacts for Apple Silicon use "aarch64" as the CPU architecture
            self.arch = "aarch64" if derived_arch == "arm64" else derived_arch

    def render(self, template):
        substitutions = {"{{VERSION}}": self.version, "{{OSNAME}}": self.os, "{{ARCH}}": self.arch}
        r = template
        for key, replacement in substitutions.items():
            r = r.replace(key, str(replacement))
        return r


class CompositeSupplier:
    def __init__(self, suppliers):
        self.suppliers = suppliers

    def __call__(self, *args, **kwargs):
        binaries = {}
        for supplier in self.suppliers:
            supplier.fetch()
        for supplier in self.suppliers:
            supplier.prepare()
        for supplier in self.suppliers:
            supplier.add(binaries)
        return binaries


class ElasticsearchFileNameResolver:
    def __init__(self, distribution_config, template_renderer):
        self.cfg = distribution_config
        self.runtime_jdk_bundled = convert.to_bool(self.cfg.get("runtime.jdk.bundled", False))
        self.template_renderer = template_renderer

    @property
    def revision(self):
        return self.template_renderer.version

    @revision.setter
    def revision(self, revision):
        self.template_renderer.version = revision

    @property
    def file_name(self):
        if self.runtime_jdk_bundled:
            url_key = "jdk.bundled.release_url"
        else:
            url_key = "jdk.unbundled.release_url"
        url = self.template_renderer.render(self.cfg[url_key])
        return url[url.rfind("/") + 1 :]

    @property
    def artifact_key(self):
        return "elasticsearch"

    def to_artifact_path(self, file_system_path):
        return file_system_path

    def to_file_system_path(self, artifact_path):
        return artifact_path


class CachedSourceSupplier:
    def __init__(self, distributions_root, source_supplier, file_resolver):
        self.distributions_root = distributions_root
        self.source_supplier = source_supplier
        self.file_resolver = file_resolver
        self.cached_path = None
        self.logger = logging.getLogger(__name__)

    @property
    def file_name(self):
        return self.file_resolver.file_name

    @property
    def cached(self):
        return self.cached_path is not None and os.path.exists(self.cached_path)

    def fetch(self):
        # Can we already resolve the artifact without fetching the source tree at all? This is the case when a specific
        # revision (instead of a meta-revision like "current") is provided and the artifact is already cached. This is
        # also needed if an external process pushes artifacts to Rally's cache which might have been built from a
        # fork. In that case the provided commit hash would not be present in any case in the main ES repo.
        maybe_an_artifact = os.path.join(self.distributions_root, self.file_name)
        if os.path.exists(maybe_an_artifact):
            self.cached_path = maybe_an_artifact
        else:
            resolved_revision = self.source_supplier.fetch()
            if resolved_revision:
                # ensure we use the resolved revision for rendering the artifact
                self.file_resolver.revision = resolved_revision
                self.cached_path = os.path.join(self.distributions_root, self.file_name)

    def prepare(self):
        if not self.cached:
            self.source_supplier.prepare()

    def add(self, binaries):
        if self.cached:
            self.logger.info("Using cached artifact in [%s]", self.cached_path)
            binaries[self.file_resolver.artifact_key] = self.file_resolver.to_artifact_path(self.cached_path)
        else:
            self.source_supplier.add(binaries)
            original_path = self.file_resolver.to_file_system_path(binaries[self.file_resolver.artifact_key])
            # this can be None if the Elasticsearch does not reside in a git repo and the user has only
            # copied all source files. In that case, we cannot resolve a revision hash and thus we cannot cache.
            if self.cached_path:
                try:
                    io.ensure_dir(io.dirname(self.cached_path))
                    shutil.copy(original_path, self.cached_path)
                    self.logger.info("Caching artifact in [%s]", self.cached_path)
                    binaries[self.file_resolver.artifact_key] = self.file_resolver.to_artifact_path(self.cached_path)
                except OSError:
                    self.logger.exception("Not caching [%s].", original_path)
            else:
                self.logger.info("Not caching [%s] (no revision info).", original_path)


class ElasticsearchSourceSupplier:
    def __init__(self, revision, es_src_dir, remote_url, car, builder, template_renderer):
        self.logger = logging.getLogger(__name__)
        self.revision = revision
        self.src_dir = es_src_dir
        self.remote_url = remote_url
        self.car = car
        self.builder = builder
        self.template_renderer = template_renderer

    def fetch(self):
        return SourceRepository("Elasticsearch", self.remote_url, self.src_dir, branch="main").fetch(self.revision)

    def prepare(self):
        if self.builder:
            self.builder.build_jdk = self.resolve_build_jdk_major(self.src_dir)

            # There are no 'x86_64' specific gradle build commands
            if self.template_renderer.arch != "x86_64":
                commands = [
                    self.template_renderer.render(self.car.mandatory_var("clean_command")),
                    self.template_renderer.render(self.car.mandatory_var("system.build_command.arch")),
                ]
            else:
                commands = [
                    self.template_renderer.render(self.car.mandatory_var("clean_command")),
                    self.template_renderer.render(self.car.mandatory_var("system.build_command")),
                ]

            self.builder.build(commands)

    def add(self, binaries):
        binaries["elasticsearch"] = self.resolve_binary()

    @classmethod
    def resolve_build_jdk_major(cls, src_dir: str) -> int:
        """
        Parses the build JDK major release version from the Elasticsearch repo
        :param src_dir: The source directory for the Elasticsearch repository
        :return: The build JDK major release version
        """
        logger = logging.getLogger(__name__)
        # This .properties file defines the versions of Java with which to
        # build and test Elasticsearch for this branch. Valid Java versions
        # are 'java' or 'openjdk' followed by the major release number.
        path = os.path.join(src_dir, ".ci", "java-versions.properties")
        major_version = None
        try:
            with open(path, encoding="UTF-8") as f:
                for line in f.readlines():
                    if "ES_BUILD_JAVA" in line:
                        java_version = line.split("=")[1].lstrip().rstrip("\n")
                        # e.g. java11
                        if "java" in java_version:
                            major_version = java_version.split("java")[1]
                        # e.g. openjdk16
                        elif "openjdk" in java_version:
                            major_version = java_version.split("openjdk")[1]
                        break
        except FileNotFoundError:
            msg = f"File [{path}] not found."
            console.warn(f"{msg}")
            logger.warning("%s", msg)

        if major_version:
            logger.info("Setting build JDK major release version to [%s].", major_version)
        else:
            major_version = 17
            logger.info("Unable to resolve build JDK major release version. Defaulting to version [%s].", major_version)
        return int(major_version)

    def resolve_binary(self):
        try:
            # There are no 'x86_64' specific gradle build commands,
            if self.template_renderer.arch != "x86_64":
                system_artifact_path = self.car.mandatory_var("system.artifact_path_pattern.arch")
            else:
                system_artifact_path = self.car.mandatory_var("system.artifact_path_pattern")
            path = os.path.join(self.src_dir, self.template_renderer.render(system_artifact_path))
            return glob.glob(path)[0]
        except IndexError:
            raise SystemSetupError("Couldn't find a tar.gz distribution. Please run Rally with the pipeline 'from-sources'.")


class PluginFileNameResolver:
    def __init__(self, plugin_name, revision=None):
        self.plugin_name = plugin_name
        self.revision = revision

    @property
    def file_name(self):
        return f"{self.plugin_name}-{self.revision}.zip"

    @property
    def artifact_key(self):
        return self.plugin_name

    def to_artifact_path(self, file_system_path):
        return f"file://{file_system_path}"

    def to_file_system_path(self, artifact_path):
        return artifact_path[len("file://") :]


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

        if dir_cfg_key in self.src_config:
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
        return SourceRepository(self.plugin.name, plugin_remote_url, self.plugin_src_dir, branch="master").fetch(self.revision)

    def prepare(self):
        if self.builder:
            command = _config_value(self.src_config, f"plugin.{self.plugin.name}.build.command")
            build_cmd = f"export JAVA_HOME={self.builder.java_home}; cd {self.override_build_dir}; {command}"
            self.builder.build([build_cmd])

    def add(self, binaries):
        binaries[self.plugin.name] = self.resolve_binary()

    def resolve_binary(self):
        artifact_path = _config_value(self.src_config, "plugin.%s.build.artifact.subdir" % self.plugin.name)
        try:
            name = glob.glob("%s/%s/*.zip" % (self.plugin_src_dir, artifact_path))[0]
            return "file://%s" % name
        except IndexError:
            raise SystemSetupError(
                "Couldn't find a plugin zip file for [%s]. Please run Rally with the pipeline 'from-sources'." % self.plugin.name
            )


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
        # Just retrieve the current revision *number* and assume that Elasticsearch has prepared the source tree.
        return SourceRepository("Elasticsearch", None, self.es_src_dir, branch="main").fetch(revision="current")

    def prepare(self):
        if self.builder:
            self.builder.build_jdk = ElasticsearchSourceSupplier.resolve_build_jdk_major(self.es_src_dir)
            self.builder.build([f"./gradlew :plugins:{self.plugin.name}:assemble"])

    def add(self, binaries):
        binaries[self.plugin.name] = self.resolve_binary()

    def resolve_binary(self):
        try:
            name = glob.glob("%s/plugins/%s/build/distributions/*.zip" % (self.es_src_dir, self.plugin.name))[0]
            return "file://%s" % name
        except IndexError:
            raise SystemSetupError(
                "Couldn't find a plugin zip file for [%s]. Please run Rally with the pipeline 'from-sources'." % self.plugin.name
            )


class ElasticsearchDistributionSupplier:
    def __init__(self, repo, version, distributions_root):
        self.repo = repo
        self.version = version
        self.distributions_root = distributions_root
        # will be defined in the prepare phase
        self.distribution_path = None
        self.logger = logging.getLogger(__name__)

    def fetch(self):
        io.ensure_dir(self.distributions_root)
        download_url = net.add_url_param_elastic_no_kpi(self.repo.download_url)
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
                raise exceptions.SystemSetupError(
                    "Cannot download Elasticsearch distribution from [%s]. Please check that the specified "
                    "version [%s] is correct." % (download_url, self.version)
                )
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
        raise exceptions.SystemSetupError(
            "Mandatory config key [%s] is undefined. Please add it in the [source] section of the config file." % key
        )


def _extract_revisions(revision):
    revisions = revision.split(",") if revision else []
    if len(revisions) == 1:
        r = revisions[0]
        if r.startswith("elasticsearch:"):
            r = r[len("elasticsearch:") :]
        # may as well be just a single plugin
        m = re.match(REVISION_PATTERN, r)
        if m:
            return {m.group(1): m.group(2)}
        else:
            return {
                "elasticsearch": r,
                # use a catch-all value
                "all": r,
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

    def __init__(self, name, remote_url, src_dir, *, branch):
        self.name = name
        self.remote_url = remote_url
        self.src_dir = src_dir
        self.branch = branch
        self.logger = logging.getLogger(__name__)

    def fetch(self, revision):
        # if and only if we want to benchmark the current revision, Rally may skip repo initialization (if it is already present)
        self._try_init(may_skip_init=revision == "current")
        return self._update(revision)

    def has_remote(self):
        return self.remote_url is not None

    def _try_init(self, may_skip_init=False):
        if not git.is_working_copy(self.src_dir):
            if self.has_remote():
                self.logger.info("Downloading sources for %s from %s to %s.", self.name, self.remote_url, self.src_dir)
                git.clone(self.src_dir, remote=self.remote_url)
            elif os.path.isdir(self.src_dir) and may_skip_init:
                self.logger.info("Skipping repository initialization for %s.", self.name)
            else:
                exceptions.SystemSetupError("A remote repository URL is mandatory for %s" % self.name)

    def _update(self, revision):
        if self.has_remote() and revision == "latest":
            self.logger.info("Fetching latest sources for %s from origin.", self.name)
            git.pull(self.src_dir, remote="origin", branch=self.branch)
        elif revision == "current":
            self.logger.info("Skip fetching sources for %s.", self.name)
        elif self.has_remote() and revision.startswith("@"):
            # convert timestamp annotated for Rally to something git understands -> we strip leading and trailing " and the @.
            git_ts_revision = revision[1:]
            self.logger.info("Fetching from remote and checking out revision with timestamp [%s] for %s.", git_ts_revision, self.name)
            git.pull_ts(self.src_dir, git_ts_revision, remote="origin", branch=self.branch)
        elif self.has_remote():  # we can have either a commit hash, branch name, or tag
            git.fetch(self.src_dir, remote="origin")
            if git.is_branch(self.src_dir, identifier=revision):
                self.logger.info("Fetching from remote and checking out branch [%s] for %s.", revision, self.name)
                git.checkout_branch(self.src_dir, remote="origin", branch=revision)
            else:  # tag or commit hash
                self.logger.info("Fetching from remote and checking out revision [%s] for %s.", revision, self.name)
                git.checkout_revision(self.src_dir, revision=revision)
        else:
            self.logger.info("Checking out local revision [%s] for %s.", revision, self.name)
            git.checkout(self.src_dir, branch=revision)
        if git.is_working_copy(self.src_dir):
            git_revision = git.head_revision(self.src_dir)
            self.logger.info("User-specified revision [%s] for [%s] results in git revision [%s]", revision, self.name, git_revision)
            return git_revision
        else:
            self.logger.info("Skipping git revision resolution for %s (%s is not a git repository).", self.name, self.src_dir)
            return None

    @classmethod
    def is_commit_hash(cls, revision):
        return revision != "latest" and revision != "current" and not revision.startswith("@")


class Builder:
    """
    A builder is responsible for creating an installable binary from the source files.

    It is not intended to be used directly but should be triggered by its mechanic.
    """

    def __init__(self, src_dir, build_jdk=None, log_dir=None):
        self.src_dir = src_dir
        self.build_jdk = build_jdk
        self._java_home = None
        self.log_dir = log_dir
        self.logger = logging.getLogger(__name__)

    @property
    def java_home(self):
        if not self._java_home:
            _, self._java_home = jvm.resolve_path(self.build_jdk)
        return self._java_home

    def build(self, commands, override_src_dir=None):
        for command in commands:
            self.run(command, override_src_dir)

    def run(self, command, override_src_dir=None):
        src_dir = self.src_dir if override_src_dir is None else override_src_dir

        io.ensure_dir(self.log_dir)
        log_file = os.path.join(self.log_dir, "build.log")

        # we capture all output to a dedicated build log file
        build_cmd = f"export JAVA_HOME={self.java_home}; cd {src_dir}; {command} >> {log_file} 2>&1"
        console.info("Creating installable binary from source files")
        self.logger.info("Running build command [%s]", build_cmd)

        if process.run_subprocess(build_cmd):
            msg = f"Executing '{command}' failed. The last 20 lines in the build log file are:\n"
            msg += "=========================================================================================================\n"
            with open(log_file, encoding="utf-8") as f:
                msg += "\t"
                msg += "\t".join(f.readlines()[-20:])
            msg += "=========================================================================================================\n"
            msg += f"The full build log is available at [{log_file}]."

            raise BuildError(msg)


class DockerBuilder:
    def __init__(self, src_dir, build_jdk=None, log_dir=None, client=None):
        self.client = client
        self.src_dir = src_dir
        self.build_jdk = build_jdk
        self.log_dir = log_dir
        io.ensure_dir(self.log_dir)
        self.log_file = os.path.join(self.log_dir, "build.log")
        self.logger = logging.getLogger(__name__)

        try:
            self.logger.info(self.client.version())
        except docker.errors.APIError as e:
            raise exceptions.SystemSetupError(self.err_msg(e))

        self.user_id = os.geteuid()
        self.user_name = getpass.getuser()
        self.group_id = os.getgid()
        try:
            # we need to get the user's group id to add it to our container image for permissions on bind mount
            self.group_name = grp.getgrgid(self.group_id)[0]
        except KeyError:
            raise SystemSetupError("Failed to retrieve current user's group name required for Docker source build method.")

        self.build_container_name = "esrally-source-builder"
        self.image_name = f"{self.build_container_name}-image"
        self.image_builder_container_name = f"{self.build_container_name}-image-builder"

    @staticmethod
    def err_msg(err):
        return (
            f"Error communicating with Docker daemon: [{err}]. Please ensure Docker is installed and your user has the correct permissions."
        )

    def resolve_jdk_build_container_image(self, build_jdk: int) -> str:
        """
        Given a JDK major release version, find a suitable docker image to use for compiling ES & ES plugins from sources
        :param build_jdk: The build JDK major version required to build Elasticsearch and plugins.
        :return: The Docker image name and respective tag that satisfies the 'build_jdk' requirement.
        """
        logger = logging.getLogger(__name__)
        # Temurin only maintains images for JDK 11, 17, 18 (at time of writing)
        # older builds of ES may require specifics like JDK 14 etc, in which case we rely on the OpenJDK images
        #
        # We modify the base image (create user/group, install git), so we need to ensure the conatiner's base image
        # is Debian based (apt required)
        if build_jdk >= 17:
            docker_image_name = f"eclipse-temurin:{build_jdk}-jdk-jammy"
            logger.info("Build JDK version [%s] is >= 17, using Docker image [%s].", build_jdk, docker_image_name)
        # There is no Debian based OpenJDK JDK 12 image
        elif build_jdk == 12:
            docker_image_name = "adoptopenjdk/openjdk12:debian"
        else:
            docker_image_name = f"openjdk:{build_jdk}-jdk-buster"
            logger.info("Build JDK version [%s] is < 17, using Docker image [%s].", build_jdk, docker_image_name)
        return docker_image_name

    def stop_containers(self, container_names):
        for c in container_names:
            try:
                existing_container = self.client.containers.get(c)
                existing_container.stop()
                existing_container.remove()
            except docker.errors.NotFound:
                pass
            except docker.errors.APIError as e:
                raise exceptions.SystemSetupError(self.err_msg(e))

    def tail_container_logs(self, output, container_name):
        try:
            with open(self.log_file, "a+", encoding="utf-8") as f:
                while True:
                    line = next(output).decode("utf-8")
                    f.write(line)
                    f.flush()
        except StopIteration:
            self.logger.info("Log stream ended for [%s]", container_name)

    def check_container_return_code(self, completion, container_name):
        if completion["StatusCode"] != 0:
            msg = f"Executing '{container_name}' failed. The last 20 lines in the build.log file are:\n"
            msg += "=========================================================================================================\n"
            with open(self.log_file, encoding="utf-8") as f:
                msg += "\t"
                msg += "\t".join(f.readlines()[-20:])
            msg += "=========================================================================================================\n"
            msg += f"The full build log is available at [{self.log_file}]"
            raise BuildError(
                f"Docker container [{container_name}] failed with status code [{completion['StatusCode']}]: "
                f"Error [{completion['Error']}]: Build log output [{msg}]"
            )
        self.logger.info("Container [%s] completed successfully.", container_name)

    def create_base_container_image(self):
        try:
            # create a new image with the required users & git installed
            image_container = self.client.containers.run(
                detach=True,
                name=self.image_builder_container_name,
                image=self.resolve_jdk_build_container_image(self.build_jdk),
                command=(
                    # {1..5} is for retrying apt update/install in case of transient network issues
                    '/bin/bash -c "for i in {1..5}; do apt update -y; apt install git -y && break || sleep 15; done; '
                    f"useradd --create-home -u {self.user_id} {self.user_name}; "
                    f"groupadd -g {self.group_id} {self.group_name}; "
                    f'usermod -g {self.group_name} {self.user_name}"'
                ),
            )

            output = image_container.logs(stream=True)
            self.tail_container_logs(output, self.image_builder_container_name)

            # wait for container to complete
            completion = image_container.wait()
            self.check_container_return_code(completion, self.image_builder_container_name)

            # create the image (i.e. docker commit)
            image = image_container.commit(self.image_name)

            return image

        except docker.errors.APIError as e:
            raise exceptions.SystemSetupError(self.err_msg(e))

    def build(self, commands):
        build_command = "; ".join(commands)
        self.run(build_command)

    def run(self, command):
        # stop & remove any pre-existing containers
        self.stop_containers([self.build_container_name, self.image_builder_container_name])
        # build our new base image
        container_image = self.create_base_container_image()
        try:
            console.info("Using Docker to create installable binary from source files")
            container = self.client.containers.run(
                detach=True,
                name=self.build_container_name,
                image=container_image.id,
                user=self.user_name,
                group_add=[self.group_id],
                command=f"/bin/bash -c \"git config --global --add safe.directory '*'; {command}\"",
                volumes=[f"{self.src_dir}:/home/{self.user_name}/elasticsearch"],
                working_dir=f"/home/{self.user_name}/elasticsearch",
            )

            output = container.logs(stream=True)
            self.tail_container_logs(output, self.build_container_name)

            # wait for container to complete
            completion = container.wait()
            self.check_container_return_code(completion, self.build_container_name)

        except docker.errors.APIError as e:
            raise exceptions.SystemSetupError(self.err_msg(e))


class DistributionRepository:
    def __init__(self, name, distribution_config, template_renderer):
        self.name = name
        self.cfg = distribution_config
        self.runtime_jdk_bundled = convert.to_bool(self.cfg.get("runtime.jdk.bundled", False))
        self.template_renderer = template_renderer

    @property
    def download_url(self):
        # team repo
        if self.runtime_jdk_bundled:
            default_key = f"jdk.bundled.{self.name}_url"
        else:
            default_key = f"jdk.unbundled.{self.name}_url"
        # rally.ini
        override_key = f"{self.name}.url"
        return self._url_for(override_key, default_key)

    @property
    def file_name(self):
        url = self.download_url
        return url[url.rfind("/") + 1 :]

    def plugin_download_url(self, plugin_name):
        # team repo
        default_key = f"plugin_{plugin_name}_{self.name}_url"
        # rally.ini
        override_key = f"plugin.{plugin_name}.{self.name}.url"
        return self._url_for(override_key, default_key, mandatory=False)

    def _url_for(self, user_defined_key, default_key, mandatory=True):
        try:
            if user_defined_key in self.cfg:
                url_template = self.cfg[user_defined_key]
            else:
                url_template = self.cfg[default_key]
        except KeyError:
            if mandatory:
                raise exceptions.SystemSetupError(f"Neither config key [{user_defined_key}] nor [{default_key}] is defined.")
            return None
        return self.template_renderer.render(url_template)

    @property
    def cache(self):
        k = f"{self.name}.cache"
        try:
            raw_value = self.cfg[k]
        except KeyError:
            raise exceptions.SystemSetupError("Mandatory config key [%s] is undefined." % k)
        try:
            return convert.to_bool(raw_value)
        except ValueError:
            raise exceptions.SystemSetupError("Value [%s] for config key [%s] is not a valid boolean value." % (raw_value, k))
