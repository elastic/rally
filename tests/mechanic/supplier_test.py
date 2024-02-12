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
# pylint: disable=protected-access

import collections
import datetime
import getpass
import os
from unittest import mock

import pytest

from esrally import config, exceptions
from esrally.mechanic import supplier, team


class TestRevisionExtractor:
    def test_single_revision(self):
        assert supplier._extract_revisions("67c2f42") == {
            "elasticsearch": "67c2f42",
            "all": "67c2f42",
        }
        assert supplier._extract_revisions("current") == {
            "elasticsearch": "current",
            "all": "current",
        }
        assert supplier._extract_revisions("@2022-12-12T11:12:13Z") == {
            "elasticsearch": "@2022-12-12T11:12:13Z",
            "all": "@2022-12-12T11:12:13Z",
        }
        assert supplier._extract_revisions("feature/branch@2024-04-20T08:00:00Z") == {
            "elasticsearch": "feature/branch@2024-04-20T08:00:00Z",
            "all": "feature/branch@2024-04-20T08:00:00Z",
        }

    def test_multiple_revisions(self):
        assert supplier._extract_revisions("elasticsearch:67c2f42,one-plugin:@2023-04-04T12:34:56,another-plugin:current") == {
            "elasticsearch": "67c2f42",
            "one-plugin": "@2023-04-04T12:34:56",
            "another-plugin": "current",
        }

    def test_invalid_revisions(self):
        with pytest.raises(exceptions.SystemSetupError) as exc:
            supplier._extract_revisions("elasticsearch 67c2f42,x-pack:current")
        assert exc.value.args[0] == "Revision [elasticsearch 67c2f42] does not match expected format [name:revision]."


class TestComponentFromRevision:
    def test_revision_sha(self):
        assert supplier._component_from_revision("e8d4211de73ad498df865e7df935feb890808eb9") == (
            "",
            "e8d4211de73ad498df865e7df935feb890808eb9",
        )

    def test_revision_ts_only(self):
        assert supplier._component_from_revision("@2023-04-03T03:04:05Z") == ("", "@2023-04-03T03:04:05Z")

    def test_revision_component_and_sha(self):
        assert supplier._component_from_revision("elasticsearch:latest") == ("elasticsearch", "latest")

    def test_revision_component_and_ts(self):
        assert supplier._component_from_revision("elasticsearch:@2023-04-03T03:04:05Z") == ("elasticsearch", "@2023-04-03T03:04:05Z")

    def test_revision_component_branch_and_ts(self):
        assert supplier._component_from_revision("elasticsearch:feature/branch@2023-04-03T03:04:05Z") == (
            "elasticsearch",
            "feature/branch@2023-04-03T03:04:05Z",
        )


class TestSourceRepository:
    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull", autospec=True)
    @mock.patch("esrally.utils.git.clone", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_intial_checkout_latest(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        # before cloning, it is not a working copy, afterwards it is
        mock_is_working_copy.side_effect = [False, True]
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src", branch="main")
        s.fetch("latest")

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_called_with("/src", remote="some-github-url")
        mock_pull.assert_called_with("/src", remote="origin", branch="main")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull")
    @mock.patch("esrally.utils.git.clone")
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_current(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src", branch="main")
        s.fetch("current")

        mock_is_working_copy.assert_called_with("/src")
        assert mock_clone.call_count == 0
        assert mock_pull.call_count == 0
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.checkout")
    @mock.patch("esrally.utils.git.pull")
    @mock.patch("esrally.utils.git.clone")
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_revision_for_local_only_repo(self, mock_is_working_copy, mock_clone, mock_pull, mock_checkout, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        # local only, we dont specify a remote
        s = supplier.SourceRepository(name="Elasticsearch", remote_url=None, src_dir="/src", branch="main")
        s.fetch("67c2f42")

        mock_is_working_copy.assert_called_with("/src")
        assert mock_clone.call_count == 0
        assert mock_pull.call_count == 0
        mock_checkout.assert_called_with("/src", branch="67c2f42")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull_ts", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_ts(self, mock_is_working_copy, mock_pull_ts, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src", branch="main")
        s.fetch("@2023-04-20T11:09:12Z")

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_ts.assert_called_with("/src", "2023-04-20T11:09:12Z", remote="origin", branch="main")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.fetch", autospec=True)
    @mock.patch("esrally.utils.git.is_branch", autospec=True)
    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.checkout_revision", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_revision(self, mock_is_working_copy, mock_checkout_revision, mock_head_revision, mock_is_branch, mock_fetch):
        mock_is_working_copy.return_value = True
        mock_is_branch.return_value = False
        mock_head_revision.return_value = "HEAD"
        mock_fetch.return_value = None

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src", branch="main")
        s.fetch("67c2f42")

        mock_is_working_copy.assert_called_with("/src")
        mock_checkout_revision.assert_called_with("/src", revision="67c2f42")
        mock_head_revision.assert_called_with("/src")

    def test_is_commit_hash(self):
        assert supplier.SourceRepository.is_commit_hash("67c2f42")

    def test_is_not_commit_hash(self):
        assert not supplier.SourceRepository.is_commit_hash("latest")
        assert not supplier.SourceRepository.is_commit_hash("current")
        assert not supplier.SourceRepository.is_commit_hash("@2015-01-01-01:00:00")


class TestBuilder:
    @mock.patch("esrally.utils.process.run_subprocess")
    @mock.patch("esrally.utils.jvm.resolve_path")
    def test_build_on_jdk_8(self, jvm_resolve_path, mock_run_subprocess):
        jvm_resolve_path.return_value = (8, "/opt/jdk8")
        mock_run_subprocess.return_value = False

        b = supplier.Builder(src_dir="/src", build_jdk=8, log_dir="logs")
        b.build(["./gradlew clean", "./gradlew assemble"])

        calls = [
            # Actual call
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; ./gradlew clean >> logs/build.log 2>&1"),
            # Return value check
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; ./gradlew assemble >> logs/build.log 2>&1"),
        ]

        mock_run_subprocess.assert_has_calls(calls)

    @mock.patch("esrally.utils.process.run_subprocess")
    @mock.patch("esrally.utils.jvm.resolve_path")
    def test_build_on_jdk_10(self, jvm_resolve_path, mock_run_subprocess):
        jvm_resolve_path.return_value = (10, "/opt/jdk10")
        mock_run_subprocess.return_value = False

        b = supplier.Builder(src_dir="/src", build_jdk=8, log_dir="logs")
        b.build(["./gradlew clean", "./gradlew assemble"])

        calls = [
            # Actual call
            mock.call("export JAVA_HOME=/opt/jdk10; cd /src; ./gradlew clean >> logs/build.log 2>&1"),
            # Return value check
            mock.call("export JAVA_HOME=/opt/jdk10; cd /src; ./gradlew assemble >> logs/build.log 2>&1"),
        ]

        mock_run_subprocess.assert_has_calls(calls)


class TestDockerBuilder:
    @mock.patch("__main__.__builtins__.open")
    def test_build(self, mocked_open):
        mock_docker_client = mock.MagicMock()
        mock_docker_client.containers.run.return_value.wait.return_value = {"StatusCode": 0}
        mock_docker_client.containers.run.return_value.commit.return_value = mock.MagicMock(id="test-image")
        # Mock end of container log output
        mocked_open.side_effect = StopIteration

        user = getpass.getuser()
        group_id = os.getgid()

        builder = supplier.DockerBuilder(src_dir="/src", build_jdk=8, log_dir="logs", client=mock_docker_client)
        builder.build(["./gradlew clean", "./gradlew assemble"])

        mock_docker_client.containers.run.assert_called_with(
            detach=True,
            name="esrally-source-builder",
            user=user,
            group_add=[group_id],
            image="test-image",
            command="/bin/bash -c \"git config --global --add safe.directory '*'; ./gradlew clean; ./gradlew assemble\"",
            volumes=[f"/src:/home/{user}/elasticsearch"],
            working_dir=f"/home/{user}/elasticsearch",
        )

    @mock.patch("__main__.__builtins__.open")
    def test_run(self, mocked_open):
        mock_docker_client = mock.MagicMock()
        mock_docker_client.containers.run.return_value.wait.return_value = {"StatusCode": 0}
        mock_docker_client.containers.run.return_value.commit.return_value = mock.MagicMock(id="test-image")
        # Mock end of container log output
        mocked_open.side_effect = StopIteration

        builder = supplier.DockerBuilder(src_dir="/src", build_jdk=8, log_dir="logs", client=mock_docker_client)
        builder.run("test command")

        mock_docker_client.containers.get.assert_has_calls(
            [
                mock.call(builder.build_container_name),
                mock.call().stop(),
                mock.call().remove,
                mock.call(builder.image_builder_container_name),
                mock.call().stop(),
                mock.call().remove,
            ]
        )

        # 'run()' first builds the image, then executes source build command container
        mock_docker_client.containers.run.assert_has_calls(
            [
                mock.call(
                    detach=True,
                    name=builder.image_builder_container_name,
                    image="openjdk:8-jdk-buster",
                    command=(
                        '/bin/bash -c "for i in {1..5}; do apt update -y; apt install git -y && break || sleep 15; done; '
                        f"useradd --create-home -u {builder.user_id} {builder.user_name}; "
                        f"groupadd -g {builder.group_id} {builder.group_name}; "
                        f'usermod -g {builder.group_name} {builder.user_name}"'
                    ),
                ),
                mock.call().logs(stream=True),
                mock.call().wait(),
                mock.call().commit(builder.image_name),
                mock.call(
                    detach=True,
                    name=builder.build_container_name,
                    image="test-image",
                    user=builder.user_name,
                    group_add=[builder.group_id],
                    command="/bin/bash -c \"git config --global --add safe.directory '*'; test command\"",
                    volumes=[f"/src:/home/{builder.user_name}/elasticsearch"],
                    working_dir=f"/home/{builder.user_name}/elasticsearch",
                ),
                mock.call().logs(stream=True),
                mock.call().wait(),
            ]
        )

    @mock.patch("__main__.__builtins__.open")
    def test_create_base_container_image(self, mocked_open):
        mock_docker_client = mock.MagicMock()
        mocked_run = mock.MagicMock()
        mock_docker_client.containers.run.return_value = mocked_run

        mocked_run.wait.return_value = {"StatusCode": 0}
        # Mock end of container log output
        mocked_open.side_effect = StopIteration

        builder = supplier.DockerBuilder(src_dir="/src", build_jdk=8, log_dir="logs", client=mock_docker_client)
        builder.create_base_container_image()

        mock_docker_client.containers.run.assert_called_with(
            detach=True,
            name=builder.image_builder_container_name,
            image="openjdk:8-jdk-buster",
            command=(
                '/bin/bash -c "for i in {1..5}; do apt update -y; apt install git -y && break || sleep 15; done; '
                f"useradd --create-home -u {builder.user_id} {builder.user_name}; "
                f"groupadd -g {builder.group_id} {builder.group_name}; "
                f'usermod -g {builder.group_name} {builder.user_name}"'
            ),
        )

        mocked_run.commit.assert_called_with(builder.image_name)

    def test_resolve_jdk_build_container_image(self):
        mock_docker_client = mock.MagicMock()
        builder = supplier.DockerBuilder(src_dir="/src", build_jdk=8, log_dir="logs", client=mock_docker_client)
        # Temurin only maintains images for JDK 11, 17, 18 (at time of writing)
        # older builds of ES may require specifics like JDK 14 etc, in which case we rely on the old OpenJDK images
        image = builder.resolve_jdk_build_container_image(18)
        assert image == "eclipse-temurin:18-jdk-jammy"

        image = builder.resolve_jdk_build_container_image(17)
        assert image == "eclipse-temurin:17-jdk-jammy"

        image = builder.resolve_jdk_build_container_image(16)
        assert image == "openjdk:16-jdk-buster"

        image = builder.resolve_jdk_build_container_image(11)
        assert image == "openjdk:11-jdk-buster"

        image = builder.resolve_jdk_build_container_image(7)
        assert image != "openjdk:11-jdk-buster"
        # There is no Debian based OpenJDK JDK 12 image
        image = builder.resolve_jdk_build_container_image(12)
        assert image == "adoptopenjdk/openjdk12:debian"

    def test_stop_containers(self):
        mock_docker_client = mock.MagicMock()
        builder = supplier.DockerBuilder(src_dir="/src", build_jdk=8, log_dir="logs", client=mock_docker_client)

        container_names = ["test-container-1", "test-container-2"]
        builder.stop_containers(container_names)
        mock_docker_client.containers.get.assert_has_calls(
            [
                mock.call("test-container-1"),
                mock.call().stop(),
                mock.call().remove,
                mock.call("test-container-2"),
                mock.call().stop(),
                mock.call().remove,
            ]
        )

    def test_tail_container_logs(self):
        mock_docker_client = mock.MagicMock()
        builder = supplier.DockerBuilder(src_dir="/src", build_jdk=8, log_dir="logs", client=mock_docker_client)

        text = b"This is docker log text"

        def docker_log_output(text):
            yield text

        with mock.patch("builtins.open", mock.mock_open()) as file:
            builder.tail_container_logs(output=docker_log_output(text), container_name="test-container-name")

        file.assert_called_with("logs/build.log", "a+", encoding="utf-8")
        file.return_value.write.assert_called_with(text.decode("utf-8"))

    def test_check_container_return_code(self, caplog):
        mock_docker_client = mock.MagicMock()
        builder = supplier.DockerBuilder(src_dir="/src", build_jdk=8, log_dir="logs", client=mock_docker_client)
        completion = {"StatusCode": 0, "Error": None}
        builder.check_container_return_code(completion, "test-container-name")
        assert "Container [test-container-name] completed successfully." in caplog.text

        completion = {"StatusCode": 1, "Error": "Vague error message"}
        with mock.patch("builtins.open", mock.mock_open()):
            with pytest.raises(exceptions.BuildError) as e:
                builder.check_container_return_code(completion, "test-container-name")
            assert (
                "Docker container [test-container-name] failed with status code [1]: Error [Vague error message]: "
                "Build log output [Executing 'test-container-name' failed. The last 20 lines in the build.log file are" in str(e.value)
            )


class TestTemplateRenderer:
    def test_uses_provided_values(self):
        renderer = supplier.TemplateRenderer(version="1.2.3", os_name="Windows", arch="arm7")
        assert (
            renderer.render("This is version {{VERSION}} on {{OSNAME}} with a {{ARCH}} CPU.")
            == "This is version 1.2.3 on Windows with a arm7 CPU."
        )

    @mock.patch("esrally.utils.sysstats.os_name", return_value="Linux")
    @mock.patch("esrally.utils.sysstats.cpu_arch", return_value="X86_64")
    def test_uses_derived_values(self, os_name, cpu_arch):
        renderer = supplier.TemplateRenderer(version="1.2.3")
        assert (
            renderer.render("This is version {{VERSION}} on {{OSNAME}} with a {{ARCH}} CPU.")
            == "This is version 1.2.3 on linux with a x86_64 CPU."
        )

    @mock.patch("esrally.utils.sysstats.os_name", return_value="Darwin")
    @mock.patch("esrally.utils.sysstats.cpu_arch", return_value="arm64")
    def test_converts_arm_architecture(self, os_name, cpu_arch):
        renderer = supplier.TemplateRenderer(version="7.16.0")
        assert (
            renderer.render("This is version {{VERSION}} on {{OSNAME}} with a {{ARCH}} CPU.")
            == "This is version 7.16.0 on darwin with a aarch64 CPU."
        )


class TestCachedElasticsearchSourceSupplier:
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.copy")
    @mock.patch("esrally.mechanic.supplier.ElasticsearchSourceSupplier")
    def test_does_not_cache_when_no_revision(self, es, copy, ensure_dir):
        def add_es_artifact(binaries):
            binaries["elasticsearch"] = "/path/to/artifact.tar.gz"

        es.fetch.return_value = None
        es.add.side_effect = add_es_artifact

        # no version / revision provided
        renderer = supplier.TemplateRenderer(version=None, os_name="linux", arch="x86_64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url": "https://elstc.co/elasticsearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
        }
        file_resolver = supplier.ElasticsearchFileNameResolver(distribution_config=dist_cfg, template_renderer=renderer)
        cached_supplier = supplier.CachedSourceSupplier(distributions_root="/tmp", source_supplier=es, file_resolver=file_resolver)

        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)

        assert copy.call_count == 0
        assert not cached_supplier.cached
        assert binaries["elasticsearch"] == "/path/to/artifact.tar.gz"

    @mock.patch("os.path.exists")
    @mock.patch("esrally.mechanic.supplier.ElasticsearchSourceSupplier")
    def test_uses_already_cached_artifact(self, es, path_exists):
        # assume that the artifact is already cached
        path_exists.return_value = True
        renderer = supplier.TemplateRenderer(version="abc123", os_name="linux", arch="x86_64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url": "https://elstc.co/elasticsearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
        }
        file_resolver = supplier.ElasticsearchFileNameResolver(distribution_config=dist_cfg, template_renderer=renderer)
        cached_supplier = supplier.CachedSourceSupplier(distributions_root="/tmp", source_supplier=es, file_resolver=file_resolver)

        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)

        assert es.fetch.call_count == 0
        assert es.prepare.call_count == 0
        assert es.add.call_count == 0
        assert cached_supplier.cached
        assert binaries["elasticsearch"] == "/tmp/elasticsearch-abc123-linux-x86_64.tar.gz"

    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.exists")
    @mock.patch("shutil.copy")
    @mock.patch("esrally.mechanic.supplier.ElasticsearchSourceSupplier")
    def test_caches_artifact(self, es, copy, path_exists, ensure_dir):
        def add_es_artifact(binaries):
            binaries["elasticsearch"] = "/path/to/artifact.tar.gz"

        path_exists.return_value = False

        es.fetch.return_value = "abc123"
        es.add.side_effect = add_es_artifact

        renderer = supplier.TemplateRenderer(version="abc123", os_name="linux", arch="x86_64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url": "https://elstc.co/elasticsearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
        }

        cached_supplier = supplier.CachedSourceSupplier(
            distributions_root="/tmp",
            source_supplier=es,
            file_resolver=supplier.ElasticsearchFileNameResolver(distribution_config=dist_cfg, template_renderer=renderer),
        )
        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)
        # path is cached now
        path_exists.return_value = True

        assert copy.call_count == 1, "artifact has been copied"
        assert es.add.call_count == 1, "artifact has been added by internal supplier"
        assert cached_supplier.cached
        assert "elasticsearch" in binaries

        # simulate a second attempt
        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}
        cached_supplier.add(binaries)

        assert copy.call_count == 1, "artifact has not been copied twice"
        # the internal supplier did not get called again as we reuse the cached artifact
        assert es.add.call_count == 1, "internal supplier is not called again"
        assert cached_supplier.cached

    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.exists")
    @mock.patch("shutil.copy")
    @mock.patch("esrally.mechanic.supplier.ElasticsearchSourceSupplier")
    def test_does_not_cache_on_copy_error(self, es, copy, path_exists, ensure_dir):
        def add_es_artifact(binaries):
            binaries["elasticsearch"] = "/path/to/artifact.tar.gz"

        path_exists.return_value = False

        es.fetch.return_value = "abc123"
        es.add.side_effect = add_es_artifact
        copy.side_effect = OSError("no space left on device")

        renderer = supplier.TemplateRenderer(version="abc123", os_name="linux", arch="x86_64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url": "https://elstc.co/elasticsearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
        }

        cached_supplier = supplier.CachedSourceSupplier(
            distributions_root="/tmp",
            source_supplier=es,
            file_resolver=supplier.ElasticsearchFileNameResolver(distribution_config=dist_cfg, template_renderer=renderer),
        )
        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)

        assert copy.call_count == 1, "artifact has been copied"
        assert es.add.call_count == 1, "artifact has been added by internal supplier"
        assert not cached_supplier.cached
        # still the uncached artifact
        assert binaries["elasticsearch"] == "/path/to/artifact.tar.gz"


class TestElasticsearchFileNameResolver:
    @classmethod
    def setup_class(cls):
        renderer = supplier.TemplateRenderer(version="8.0.0-SNAPSHOT", os_name="linux", arch="x86_64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url": "https://elstc.co/elasticsearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
        }

        cls.resolver = supplier.ElasticsearchFileNameResolver(distribution_config=dist_cfg, template_renderer=renderer)

    def test_resolve(self):
        self.resolver.revision = "abc123"
        assert self.resolver.file_name == "elasticsearch-abc123-linux-x86_64.tar.gz"

    def test_artifact_key(self):
        assert self.resolver.artifact_key == "elasticsearch"

    def test_to_artifact_path(self):
        file_system_path = "/tmp/test"
        assert self.resolver.to_artifact_path(file_system_path) == file_system_path

    def test_to_file_system_path(self):
        artifact_path = "/tmp/test"
        assert self.resolver.to_file_system_path(artifact_path) == artifact_path


class TestPluginFileNameResolver:
    @classmethod
    def setup_class(cls):
        cls.resolver = supplier.PluginFileNameResolver("test-plugin")

    def test_resolve(self):
        self.resolver.revision = "abc123"
        assert self.resolver.file_name == "test-plugin-abc123.zip"

    def test_artifact_key(self):
        assert self.resolver.artifact_key == "test-plugin"

    def test_to_artifact_path(self):
        file_system_path = "/tmp/test"
        assert self.resolver.to_artifact_path(file_system_path) == f"file://{file_system_path}"

    def test_to_file_system_path(self):
        file_system_path = "/tmp/test"
        assert self.resolver.to_file_system_path(f"file://{file_system_path}") == file_system_path


class TestPrune:
    LStat = collections.namedtuple("LStat", "st_ctime")

    @mock.patch("os.path.exists")
    @mock.patch("os.listdir")
    @mock.patch("os.path.isfile")
    @mock.patch("os.lstat")
    @mock.patch("os.remove")
    def test_does_not_touch_nonexisting_directory(self, rm, lstat, isfile, listdir, exists):
        exists.return_value = False

        supplier._prune(root_path="/tmp/test", max_age_days=7)

        assert listdir.call_count == 0, "attempted to list a non-existing directory"

    @mock.patch("os.path.exists")
    @mock.patch("os.listdir")
    @mock.patch("os.path.isfile")
    @mock.patch("os.lstat")
    @mock.patch("os.remove")
    def test_prunes_old_files(self, rm, lstat, isfile, listdir, exists):
        exists.return_value = True
        listdir.return_value = ["elasticsearch-6.8.0.tar.gz", "some-subdir", "elasticsearch-7.3.0-darwin-x86_64.tar.gz"]
        isfile.side_effect = [True, False, True]

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        ten_days_ago = now - datetime.timedelta(days=10)
        one_day_ago = now - datetime.timedelta(days=1)

        lstat.side_effect = [
            # elasticsearch-6.8.0.tar.gz
            self.LStat(st_ctime=int(ten_days_ago.timestamp())),
            # elasticsearch-7.3.0-darwin-x86_64.tar.gz
            self.LStat(st_ctime=int(one_day_ago.timestamp())),
        ]

        supplier._prune(root_path="/tmp/test", max_age_days=7)

        rm.assert_called_with("/tmp/test/elasticsearch-6.8.0.tar.gz")


class TestElasticsearchSourceSupplier:
    def test_no_build(self):
        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={"clean_command": "./gradlew clean", "system.build_command": "./gradlew assemble"},
        )
        renderer = supplier.TemplateRenderer(version=None)
        es = supplier.ElasticsearchSourceSupplier(
            revision="abc", es_src_dir="/src", remote_url="", car=car, builder=None, template_renderer=renderer
        )
        es.prepare()
        # nothing has happened (intentionally) because there is no builder

    def test_build(self):
        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={"clean_command": "./gradlew clean", "system.build_command": "./gradlew assemble"},
        )
        builder = mock.create_autospec(supplier.Builder)
        renderer = supplier.TemplateRenderer(version="abc", arch="x86_64")
        es = supplier.ElasticsearchSourceSupplier(
            revision="abc", es_src_dir="/src", remote_url="", car=car, builder=builder, template_renderer=renderer
        )
        es.prepare()

        builder.build.assert_called_once_with(["./gradlew clean", "./gradlew assemble"])

    def test_build_arm(self):
        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={"clean_command": "./gradlew clean", "system.build_command.arch": "./gradlew assemble"},
        )
        builder = mock.create_autospec(supplier.Builder)
        renderer = supplier.TemplateRenderer(version="abc", arch="aarch64")
        es = supplier.ElasticsearchSourceSupplier(
            revision="abc", es_src_dir="/src", remote_url="", car=car, builder=builder, template_renderer=renderer
        )
        es.prepare()

        builder.build.assert_called_once_with(["./gradlew clean", "./gradlew assemble"])

    def test_raises_error_on_missing_car_variable(self):
        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={
                "clean_command": "./gradlew clean",
                # system.build_command is not defined
            },
        )
        renderer = supplier.TemplateRenderer(version="abc", arch="x86_64")
        builder = mock.create_autospec(supplier.Builder)
        es = supplier.ElasticsearchSourceSupplier(
            revision="abc", es_src_dir="/src", remote_url="", car=car, builder=builder, template_renderer=renderer
        )
        with pytest.raises(exceptions.SystemSetupError, match='Car "default" requires config key "system.build_command"'):
            es.prepare()

        assert builder.build.call_count == 0

    @mock.patch("glob.glob", lambda p: ["elasticsearch.tar.gz"])
    def test_add_elasticsearch_binary(self):
        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={
                "clean_command": "./gradlew clean",
                "system.build_command": "./gradlew assemble",
                "system.artifact_path_pattern": "distribution/archives/tar/build/distributions/*.tar.gz",
            },
        )
        renderer = supplier.TemplateRenderer(version="abc", arch="x86_64")
        es = supplier.ElasticsearchSourceSupplier(
            revision="abc", es_src_dir="/src", remote_url="", car=car, builder=None, template_renderer=renderer
        )
        binaries = {}
        es.add(binaries=binaries)
        assert binaries == {"elasticsearch": "elasticsearch.tar.gz"}

    @mock.patch("glob.glob", lambda p: ["elasticsearch.tar.gz"])
    def test_add_elasticsearch_binary_arm(self):
        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={
                "clean_command": "./gradlew clean",
                "system.build_command.arch": "./gradlew assemble",
                "system.artifact_path_pattern.arch": "distribution/archives/tar/build/distributions/*.tar.gz",
            },
        )
        renderer = supplier.TemplateRenderer(version="abc", arch="aarch64")
        es = supplier.ElasticsearchSourceSupplier(
            revision="abc", es_src_dir="/src", remote_url="", car=car, builder=None, template_renderer=renderer
        )
        binaries = {}
        es.add(binaries=binaries)
        assert binaries == {"elasticsearch": "elasticsearch.tar.gz"}

    def test_resolve_build_jdk_major(self, caplog):
        def text(build_java, runtime_java):
            # based off of https://github.com/elastic/elasticsearch/blob/main/.ci/java-versions.properties
            text = (
                "# This file is used with all of the non-matrix tests in Jenkins.\n"
                "\n"
                "# This .properties file defines the versions of Java with which to\n"
                "# build and test Elasticsearch for this branch. Valid Java versions\n"
                "# are 'java' or 'openjdk' followed by the major release number.\n"
                "\n"
                f"ES_BUILD_JAVA={build_java}\n"
                f"ES_RUNTIME_JAVA={runtime_java}\n"
            )

            return text

        test_file_jdk18 = text("openjdk18", "openjdk18")
        with mock.patch("builtins.open", mock.mock_open(read_data=test_file_jdk18)):
            major_ver = supplier.ElasticsearchSourceSupplier.resolve_build_jdk_major("test")
            assert major_ver == 18

        test_file_jdk12 = text("openjdk12", "java8")
        with mock.patch("builtins.open", mock.mock_open(read_data=test_file_jdk12)):
            major_ver = supplier.ElasticsearchSourceSupplier.resolve_build_jdk_major("test")
            assert major_ver == 12

        test_file_jdk_11 = text("java11", "java8")

        with mock.patch("builtins.open", mock.mock_open(read_data=test_file_jdk_11)):
            major_ver = supplier.ElasticsearchSourceSupplier.resolve_build_jdk_major("test")
            assert major_ver == 11

        test_file_no_jdk = (
            "# This file is used with all of the non-matrix tests in Jenkins.\n"
            "\n"
            "# This .properties file defines the versions of Java with which to\n"
            "# build and test Elasticsearch for this branch. Valid Java versions\n"
            "# are 'java' or 'openjdk' followed by the major release number.\n"
            "\n"
        )

        with mock.patch("builtins.open", mock.mock_open(read_data=test_file_no_jdk)):
            major_ver = supplier.ElasticsearchSourceSupplier.resolve_build_jdk_major("test")
            assert major_ver == 17
            assert "Unable to resolve build JDK major release version. Defaulting to version [17]." in caplog.text

        with mock.patch("builtins.open", mock.mock_open()) as mocked_open:
            mocked_open.side_effect = FileNotFoundError
            major_ver = supplier.ElasticsearchSourceSupplier.resolve_build_jdk_major("test")
            assert "File [test/.ci/java-versions.properties] not found." in caplog.text
            assert "Unable to resolve build JDK major release version. Defaulting to version [17]." in caplog.text
            assert major_ver == 17


class TestExternalPluginSourceSupplier:
    def setup_method(self, method):
        self.along_es = supplier.ExternalPluginSourceSupplier(
            plugin=team.PluginDescriptor("some-plugin", core_plugin=False),
            revision="abc",
            # built along-side ES
            src_dir="/src",
            src_config={
                "plugin.some-plugin.src.subdir": "elasticsearch-extra/some-plugin",
                "plugin.some-plugin.build.artifact.subdir": "plugin/build/distributions",
            },
            builder=None,
        )

        self.standalone = supplier.ExternalPluginSourceSupplier(
            plugin=team.PluginDescriptor("some-plugin", core_plugin=False),
            revision="abc",
            # built separately
            src_dir=None,
            src_config={
                "plugin.some-plugin.src.dir": "/Projects/src/some-plugin",
                "plugin.some-plugin.build.artifact.subdir": "build/distributions",
            },
            builder=None,
        )

    def test_invalid_config_no_source(self):
        with pytest.raises(
            exceptions.SystemSetupError,
            match="Neither plugin.some-plugin.src.dir nor plugin.some-plugin.src.subdir are set for plugin some-plugin.",
        ):
            supplier.ExternalPluginSourceSupplier(
                plugin=team.PluginDescriptor("some-plugin", core_plugin=False),
                revision="abc",
                # built separately
                src_dir=None,
                src_config={
                    # but no source config
                    # "plugin.some-plugin.src.dir": "/Projects/src/some-plugin",
                    "plugin.some-plugin.build.artifact.subdir": "build/distributions"
                },
                builder=None,
            )

    def test_invalid_config_duplicate_source(self):
        with pytest.raises(
            exceptions.SystemSetupError,
            match="Can only specify one of plugin.duplicate.src.dir and plugin.duplicate.src.subdir but both are set.",
        ):
            supplier.ExternalPluginSourceSupplier(
                plugin=team.PluginDescriptor("duplicate", core_plugin=False),
                revision="abc",
                src_dir=None,
                src_config={
                    "plugin.duplicate.src.subdir": "elasticsearch-extra/some-plugin",
                    "plugin.duplicate.src.dir": "/Projects/src/some-plugin",
                    "plugin.duplicate.build.artifact.subdir": "build/distributions",
                },
                builder=None,
            )

    def test_standalone_plugin_overrides_build_dir(self):
        assert self.standalone.override_build_dir == "/Projects/src/some-plugin"

    def test_along_es_plugin_keeps_build_dir(self):
        assert self.along_es.override_build_dir is None

    @mock.patch("glob.glob", lambda p: ["/src/elasticsearch-extra/some-plugin/plugin/build/distributions/some-plugin.zip"])
    def test_add_binary_built_along_elasticsearch(self):
        binaries = {}
        self.along_es.add(binaries)
        assert binaries == {
            "some-plugin": "file:///src/elasticsearch-extra/some-plugin/plugin/build/distributions/some-plugin.zip",
        }

    @mock.patch("glob.glob", lambda p: ["/Projects/src/some-plugin/build/distributions/some-plugin.zip"])
    def test_resolve_plugin_binary_built_standalone(self):
        binaries = {}
        self.along_es.add(binaries)
        assert binaries == {
            "some-plugin": "file:///Projects/src/some-plugin/build/distributions/some-plugin.zip",
        }


class TestCorePluginSourceSupplier:
    @mock.patch("glob.glob", lambda p: ["/src/elasticsearch/core-plugin/build/distributions/core-plugin.zip"])
    def test_resolve_plugin_binary(self):
        s = supplier.CorePluginSourceSupplier(
            plugin=team.PluginDescriptor("core-plugin", core_plugin=True),
            # built separately
            es_src_dir="/src/elasticsearch",
            builder=None,
        )
        binaries = {}
        s.add(binaries)
        assert binaries == {
            "core-plugin": "file:///src/elasticsearch/core-plugin/build/distributions/core-plugin.zip",
        }


class TestPluginDistributionSupplier:
    def test_resolve_plugin_url(self):
        v = {"plugin_custom-analyzer_release_url": "http://example.org/elasticearch/custom-analyzer-{{VERSION}}.zip"}
        renderer = supplier.TemplateRenderer(version="6.3.0")
        s = supplier.PluginDistributionSupplier(
            repo=supplier.DistributionRepository(name="release", distribution_config=v, template_renderer=renderer),
            plugin=team.PluginDescriptor("custom-analyzer"),
        )
        binaries = {}
        s.add(binaries)
        assert binaries == {
            "custom-analyzer": "http://example.org/elasticearch/custom-analyzer-6.3.0.zip",
        }


class TestCreateSupplier:
    def test_derive_supply_requirements_es_source_build(self):
        # corresponds to --revision="abc"
        requirements = supplier._supply_requirements(
            sources=True, distribution=False, plugins=[], revisions={"elasticsearch": "abc"}, distribution_version=None
        )
        assert requirements == {"elasticsearch": ("source", "abc", True)}

    def test_derive_supply_requirements_es_distribution(self):
        # corresponds to --distribution-version=6.0.0
        requirements = supplier._supply_requirements(
            sources=False, distribution=True, plugins=[], revisions={}, distribution_version="6.0.0"
        )
        assert requirements == {"elasticsearch": ("distribution", "6.0.0", False)}

    def test_derive_supply_requirements_es_and_plugin_source_build(self):
        # corresponds to --revision="elasticsearch:abc,community-plugin:effab"
        core_plugin = team.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = team.PluginDescriptor("community-plugin", core_plugin=False)

        requirements = supplier._supply_requirements(
            sources=True,
            distribution=False,
            plugins=[core_plugin, external_plugin],
            revisions={"elasticsearch": "abc", "all": "abc", "community-plugin": "effab"},
            distribution_version=None,
        )
        assert requirements == {
            "elasticsearch": ("source", "abc", True),
            # core plugin configuration is forced to be derived from ES
            "analysis-icu": ("source", "abc", True),
            "community-plugin": ("source", "effab", True),
        }

    def test_derive_supply_requirements_es_distribution_and_plugin_source_build(self):
        # corresponds to --revision="community-plugin:effab" --distribution-version="6.0.0"
        core_plugin = team.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = team.PluginDescriptor("community-plugin", core_plugin=False)

        requirements = supplier._supply_requirements(
            sources=False,
            distribution=True,
            plugins=[core_plugin, external_plugin],
            revisions={"community-plugin": "effab"},
            distribution_version="6.0.0",
        )
        # core plugin is not contained, its configured is forced to be derived by ES
        assert requirements == {
            "elasticsearch": ("distribution", "6.0.0", False),
            # core plugin configuration is forced to be derived from ES
            "analysis-icu": ("distribution", "6.0.0", False),
            "community-plugin": ("source", "effab", True),
        }

    def test_create_suppliers_for_es_only_config(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "distribution.version", "6.0.0")
        # default value from command line
        cfg.add(config.Scope.application, "mechanic", "source.revision", "current")
        cfg.add(config.Scope.application, "mechanic", "distribution.repository", "release")
        cfg.add(
            config.Scope.application,
            "distributions",
            "release.url",
            "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
        )
        cfg.add(config.Scope.application, "distributions", "release.cache", True)
        cfg.add(config.Scope.application, "node", "root.dir", "/opt/rally")

        car = team.Car("default", root_path=None, config_paths=[])

        composite_supplier = supplier.create(cfg, sources=False, distribution=True, car=car)

        assert len(composite_supplier.suppliers) == 1
        assert isinstance(composite_supplier.suppliers[0], supplier.ElasticsearchDistributionSupplier)

    @mock.patch("esrally.utils.jvm.resolve_path", lambda v: (v, f"/opt/java/java{v}"))
    def test_create_suppliers_for_es_distribution_plugin_source_build(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "distribution.version", "6.0.0")
        # default value from command line
        cfg.add(config.Scope.application, "mechanic", "source.revision", "community-plugin:current")
        cfg.add(config.Scope.application, "mechanic", "distribution.repository", "release")
        cfg.add(
            config.Scope.application,
            "distributions",
            "release.url",
            "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
        )
        cfg.add(config.Scope.application, "distributions", "release.cache", True)
        cfg.add(config.Scope.application, "node", "root.dir", "/opt/rally")
        cfg.add(config.Scope.application, "node", "src.root.dir", "/opt/rally/src")
        cfg.add(config.Scope.application, "source", "elasticsearch.src.subdir", "elasticsearch")
        cfg.add(config.Scope.application, "source", "plugin.community-plugin.src.dir", "/home/user/Projects/community-plugin")

        car = team.Car("default", root_path=None, config_paths=[], variables={"build.jdk": "10"})
        core_plugin = team.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = team.PluginDescriptor("community-plugin", core_plugin=False)

        # --revision="community-plugin:effab" --distribution-version="6.0.0"
        composite_supplier = supplier.create(cfg, sources=False, distribution=True, car=car, plugins=[core_plugin, external_plugin])

        assert len(composite_supplier.suppliers) == 3
        assert isinstance(composite_supplier.suppliers[0], supplier.ElasticsearchDistributionSupplier)
        assert isinstance(composite_supplier.suppliers[1], supplier.PluginDistributionSupplier)
        assert composite_supplier.suppliers[1].plugin == core_plugin
        assert isinstance(composite_supplier.suppliers[2].source_supplier, supplier.ExternalPluginSourceSupplier)
        assert composite_supplier.suppliers[2].source_supplier.plugin == external_plugin
        assert composite_supplier.suppliers[2].source_supplier.builder is not None

    @mock.patch("esrally.utils.jvm.resolve_path", lambda v: (v, f"/opt/java/java{v}"))
    def test_create_suppliers_skips_plugins_converted_to_modules(self, caplog):
        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "source.revision", "current")
        cfg.add(config.Scope.application, "node", "root.dir", "/opt/rally")
        cfg.add(config.Scope.application, "node", "src.root.dir", "/opt/rally/src")
        cfg.add(config.Scope.application, "source", "elasticsearch.src.subdir", "elasticsearch")
        cfg.add(config.Scope.application, "source", "remote.repo.url", "https://github.com/elastic/elasticsearch.git")
        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={"clean_command": "./gradlew clean", "build_command": "./gradlew assemble", "build.jdk": "11"},
        )

        plugins_moved_to_modules = ["repository-s3", "repository-gcs", "repository-azure"]
        plugins = []
        for p in plugins_moved_to_modules:
            plugins.append(team.PluginDescriptor(p, core_plugin=False))

        composite_supplier = supplier.create(cfg, sources=True, distribution=False, car=car, plugins=plugins)

        for p in plugins:
            assert f"Plugin [{p.name}] is now an Elasticsearch module and no longer needs to be built from source." in caplog.messages
            assert p not in composite_supplier.suppliers

    @mock.patch("esrally.utils.jvm.resolve_path", lambda v: (v, f"/opt/java/java{v}"))
    def test_create_suppliers_for_es_and_plugin_source_build(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "source.revision", "elasticsearch:abc,community-plugin:current")
        cfg.add(config.Scope.application, "mechanic", "distribution.repository", "release")
        cfg.add(
            config.Scope.application,
            "distributions",
            "release.url",
            "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
        )
        cfg.add(config.Scope.application, "distributions", "release.cache", True)
        cfg.add(config.Scope.application, "node", "root.dir", "/opt/rally")
        cfg.add(config.Scope.application, "node", "src.root.dir", "/opt/rally/src")
        cfg.add(config.Scope.application, "source", "elasticsearch.src.subdir", "elasticsearch")
        cfg.add(config.Scope.application, "source", "remote.repo.url", "https://github.com/elastic/elasticsearch.git")
        cfg.add(config.Scope.application, "source", "plugin.community-plugin.src.subdir", "elasticsearch-extra/community-plugin")

        car = team.Car(
            "default",
            root_path=None,
            config_paths=[],
            variables={"clean_command": "./gradlew clean", "build_command": "./gradlew assemble", "build.jdk": "11"},
        )
        core_plugin = team.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = team.PluginDescriptor("community-plugin", core_plugin=False)

        # --revision="elasticsearch:abc,community-plugin:effab"
        composite_supplier = supplier.create(cfg, sources=True, distribution=False, car=car, plugins=[core_plugin, external_plugin])

        assert len(composite_supplier.suppliers) == 3
        assert isinstance(composite_supplier.suppliers[0].source_supplier, supplier.ElasticsearchSourceSupplier)
        assert isinstance(composite_supplier.suppliers[1].source_supplier, supplier.CorePluginSourceSupplier)
        assert composite_supplier.suppliers[1].source_supplier.plugin == core_plugin
        assert isinstance(composite_supplier.suppliers[2].source_supplier, supplier.ExternalPluginSourceSupplier)
        assert composite_supplier.suppliers[2].source_supplier.plugin == external_plugin
        assert composite_supplier.suppliers[2].source_supplier.builder is not None


class TestDistributionRepository:
    @mock.patch("esrally.utils.sysstats.os_name", return_value="Linux")
    @mock.patch("esrally.utils.sysstats.cpu_arch", return_value="X86_64")
    def test_release_repo_config_with_default_url(self, os_name, cpu_arch):
        renderer = supplier.TemplateRenderer(version="7.3.2")
        repo = supplier.DistributionRepository(
            name="release",
            distribution_config={
                "runtime.jdk.bundled": "true",
                "jdk.bundled.release_url": (
                    "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz"
                ),
                "release.cache": "true",
            },
            template_renderer=renderer,
        )
        assert repo.download_url == "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.3.2-linux-x86_64.tar.gz"
        assert repo.file_name == "elasticsearch-7.3.2-linux-x86_64.tar.gz"
        assert repo.cache

    def test_release_repo_config_with_user_url(self):
        renderer = supplier.TemplateRenderer(version="2.4.3")
        repo = supplier.DistributionRepository(
            name="release",
            distribution_config={
                "jdk.unbundled.release_url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                "runtime.jdk.bundled": "false",
                # user override
                "release.url": "https://es-mirror.example.org/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                "release.cache": "false",
            },
            template_renderer=renderer,
        )
        assert repo.download_url == "https://es-mirror.example.org/downloads/elasticsearch/elasticsearch-2.4.3.tar.gz"
        assert repo.file_name == "elasticsearch-2.4.3.tar.gz"
        assert not repo.cache

    def test_missing_url(self):
        renderer = supplier.TemplateRenderer(version="2.4.3")
        repo = supplier.DistributionRepository(
            name="miss",
            distribution_config={
                "jdk.unbundled.release_url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                "runtime.jdk.bundled": "false",
                "release.cache": "true",
            },
            template_renderer=renderer,
        )
        with pytest.raises(exceptions.SystemSetupError) as exc:
            # pylint: disable=pointless-statement
            # noinspection PyStatementEffect
            repo.download_url
        assert exc.value.args[0] == "Neither config key [miss.url] nor [jdk.unbundled.miss_url] is defined."

    def test_missing_cache(self):
        renderer = supplier.TemplateRenderer(version="2.4.3")
        repo = supplier.DistributionRepository(
            name="release",
            distribution_config={
                "jdk.unbundled.release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                "runtime.jdk.bundled": "false",
            },
            template_renderer=renderer,
        )
        with pytest.raises(exceptions.SystemSetupError) as exc:
            # pylint: disable=pointless-statement
            # noinspection PyStatementEffect
            repo.cache
        assert exc.value.args[0] == "Mandatory config key [release.cache] is undefined."

    def test_invalid_cache_value(self):
        renderer = supplier.TemplateRenderer(version="2.4.3")
        repo = supplier.DistributionRepository(
            name="release",
            distribution_config={
                "jdk.unbundled.release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                "runtime.jdk.bundled": "false",
                "release.cache": "Invalid",
            },
            template_renderer=renderer,
        )
        with pytest.raises(exceptions.SystemSetupError) as exc:
            # pylint: disable=pointless-statement
            # noinspection PyStatementEffect
            repo.cache
        assert exc.value.args[0] == "Value [Invalid] for config key [release.cache] is not a valid boolean value."

    def test_plugin_config_with_default_url(self):
        renderer = supplier.TemplateRenderer(version="5.5.0")
        repo = supplier.DistributionRepository(
            name="release",
            distribution_config={
                "runtime.jdk.bundled": "false",
                "plugin_example_release_url": "https://artifacts.example.org/downloads/plugins/example-{{VERSION}}.zip",
            },
            template_renderer=renderer,
        )
        assert repo.plugin_download_url("example") == "https://artifacts.example.org/downloads/plugins/example-5.5.0.zip"

    def test_plugin_config_with_user_url(self):
        renderer = supplier.TemplateRenderer(version="5.5.0")
        repo = supplier.DistributionRepository(
            name="release",
            distribution_config={
                "runtime.jdk.bundled": "false",
                "plugin_example_release_url": "https://artifacts.example.org/downloads/plugins/example-{{VERSION}}.zip",
                # user override
                "plugin.example.release.url": "https://mirror.example.org/downloads/plugins/example-{{VERSION}}.zip",
            },
            template_renderer=renderer,
        )
        assert repo.plugin_download_url("example") == "https://mirror.example.org/downloads/plugins/example-5.5.0.zip"

    def test_missing_plugin_config(self):
        renderer = supplier.TemplateRenderer(version="5.5.0")
        repo = supplier.DistributionRepository(
            name="release",
            distribution_config={
                "runtime.jdk.bundled": "false",
            },
            template_renderer=renderer,
        )
        assert repo.plugin_download_url("not existing") is None
