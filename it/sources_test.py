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
import json

import it
from esrally.utils import process


@it.random_rally_config
def test_sources(cfg):
    port = 19200
    it.wait_until_port_is_free(port_number=port)

    # Default sources build method
    assert (
        it.race(
            cfg,
            f"--revision=latest --track=geonames --test-mode  --target-hosts=127.0.0.1:{port} "
            f"--challenge=append-no-conflicts --car=4gheap,basic-license --elasticsearch-plugins=analysis-icu",
        )
        == 0
    )

    # Default sources build method
    it.wait_until_port_is_free(port_number=port)
    assert (
        it.race(
            cfg,
            f"--pipeline=from-sources --track=geonames --test-mode --target-hosts=127.0.0.1:{port} "
            f'--challenge=append-no-conflicts-index-only --car="4gheap,basic-license,ea"',
        )
        == 0
    )

    # Docker sources build method
    assert (
        it.race(
            cfg,
            f"--revision=@2022-07-07 --track=geonames --test-mode  --target-hosts=127.0.0.1:{port} "
            f"--challenge=append-no-conflicts --car=4gheap,basic-license --elasticsearch-plugins=analysis-icu "
            f"--source-build-method=docker",
        )
        == 0
    )

    # Docker sources build method
    it.wait_until_port_is_free(port_number=port)
    assert (
        it.race(
            cfg,
            f"--pipeline=from-sources --track=geonames --test-mode --target-hosts=127.0.0.1:{port} "
            f'--source-build-method=docker --challenge=append-no-conflicts-index-only --car="4gheap,basic-license,ea"',
        )
        == 0
    )


@it.random_rally_config
def test_build_es_and_plugin_with_docker(cfg):
    assert (
        it.esrally(
            cfg,
            "build --source-build-method=docker --revision=latest --target-arch aarch64 --target-os linux "
            "--elasticsearch-plugins=analysis-icu --quiet",
        )
        == 0
    )


@it.random_rally_config
def test_build_es(cfg):
    assert (
        it.esrally(
            cfg,
            "build --revision=latest --target-arch aarch64 --target-os linux --quiet",
        )
        == 0
    )


def test_build_es_linux_aarch64_output():
    def run_command(target_os, target_arch):
        try:
            output = process.run_subprocess_with_output(
                f"esrally build --revision=latest --target-arch {target_arch} --target-os {target_os} --quiet"
            )
            print(output)
            elasticsearch = json.loads("".join(output))["elasticsearch"]
            assert f"{target_os}-{target_arch}" in elasticsearch

        except BaseException as e:
            raise AssertionError(f"Failed to build Elasticsearch for [{target_os}, {target_arch}].", e)

    run_command("linux", "aarch64")
    run_command("linux", "x86_64")
