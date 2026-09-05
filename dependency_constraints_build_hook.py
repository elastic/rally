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

import pathlib
import shutil
import subprocess
import tempfile

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class DependencyConstraintsBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        self._temporary_directory = pathlib.Path(tempfile.mkdtemp())
        constraints_path = self._temporary_directory / "rally-constraints.txt"
        command = [
            "uv",
            "export",
            "--frozen",
            "--format",
            "requirements.txt",
            "--no-dev",
            "--no-emit-project",
            "--no-hashes",
            "--no-annotate",
            "--no-header",
            "--output-file",
            str(constraints_path),
        ]
        try:
            subprocess.run(command, cwd=self.root, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            shutil.rmtree(self._temporary_directory)
            details = e.stderr or e.stdout
            raise RuntimeError(f"Could not generate Rally's dependency constraints: {details}") from e
        constraints = constraints_path.read_text(encoding="utf-8")
        generated_constraints = "# Generated from uv.lock by Rally's build hook.\n" + constraints
        constraints_path.write_text(generated_constraints, encoding="utf-8")
        build_data["force_include"][str(constraints_path)] = "esrally/resources/rally-constraints.txt"

    def finalize(self, version, build_data, artifact_path):
        shutil.rmtree(self._temporary_directory)
