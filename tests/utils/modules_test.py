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

import sys

import pytest

from esrally import exceptions
from esrally.utils import modules


@pytest.fixture(autouse=True)
def restore_sys_path():
    original_path = list(sys.path)
    yield
    sys.path[:] = original_path


def track_dir(root, name="unittest_track"):
    d = root / name
    d.mkdir()
    (d / "track.py").write_text("loaded = ['track']\n", encoding="utf-8")
    return d


def module_dir(root, name):
    d = root / name
    d.mkdir()
    return d


class TestComponentLoader:
    def test_loads_modules_from_nested_directories(self, tmp_path):
        d = track_dir(tmp_path)
        (module_dir(d, "parameter_sources") / "custom.py").write_text("loaded = ['custom']\n", encoding="utf-8")

        loader = modules.ComponentLoader(root_path=str(d), component_entry_point="track")

        assert loader.can_load()
        root_modules = loader.load()
        assert [m.loaded for m in root_modules] == [["track"]]

    def test_ignores_directories_starting_with_underscore(self, tmp_path):
        d = track_dir(tmp_path)
        (module_dir(d, "_tools") / "broken.py").write_text("import this_dependency_does_not_exist\n", encoding="utf-8")

        loader = modules.ComponentLoader(root_path=str(d), component_entry_point="track")

        root_modules = loader.load()
        assert [m.loaded for m in root_modules] == [["track"]]

    @pytest.mark.parametrize("test_dir_name", ["test", "tests", "test_helpers"])
    def test_ignores_test_directories(self, tmp_path, test_dir_name):
        d = track_dir(tmp_path)
        (module_dir(d, test_dir_name) / "custom_test.py").write_text("import this_dependency_does_not_exist\n", encoding="utf-8")

        loader = modules.ComponentLoader(root_path=str(d), component_entry_point="track")

        root_modules = loader.load()
        assert [m.loaded for m in root_modules] == [["track"]]

    def test_ignores_test_directories_only_at_directory_level(self, tmp_path):
        d = track_dir(tmp_path)
        (d / "test_param_source.py").write_text("import this_dependency_does_not_exist\n", encoding="utf-8")

        loader = modules.ComponentLoader(root_path=str(d), component_entry_point="track")

        with pytest.raises(exceptions.SystemSetupError, match="Could not load component \\[unittest_track\\]"):
            loader.load()

    def test_raises_error_if_a_module_cannot_be_loaded(self, tmp_path):
        d = track_dir(tmp_path)
        (module_dir(d, "parameter_sources") / "broken.py").write_text("import this_dependency_does_not_exist\n", encoding="utf-8")

        loader = modules.ComponentLoader(root_path=str(d), component_entry_point="track")

        with pytest.raises(exceptions.SystemSetupError, match="Could not load component \\[unittest_track\\]"):
            loader.load()
