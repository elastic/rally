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

import importlib.util
import logging
import os
import sys
from collections.abc import Collection, Iterator
from types import ModuleType
from typing import Tuple, Union

from esrally import exceptions
from esrally.utils import io


class ComponentLoader:
    """
    Loads a dynamically defined component. A component in this terminology is any piece of code that is not part of the Rally core code base
    but extends it. Examples include custom runners or parameter sources for tracks or install hooks for Elasticsearch plugins.

    A component has always a well-defined entry point. This is the "main" Python file (e.g. ``track.py`` for tracks or ``plugin.py`` for
    install hooks. A component may also consist of multiple Python modules.

    """

    def __init__(self, root_path: Union[str, Collection[str]], component_entry_point: str, recurse: bool = True):
        """
        Creates a new component loader.

        :param root_path: An absolute path or list of paths to a directory which contains the component entry point.
        :param component_entry_point: The name of the component entry point. A corresponding file with the extension ".py" must exist in the
        ``root_path``.
        :param recurse: Search recursively for modules but ignore modules starting with "_" (Default: ``True``).
        """
        self.root_path: Collection[str] = root_path if isinstance(root_path, list) else [str(root_path)]
        self.component_entry_point = component_entry_point
        self.recurse = recurse
        self.logger = logging.getLogger(__name__)

    def _modules(self, module_paths: Collection[str], component_name: str, root_path: str) -> Iterator[tuple[str, str]]:
        for path in module_paths:
            for filename in os.listdir(path):
                name, ext = os.path.splitext(filename)
                if ext.endswith(".py"):
                    file_absolute_path = os.path.join(path, filename)
                    root_absolute_path = os.path.join(path, name)
                    root_relative_path = root_absolute_path[len(root_path) + len(os.path.sep) :]
                    module_name = "%s.%s" % (component_name, root_relative_path.replace(os.path.sep, "."))
                    yield module_name, file_absolute_path

    def _load_component(self, component_name: str, module_dirs: Collection[str], root_path: str) -> ModuleType:
        # precondition: A module with this name has to exist provided that the caller has called #can_load() before.
        root_module_name = "%s.%s" % (component_name, self.component_entry_point)
        for name, p in self._modules(module_dirs, component_name, root_path):
            self.logger.debug("Loading module [%s]: %s", name, p)
            # Use the util methods instead of `importlib.import_module` to allow for more fine-grained control over the import process.
            # in particular, we want to be able to import multiple modules that use the same name, but are from different directories.
            spec = importlib.util.spec_from_file_location(name, p)
            if spec is None or spec.loader is None:
                raise exceptions.SystemSetupError(f"Could not load module [{name}]")
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            if name == root_module_name:
                root_module = m
        return root_module

    def can_load(self):
        """
        :return: True iff the component entry point could be found.
        """
        return all(self.root_path) and all(
            os.path.exists(os.path.join(root_path, "%s.py" % self.component_entry_point)) for root_path in self.root_path
        )

    def load(self) -> Collection[ModuleType]:
        """
        Loads components with the given component entry point.

        Precondition: ``ComponentLoader#can_load() == True``.

        :return: The root modules.
        """
        root_modules = []
        for root_path in self.root_path:
            component_name = io.basename(root_path)
            self.logger.info("Loading component [%s] from [%s]", component_name, root_path)
            module_dirs = []
            # search all paths within this directory for modules but exclude all directories starting with "_"
            if self.recurse:
                for dirpath, dirs, _ in os.walk(root_path):
                    module_dirs.append(dirpath)
                    ignore = []
                    for d in dirs:
                        if d.startswith("_"):
                            self.logger.debug("Removing [%s] from load path.", d)
                            ignore.append(d)
                    for d in ignore:
                        dirs.remove(d)
            else:
                module_dirs.append(root_path)
            # load path is only the root of the package hierarchy
            component_root_path = os.path.abspath(os.path.join(root_path, os.pardir))
            self.logger.debug("Adding [%s] to Python load path.", component_root_path)
            # needs to be at the beginning of the system path, otherwise import machinery tries to load application-internal modules
            sys.path.insert(0, component_root_path)
            try:
                root_module = self._load_component(component_name, module_dirs, root_path)
                root_modules.append(root_module)
            except BaseException:
                msg = f"Could not load component [{component_name}]"
                self.logger.exception(msg)
                raise exceptions.SystemSetupError(msg)
        return root_modules
