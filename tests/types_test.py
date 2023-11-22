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

from configparser import ConfigParser
from importlib import import_module
from inspect import isclass, getsourcelines, signature
from os.path import sep
from pathlib import Path as _Path
from types import FunctionType
from typing import Optional, get_args

from esrally import types


class Path(_Path):
    # populates _flavour manually because Path.__new__() doesn't for subclasses
    _flavour = _Path()._flavour

    def glob_modules(self, pattern, *args, **kwargs):
        for file in self.glob(pattern, *args, **kwargs):
            if not file.match("*.py"):
                continue
            pyfile = file.relative_to(self)
            modpath = pyfile.parent if pyfile.name == "__init__.py" else pyfile.with_suffix("")
            yield import_module(str(modpath).replace(sep, '.'))


project_root = Path(__file__).parent / ".."


class TestLiteralArgs:
    def test_order_of_literal_args(self):
        for literal in (types.Section, types.Key):
            args = get_args(literal)
            assert tuple(args) == tuple(sorted(args)), "Literal args are not sorted"

    def test_uniqueness_of_literal_args(self):
        def _excerpt(lines, start, stop):
            """Yields lines between start and stop markers not including both ends"""
            started = False
            for line in lines:
                if not started and start in line:
                    started = True
                elif started and stop in line:
                    break
                elif started:
                    yield line

        sourcelines, _ = getsourcelines(types)
        for name in ("Section", "Key"):
            args = tuple(sorted(_excerpt(sourcelines, f"{name} = Literal[", "]")))
            assert args == tuple(sorted(set(args))), "Literal args are duplicate"

    def test_appearance_of_literal_args(self):
        args = {f'"{arg}"' for arg in get_args(types.Section) + get_args(types.Key)}

        for pyfile in project_root.glob("[!.]*/**/*.py"):
            if pyfile == project_root / "esrally/types.py":
                continue  # Should skip esrally.types module

            source = pyfile.read_text(encoding="utf-8", errors="replace")  # No need to be so strict
            for arg in args.copy():
                if arg in source:
                    args.remove(arg)  # Keep only args that have not been found in any .py files

            if not args:
                break  # No need to look at more .py files because all args are already found

        assert not args, "literal args are not found in any .py files"


def assert_params(fn, identifer, *types):
    for param in signature(fn).parameters.values():
        if identifer == param.name:
            assert param.annotation in types, f"parameter annotation of '{identifer}' is wrong at {fn.__name__}()"


def assert_return(fn, identifer, *types):
    return_annotation = signature(fn).return_annotation
    sourcelines, _ = getsourcelines(fn)
    for line in sourcelines:
        if line.endswith(f"    return {identifer}"):
            assert return_annotation in types, f"return annotation is wrong at {fn.__name__}()"


def assert_annotations(obj, identifer, *types):
    for name in dir(obj):
        if name.startswith("_"):
            continue

        attr = getattr(obj, name)
        if isclass(attr):
            try:
                if not attr.__module__.startswith(obj.__module__):
                    continue
            except AttributeError:
                if not attr.__module__.startswith(obj.__name__):
                    continue
            assert_annotations(attr, identifer, *types)
        elif type(attr) is FunctionType:
            assert_params(attr, identifer, *types)
            assert_return(attr, identifer, *types)


class TestConfigTypeHint:
    def test_esrally_module(self):
        for module in project_root.glob_modules("esrally/**/*.py"):
            assert_annotations(module, "cfg", types.Config)
            assert_annotations(module, "config", types.Config, Optional[types.Config], ConfigParser)
