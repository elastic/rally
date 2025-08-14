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

import builtins
import itertools
from configparser import ConfigParser
from importlib import import_module
from inspect import getsourcelines, isclass, signature
from pathlib import Path
from types import FunctionType, ModuleType
from typing import Optional, get_args

import pytest

import esrally
from esrally import types

ESRALLY_ROOT_DIR = Path(esrally.__file__).parent.parent.absolute()

ESRALLY_PYTHON_FILES = [f.relative_to(ESRALLY_ROOT_DIR) for f in ESRALLY_ROOT_DIR.glob("esrally/**/*.py") if f.match("*.py")]
TESTS_PYTHON_FILES = [f.relative_to(ESRALLY_ROOT_DIR) for f in ESRALLY_ROOT_DIR.glob("tests/**/*.py") if f.match("*.py")]


@pytest.fixture(scope="session", params=["Section", "Key"])
def literal_name(request) -> str:
    return request.param


@pytest.fixture(scope="session")
def literal_values(literal_name: str) -> list[str]:
    lines, _ = getsourcelines(types)
    start = lines.index(f"{literal_name} = Literal[\n")
    end = lines.index("]\n", start + 1)
    lines = [l.strip().replace(" ", "").rstrip(",").replace('"', "") for l in lines[start + 1 : end]]
    return lines


def test_literal_values(literal_values: list[str]):
    assert literal_values == sorted(literal_values), "literals are not sorted"
    assert literal_values == sorted(set(literal_values)), "literals have duplicate entries"


@pytest.fixture(scope="session")
def esrally_source_code() -> str:
    source_files = sorted(f for f in itertools.chain(ESRALLY_PYTHON_FILES, TESTS_PYTHON_FILES) if str(f) != "esrally/types.py")
    source_text = "\n".join((ESRALLY_ROOT_DIR / f).read_text(encoding="utf-8", errors="replace") for f in source_files)
    return source_text


@pytest.fixture(scope="session", params=get_args(types.Key))
def key_name(request) -> str:
    return request.param


def test_key_name(esrally_source_code: str, key_name: str):
    assert f'"{key_name}"' in esrally_source_code or f"'{key_name}'" in esrally_source_code, "unused key name"


@pytest.fixture(scope="session", params=get_args(types.Section))
def section_name(request) -> str:
    return request.param


def test_section_name(esrally_source_code: str, section_name: str):
    assert f'"{section_name}"' in esrally_source_code or f"'{section_name}'" in esrally_source_code, "unused session name"


def module_name(path: Path) -> str:
    if path.match("*/__init__.py"):
        path = path.parent
    else:
        path = path.with_suffix("")
    return path.as_posix().replace("/", ".")


ESRALLY_MODULE_NAMES = [module_name(f) for f in ESRALLY_PYTHON_FILES]


@pytest.fixture(scope="session", params=ESRALLY_MODULE_NAMES)
def module(request) -> ModuleType:
    return import_module(request.param)


def test_module_annotations(module: ModuleType):
    assert_annotations(module, "cfg", types.Config, "types.Config", "AnyConfig")
    assert_annotations(module, "config", types.Config, Optional[types.Config], ConfigParser)


def assert_fn_param_annotations(fn, ident, *expects):
    for param in signature(fn).parameters.values():
        if param.name == ident:
            assert param.annotation in expects, f"'{ident}' of {fn.__name__}() is not annotated expectedly"


def assert_fn_return_annotation(fn, ident, *expects):
    sourcelines, _ = getsourcelines(fn)
    for line in sourcelines:
        if line.endswith(f"    return {ident}"):
            assert signature(fn).return_annotation in expects, f"return of {fn.__name__}() is not annotated expectedly"


def assert_annotations(obj, ident, *expects):
    """Asserts annotations recursively in the object"""
    for name in dir(obj):
        if name.startswith("_"):
            continue

        attr = getattr(obj, name)
        if attr in vars(builtins).values() or type(attr) in vars(builtins).values():
            continue  # skip builtins

        obj_path = getattr(obj, "__module__", getattr(obj, "__qualname__", obj.__name__))
        try:
            attr_path = getattr(attr, "__module__", getattr(attr, "__qualname__", attr.__name__))
        except AttributeError:
            pass
        else:
            if attr_path and not attr_path.startswith(obj_path):
                continue  # the attribute is brought from outside the object

        if isclass(attr):
            assert_annotations(attr, ident, *expects)
        elif isinstance(attr, FunctionType):
            assert_fn_param_annotations(attr, ident, *expects)
            assert_fn_return_annotation(attr, ident, *expects)
