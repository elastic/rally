#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
import os
from datetime import date
from os.path import dirname, join

from sphinx.config import ConfigError

# -- General configuration ------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.ifconfig",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix(es) of source filenames. You can specify multiple suffix as a list of string:
# source_suffix = ['.rst', '.md']
source_suffix = ".rst"
root_doc = "index"
language = None

CI_VARS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".ci", "variables.json")


def read_min_python_version():
    try:
        with open(CI_VARS, "rt") as fp:
            return json.load(fp)["python_versions"]["MIN_PY_VER"]
    except KeyError as e:
        raise ConfigError(f"Failed building docs as required key [{e}] couldn't be found in the file [{CI_VARS}].")


GLOBAL_SUBSTITUTIONS = {
    "{MIN_PY_VER}": read_min_python_version(),
}


# inspiration from https://github.com/sphinx-doc/sphinx/issues/4054#issuecomment-329097229
def replace_globals(app, docname, source):
    tmp_source = source[0]
    for k, v in GLOBAL_SUBSTITUTIONS.items():
        tmp_source = tmp_source.replace(k, v)
    source[0] = tmp_source


def setup(app):
    app.connect("source-read", replace_globals)


def read_min_es_version():
    with open(join(dirname(__file__), os.pardir, "esrally/min-es-version.txt")) as f:
        return f.read().strip()


year = date.today().year

rst_prolog = f"""
.. |year| replace:: {year}
.. |MIN_PY_VER| replace:: {read_min_python_version()}
.. |min_es_version| replace:: {read_min_es_version()}
"""

# General information about the project.
project = "Rally"
copyright = "%i, Elasticsearch B.V." % year
author = "Daniel Mitterdorfer"

import esrally

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# Technically the short X.Y version, but let's use the full version, including .dev0
version = esrally.__version__
# The full version, including alpha/beta/rc tags.
release = version

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
# today = ''
# Else, today_fmt is used as the format for a strftime call.
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["_build"]

# If true, '()' will be appended to :func: etc. cross-reference text.
# add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
# add_module_names = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "furo"

html_static_path = ["."]
html_logo = "rally-logo.svg"

# The name of an image file (relative to this directory) to use as a favicon of
# the docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
# html_extra_path = []


# Output file base name for HTML help builder.
htmlhelp_basename = "Rallydoc"

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {}

latex_documents = [
    (
        root_doc,
        "Rally.tex",
        "Rally Documentation",
        "Daniel Mitterdorfer",
        "manual",
    ),
]

# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (
        root_doc,
        "esrally",
        "Rally Documentation",
        [author],
        1,
    )
]

# If true, show URL addresses after external links.
# man_show_urls = False

# -- Options for Texinfo output -------------------------------------------
texinfo_documents = [
    (
        root_doc,
        "Rally",
        "Rally Documentation",
        author,
        "Rally",
        "Macrobenchmarking framework for Elasticsearch.",
        "Miscellaneous",
    ),
]
