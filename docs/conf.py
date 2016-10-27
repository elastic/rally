#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Rally documentation build configuration file, created by sphinx-quickstart

import os
from datetime import date
from os.path import join, dirname

# -- General configuration ------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.ifconfig"
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames. You can specify multiple suffix as a list of string:
# source_suffix = ['.rst', '.md']
source_suffix = '.rst'
master_doc = 'index'
language = None

year = date.today().year

rst_prolog = '''
.. |year| replace:: {0}
'''.format(year)



# General information about the project.
project = "Rally"
copyright = "%i, Daniel Mitterdorfer" % year
author = "Daniel Mitterdorfer"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.


# development versions always have the suffix '.dev0'
def read_version(full_version=True):
    with open(join(dirname(__file__), os.pardir, "version.txt")) as f:
        raw_version = f.read().strip()
        return raw_version if full_version else raw_version.replace(".dev0", "")


version = read_version(full_version=False)
# The full version, including alpha/beta/rc tags.
release = read_version()

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
#today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build']

# If true, '()' will be appended to :func: etc. cross-reference text.
#add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#add_module_names = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
# html_theme = 'alabaster'

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

if not on_rtd:  # only import and set the theme if we're building docs locally
    import sphinx_rtd_theme
    html_theme = 'sphinx_rtd_theme'
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
#html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
#html_logo = None

# The name of an image file (relative to this directory) to use as a favicon of
# the docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
#html_favicon = None

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#html_extra_path = []


# Output file base name for HTML help builder.
htmlhelp_basename = 'Rallydoc'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {}

latex_documents = [
    (master_doc, 'Rally.tex', 'Rally Documentation',
     'Daniel Mitterdorfer', 'manual'),
]

# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'esrally', 'Rally Documentation',
     [author], 1)
]

# If true, show URL addresses after external links.
#man_show_urls = False

# -- Options for Texinfo output -------------------------------------------
texinfo_documents = [
    (master_doc, 'Rally', 'Rally Documentation',
     author, 'Rally', 'Macrobenchmarking framework for Elasticsearch.',
     'Miscellaneous'),
]
