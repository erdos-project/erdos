# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import sys

sys.path.insert(0, os.path.abspath("../../"))

# -- Project information -----------------------------------------------------

project = "ERDOS"
copyright = "2018, The ERDOS Team"
author = "The ERDOS Team"

# The short X.Y version
version = "0.4"
# The full version, including alpha/beta/rc tags
release = "0.4.0"

# -- General configuration ---------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = "1.0"

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]

# Enable autodoc without requiring installation of listed modules
from unittest import mock  # noqa: E402

mock_modules = ["numpy"]
for mod_name in mock_modules:
    sys.modules[mod_name] = mock.Mock()


def mock_internal_type(qualname: str) -> mock.Mock:
    """Fixes an autodoc error when mocking internal types as arguments."""
    mocked_class = mock.Mock()
    mocked_class.__qualname__ = qualname
    return mocked_class


# Fix autodoc errors when using internal types as arguments.
internal_classes = {
    "PyTimestamp": "erdos.internal.PyTimestamp",
    "PyWriteStream": "erdos.internal.PyWriteStream",
    "PyReadStream": "erdos.internal.PyReadStream",
    "PyOperatorStream": "erdos.internal.PyOperatorStream",
    "PyStream": "erdos.internal.PyStream",
}

mock_erdos_internal = mock.Mock()
for classname, qualname in internal_classes.items():
    setattr(mock_erdos_internal, classname, mock_internal_type(qualname))
sys.modules["erdos.internal"] = mock_erdos_internal

# Ensure that all references resolve.
nitpicky = True
# Ignore bugs for references to the typing library.
nitpick_ignore = [
    ("py:data", "typing.Any"),
    ("py:data", "typing.Optional"),
    ("py:data", "typing.Tuple"),
    ("py:data", "typing.Callable"),
    ("py:obj", "erdos.streams.T"),
    ("py:obj", "erdos.operator.T"),
    ("py:obj", "erdos.operator.U"),
    ("py:obj", "erdos.operator.V"),
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = [".rst", ".md"]
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
import sphinx_rtd_theme  # noqa: E402

html_theme = "sphinx_rtd_theme"
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
# html_theme_options = {}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
#
# The default sidebars (for documents that don"t match any pattern) are
# defined by theme itself.  Builtin themes are using these templates by
# default: ``["localtoc.html", "relations.html", "sourcelink.html",
# "searchbox.html"]``.
#
html_sidebars = {"**": ["index.html"]}

# -- Options for HTMLHelp output ---------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = "ERDOSdoc"

# -- Options for LaTeX output ------------------------------------------------

latex_elements = {
    # The paper size ("letterpaper" or "a4paper").
    #
    # "papersize": "letterpaper",
    # The font size ("10pt", "11pt" or "12pt").
    #
    # "pointsize": "10pt",
    # Additional stuff for the LaTeX preamble.
    #
    # "preamble": "",
    # Latex figure (float) alignment
    #
    # "figure_align": "htbp",
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, "ERDOS.tex", "ERDOS Documentation", "The ERDOS Team", "manual"),
]

# -- Options for manual page output ------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [(master_doc, "erdos", "ERDOS Documentation", [author], 1)]

# -- Options for Texinfo output ----------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (
        master_doc,
        "ERDOS",
        "ERDOS Documentation",
        author,
        "ERDOS",
        "A platform for developing self-driving cars and robotics applications.",
        "Miscellaneous",
    ),
]

# -- Options for Epub output -------------------------------------------------

# Bibliographic Dublin Core info.
epub_title = project

# The unique identifier of the text. This can be a ISBN number
# or the project homepage.
#
# epub_identifier = ""

# A unique identification for the text.
#
# epub_uid = ""

# A list of files that should not be packed into the epub file.
epub_exclude_files = ["search.html"]

# -- Extension configuration -------------------------------------------------

# Python methods should be presented in source order
autodoc_member_order = "bysource"

# Enables automatic type inference from parameters in Napoleon.
napoleon_use_param = True
