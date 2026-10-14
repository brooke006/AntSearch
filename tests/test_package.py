from importlib.metadata import version

import antsearch


def test_package_imports():
    assert antsearch.__version__


def test_version_matches_metadata():
    assert antsearch.__version__ == version("antsearch")
