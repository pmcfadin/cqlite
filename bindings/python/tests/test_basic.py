"""Basic tests for cqlite Python bindings."""

import cqlite


def test_version():
    """Test version string is accessible."""
    version = cqlite.version()
    assert isinstance(version, str)
    assert len(version) > 0
    assert version == "0.3.0"


def test_version_attribute():
    """Test __version__ attribute."""
    assert hasattr(cqlite, "__version__")
    assert isinstance(cqlite.__version__, str)
    assert cqlite.__version__ == cqlite.version()
